//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
)

func stallTestSources(t *testing.T) stallSources {
	pool, err := admission.New(admission.Limits{MaxConcurrent: 2, MemoryBytes: 1024, ScratchBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	cgroup := t.TempDir()
	return stallSources{cgroupRoot: cgroup, unitCgroup: cgroup, pool: pool, metrics: func() string { return "m 1\n" },
		poolReads: func() (int64, int64, int64) { return 1, 2, 3 }}
}

// The harness must capture where a run is stuck, once, only for requests that
// are in flight and have made no progress, and never for an idle node.
func TestStallWatchCapturesOnlyWhenRequestsStopCompleting(t *testing.T) {
	src, logf := stallTestSources(t), func(string, ...any) {}
	w := newStallWatch(t.TempDir())
	now := time.Now()
	if w.poll(now.Add(time.Hour), src, logf) {
		t.Fatal("an idle node was reported as stalled")
	}
	done := w.begin()
	if w.poll(now.Add(stallAfter/2), src, logf) {
		t.Fatal("a request that just started was reported as stalled")
	}
	done(200)
	if w.completed.Load() != 1 {
		t.Fatal("completion not counted")
	}
	w.begin() // never finishes
	late := time.Now().Add(stallAfter + time.Second)
	if !w.poll(late, src, logf) || w.captures != 1 {
		t.Fatal("stalled requests were not captured")
	}
	first := filepath.Join(w.dir, "capture-1")
	for _, name := range []string{"goroutines.txt", "summary.json", "admission.json", "metrics.prom", "procs.txt", "source-backends.txt"} {
		if raw, err := os.ReadFile(filepath.Join(first, name)); err != nil || len(raw) == 0 {
			t.Fatalf("capture is missing %s: %v", name, err)
		}
	}
	if raw, _ := os.ReadFile(filepath.Join(first, "goroutines.txt")); !strings.Contains(string(raw), "goroutine ") {
		t.Fatal("goroutine dump is empty")
	}
	if w.poll(late.Add(time.Second), src, logf); w.captures != 1 {
		t.Fatal("captured again before the recheck delay")
	}
	w.poll(late.Add(stallRecheck+time.Second), src, logf)
	if w.captures != 2 {
		t.Fatal("no second capture to tell stuck from slow")
	}
	w.poll(late.Add(time.Hour), src, logf)
	if w.captures != 2 {
		t.Fatal("captured more than twice")
	}
}
