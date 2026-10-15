// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// recordingLauncher fails launches of the binaries in failing and records
// which binary each launch asked for.
type recordingLauncher struct {
	mu      sync.Mutex
	asked   []string
	failing map[string]bool
	serve   func(*fakeWorkerProcess) // when set, launched workers answer
}

func (l *recordingLauncher) Launch(_ context.Context, _ *Executor, cfg OperationProcessConfig, _ query.Limits) (WorkerProcess, func() error, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asked = append(l.asked, cfg.Binary)
	if l.failing[cfg.Binary] {
		return nil, nil, errors.New("launch failed")
	}
	proc := newFakeWorkerProcess()
	if l.serve != nil {
		go l.serve(proc)
	}
	return proc, proc.Kill, nil
}

func slimFixture(failing ...string) (*SlimFallbackLauncher, *recordingLauncher, *bool) {
	inner := &recordingLauncher{failing: map[string]bool{}}
	for _, f := range failing {
		inner.failing[f] = true
	}
	slim := OperationProcessConfig{Binary: "/opt/kelvo/kelvo-adapter-relational", SHA256: strings.Repeat("b", 64)}
	full := OperationProcessConfig{Binary: "/opt/kelvo/kelvo-adapter-go", SHA256: strings.Repeat("a", 64)}
	usable := false
	l := NewSlimFallbackLauncher(inner, slim, full)
	l.usable = func(context.Context, OperationProcessConfig) bool { return usable }
	return l, inner, &usable
}

// A slim launch that fails its check is retried on the full adapter, and
// later launches keep using the full adapter without trying the slim one.
func TestSlimLauncherFallsBackWhenSlimFailsItsCheck(t *testing.T) {
	l, inner, _ := slimFixture("/opt/kelvo/kelvo-adapter-relational")
	for range 3 {
		if _, _, err := l.Launch(context.Background(), nil, l.slim, query.Limits{}); err != nil {
			t.Fatal("a read's launch failed because the slim binary did:", err)
		}
	}
	want := []string{l.slim.Binary, l.full.Binary, l.full.Binary, l.full.Binary}
	if strings.Join(inner.asked, ",") != strings.Join(want, ",") || !l.Degraded() || l.Fallbacks() != 3 {
		t.Fatalf("asked %v degraded=%v fallbacks=%d", inner.asked, l.Degraded(), l.Fallbacks())
	}
}

// A launch that fails for another reason (the slim binary still verifies) is
// reported, not hidden behind a fallback.
func TestSlimLauncherKeepsGenericLaunchFailures(t *testing.T) {
	l, inner, usable := slimFixture("/opt/kelvo/kelvo-adapter-relational")
	*usable = true
	if _, _, err := l.Launch(context.Background(), nil, l.slim, query.Limits{}); err == nil {
		t.Fatal("a launch failure of a usable binary was swallowed")
	}
	if len(inner.asked) != 1 || l.Degraded() || l.Fallbacks() != 0 {
		t.Fatalf("asked %v degraded=%v fallbacks=%d", inner.asked, l.Degraded(), l.Fallbacks())
	}
}

// Launches return to the slim adapter once a check finds it usable again.
func TestSlimLauncherRecoversAfterARecheck(t *testing.T) {
	l, inner, usable := slimFixture("/opt/kelvo/kelvo-adapter-relational")
	_, _, _ = l.Launch(context.Background(), nil, l.slim, query.Limits{})
	if !l.Degraded() {
		t.Fatal("not degraded")
	}
	// Within the recheck window the slim binary is not even tried.
	if _, _, err := l.Launch(context.Background(), nil, l.slim, query.Limits{}); err != nil || len(inner.asked) != 3 {
		t.Fatal("recheck ran inside its window", inner.asked, err)
	}
	*usable = true
	inner.failing = map[string]bool{}
	l.mu.Lock()
	l.checked = time.Now().Add(-2 * slimRecheckEvery)
	l.mu.Unlock()
	if _, _, err := l.Launch(context.Background(), nil, l.slim, query.Limits{}); err != nil {
		t.Fatal(err)
	}
	if l.Degraded() || inner.asked[len(inner.asked)-1] != l.slim.Binary {
		t.Fatalf("did not return to the slim adapter: degraded=%v asked=%v", l.Degraded(), inner.asked)
	}
}

// Launches for any other binary (durable operations) are untouched.
func TestSlimLauncherPassesOtherBinariesThrough(t *testing.T) {
	l, inner, _ := slimFixture("/opt/kelvo/kelvo-adapter-relational")
	if _, _, err := l.Launch(context.Background(), nil, l.full, query.Limits{}); err != nil {
		t.Fatal(err)
	}
	if len(inner.asked) != 1 || inner.asked[0] != l.full.Binary || l.Degraded() {
		t.Fatalf("asked %v", inner.asked)
	}
}
