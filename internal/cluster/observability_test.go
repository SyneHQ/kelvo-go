// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func testObserver(slow time.Duration) (*readObserver, *fakeClock, *[]string) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	var lines []string
	o := newReadObserver(&ObservabilityConfig{SlowReadThreshold: slow})
	o.now = clock.now
	o.logf = func(f string, a ...any) { lines = append(lines, fmt.Sprintf(f, a...)) }
	return o, clock, &lines
}

func TestReadObserverCapsTenantLabels(t *testing.T) {
	o, _, _ := testObserver(0)
	for i := 0; i < maxTenantLabels+10; i++ {
		o.begin()
		o.end(fmt.Sprintf("t%d", i), "j", query.Request{}, time.Millisecond, query.Stats{}, nil)
	}
	if len(o.tenants) != maxTenantLabels+1 || o.tenants["other"].duration.Count != 10 {
		t.Fatalf("tenants=%d other=%+v", len(o.tenants), o.tenants["other"])
	}
}

func TestReadObserverMetricsFormat(t *testing.T) {
	o, _, _ := testObserver(0)
	o.begin()
	o.end("acme", "j", query.Request{}, 20*time.Millisecond, query.Stats{}, nil)
	o.begin()
	o.end("acme", "j", query.Request{}, time.Second, query.Stats{}, context.DeadlineExceeded)
	var out bytes.Buffer
	o.writeMetrics(&out)
	for _, want := range []string{
		`kelvo_reads_total{tenant="acme",outcome="success",class="none"} 1`,
		`kelvo_reads_total{tenant="acme",outcome="canceled",class="deadline"} 1`,
		`kelvo_read_duration_seconds_bucket{tenant="acme",le="0.05"} 1`,
		`kelvo_read_duration_seconds_bucket{tenant="acme",le="+Inf"} 2`,
		`kelvo_read_duration_seconds_count{tenant="acme"} 2`,
		"# TYPE kelvo_read_duration_seconds histogram",
		"kelvo_reads_in_flight 0",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in\n%s", want, out.String())
		}
	}
}

func TestSlowReadLogOmitsSQLAndRateLimits(t *testing.T) {
	o, clock, lines := testObserver(time.Second)
	req := query.Request{SQL: "SELECT secret FROM t", Delegation: "tok", Mode: "federated", Sources: []string{"pg1", "bad id;"}}
	o.begin()
	o.end("acme", "job1", req, time.Millisecond, query.Stats{}, nil)
	if len(*lines) != 0 {
		t.Fatal("fast read logged")
	}
	for i := 0; i < 25; i++ {
		o.begin()
		o.end("acme", "job1", req, 2*time.Second, query.Stats{Rows: 3, Bytes: 9}, errors.New("dsn=postgres://u:p@h"))
	}
	if len(*lines) != slowLogPerSecond || o.dropped != 25-slowLogPerSecond {
		t.Fatalf("lines=%d dropped=%d", len(*lines), o.dropped)
	}
	line := (*lines)[0]
	for _, bad := range []string{"SELECT", "secret", "tok", "postgres", "bad id"} {
		if strings.Contains(line, bad) {
			t.Fatalf("leaked %q: %s", bad, line)
		}
	}
	for _, want := range []string{`tenant="acme"`, `job="job1"`, `mode="federated"`, `sources="pg1"`, "rows=3", "bytes=9", "class=execution"} {
		if !strings.Contains(line, want) {
			t.Fatalf("missing %q: %s", want, line)
		}
	}
	clock.t = clock.t.Add(time.Second)
	o.begin()
	o.end("acme", "job1", req, 2*time.Second, query.Stats{}, nil)
	if len(*lines) != slowLogPerSecond+1 {
		t.Fatal("window did not reset")
	}
}

func TestStallDetectorFiresOncePerWindowAndPrunes(t *testing.T) {
	o, clock, _ := testObserver(0)
	dir := filepath.Join(t.TempDir(), "dumps")
	s := newStallDetector(StallDumpConfig{Dir: dir, After: 30 * time.Second}, o)
	s.write = func(w io.Writer) error { _, err := io.WriteString(w, "goroutine dump"); return err }
	o.stall = s

	if s.check() {
		t.Fatal("dump while idle")
	}
	o.begin()
	clock.t = clock.t.Add(29 * time.Second)
	if s.check() {
		t.Fatal("dump before threshold")
	}
	clock.t = clock.t.Add(time.Second)
	if !s.check() || s.check() {
		t.Fatal("want exactly one dump in the window")
	}
	clock.t = clock.t.Add(9 * time.Minute)
	if s.check() {
		t.Fatal("second dump inside 10m")
	}
	for i := 0; i < 7; i++ {
		clock.t = clock.t.Add(stallDumpInterval)
		if !s.check() {
			t.Fatalf("dump %d missing", i)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != stallDumpKeep {
		t.Fatalf("kept %d files", len(entries))
	}
	info, _ := os.Stat(filepath.Join(dir, entries[0].Name()))
	dirInfo, _ := os.Stat(dir)
	if runtime.GOOS != "windows" && (info.Mode().Perm() != 0o600 || dirInfo.Mode().Perm() != 0o700) {
		t.Fatalf("modes %v %v", info.Mode().Perm(), dirInfo.Mode().Perm())
	}
	if s.count() != 8 {
		t.Fatalf("count %d", s.count())
	}
	// A completion resets the stall.
	o.end("acme", "j", query.Request{}, time.Second, query.Stats{}, nil)
	clock.t = clock.t.Add(stallDumpInterval * 2)
	o.begin()
	if o.stalled(time.Minute) {
		t.Fatal("fresh read counted as stalled")
	}
}

func TestObservabilityConfigValidate(t *testing.T) {
	dir := t.TempDir()
	good := []*ObservabilityConfig{nil, {}, {SlowReadThreshold: 10 * time.Minute}, {StallDump: &StallDumpConfig{Dir: dir, After: time.Minute}}}
	for i, c := range good {
		if err := c.Validate(); err != nil {
			t.Errorf("good %d: %v", i, err)
		}
	}
	bad := []*ObservabilityConfig{
		{SlowReadThreshold: -1}, {SlowReadThreshold: 11 * time.Minute},
		{StallDump: &StallDumpConfig{Dir: "relative", After: time.Minute}},
		{StallDump: &StallDumpConfig{Dir: dir + "/../x", After: time.Minute}},
		{StallDump: &StallDumpConfig{Dir: dir}},
	}
	for i, c := range bad {
		if c.Validate() == nil {
			t.Errorf("bad %d accepted", i)
		}
	}
}
