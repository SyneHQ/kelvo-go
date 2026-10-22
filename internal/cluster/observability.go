// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"fmt"
	"io"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

const (
	maxTenantLabels   = 256
	otherTenantLabel  = "other"
	slowLogPerSecond  = 10
	maxSlowLogSources = 16
)

type readKey struct{ outcome, class string }

type tenantReads struct {
	counts   map[readKey]uint64
	duration telemetry.Histogram
}

// readObserver records per-tenant read metrics, the slow-read log and the
// state the stall detector watches. Memory is bounded: at most maxTenantLabels+1
// tenants, each with the fixed outcome/class vocabulary. It never sees SQL.
type readObserver struct {
	now  func() time.Time
	logf func(format string, args ...any)
	slow time.Duration

	mu       sync.Mutex
	tenants  map[string]*tenantReads
	inFlight int
	progress time.Time // last completion, or when the node became busy
	winStart time.Time
	winCount int
	dropped  uint64

	stall *stallDetector
}

func newReadObserver(cfg *ObservabilityConfig) *readObserver {
	o := &readObserver{now: time.Now, logf: log.Printf, tenants: map[string]*tenantReads{}}
	if cfg != nil {
		o.slow = cfg.SlowReadThreshold
	}
	return o
}

func (o *readObserver) begin() {
	if o == nil {
		return
	}
	o.mu.Lock()
	if o.inFlight == 0 {
		o.progress = o.now()
	}
	o.inFlight++
	o.mu.Unlock()
}

// end records one finished read. stats may be zero on failure.
func (o *readObserver) end(tenant, jobID string, req query.Request, d time.Duration, stats query.Stats, err error) {
	if o == nil {
		return
	}
	outcome, class := "success", "none"
	if err != nil {
		class = worker.FailureClass(err)
		outcome = "error"
		if class == "cancelled" || class == "deadline" {
			outcome = "canceled"
		}
	}
	now := o.now()
	o.mu.Lock()
	o.inFlight--
	o.progress = now
	label := tenant
	t := o.tenants[label]
	if t == nil {
		if len(o.tenants) >= maxTenantLabels {
			label = otherTenantLabel
			t = o.tenants[label]
		}
		if t == nil {
			t = &tenantReads{counts: map[readKey]uint64{}}
			o.tenants[label] = t
		}
	}
	t.counts[readKey{outcome, class}]++
	t.duration.Observe(d)
	logIt := false
	if o.slow > 0 && d >= o.slow {
		if now.Sub(o.winStart) >= time.Second || o.winStart.IsZero() {
			o.winStart, o.winCount = now, 0
		}
		if o.winCount < slowLogPerSecond {
			o.winCount++
			logIt = true
		} else {
			o.dropped++
		}
	}
	o.mu.Unlock()
	if logIt {
		o.logf("kelvo slow read: tenant=%q job=%q mode=%q sources=%q duration_ms=%d rows=%d bytes=%d outcome=%s class=%s",
			tenant, jobID, req.Mode, safeSources(req.Sources), d.Milliseconds(), stats.Rows, stats.Bytes, outcome, class)
	}
}

// safeSources lists only well-formed source IDs, never other request content.
func safeSources(ids []string) string {
	var out []string
	for _, id := range ids {
		if len(out) == maxSlowLogSources {
			break
		}
		if catalog.ValidID(id) {
			out = append(out, id)
		}
	}
	return strings.Join(out, ",")
}

// stalled reports whether reads are in flight and none finished for after.
func (o *readObserver) stalled(after time.Duration) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.inFlight > 0 && o.now().Sub(o.progress) >= after
}

func (o *readObserver) writeMetrics(w io.Writer) {
	if o == nil {
		return
	}
	o.mu.Lock()
	names := make([]string, 0, len(o.tenants))
	snap := make(map[string]tenantReads, len(o.tenants))
	for name, t := range o.tenants {
		names = append(names, name)
		c := make(map[readKey]uint64, len(t.counts))
		for k, v := range t.counts {
			c[k] = v
		}
		snap[name] = tenantReads{counts: c, duration: t.duration}
	}
	inFlight, dropped := o.inFlight, o.dropped
	o.mu.Unlock()
	slices.Sort(names)

	_, _ = fmt.Fprint(w, "# HELP kelvo_reads_total Reads completed on this node by tenant, outcome and failure class (class is none on success). Tenants beyond 256 are folded into other.\n# TYPE kelvo_reads_total counter\n")
	for _, name := range names {
		keys := make([]readKey, 0, len(snap[name].counts))
		for k := range snap[name].counts {
			keys = append(keys, k)
		}
		slices.SortFunc(keys, func(a, b readKey) int { return strings.Compare(a.outcome+"/"+a.class, b.outcome+"/"+b.class) })
		for _, k := range keys {
			_, _ = fmt.Fprintf(w, "kelvo_reads_total{tenant=%q,outcome=%q,class=%q} %d\n", name, k.outcome, k.class, snap[name].counts[k])
		}
	}
	_, _ = fmt.Fprint(w, "# HELP kelvo_read_duration_seconds Node read duration by tenant, from execution start to completion.\n# TYPE kelvo_read_duration_seconds histogram\n")
	for _, name := range names {
		telemetry.WriteHistogramSeries(w, "kelvo_read_duration_seconds", fmt.Sprintf("tenant=%q", name), snap[name].duration)
	}
	_, _ = fmt.Fprintf(w, "# HELP kelvo_reads_in_flight Reads currently executing on this node.\n# TYPE kelvo_reads_in_flight gauge\nkelvo_reads_in_flight %d\n"+
		"# HELP kelvo_slow_read_log_dropped_total Slow-read log lines dropped by the rate limit.\n# TYPE kelvo_slow_read_log_dropped_total counter\nkelvo_slow_read_log_dropped_total %d\n", inFlight, dropped)
	if o.stall != nil {
		_, _ = fmt.Fprintf(w, "# HELP kelvo_stall_dumps_total Goroutine dumps written because reads were in flight with no completion.\n# TYPE kelvo_stall_dumps_total counter\nkelvo_stall_dumps_total %d\n", o.stall.count())
	}
}
