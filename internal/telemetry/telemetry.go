// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package telemetry provides bounded, process-local diagnostics. Aggregate
// lifecycle metrics never store query text, identifiers, credentials, tenant
// names or source names. The separate SourceHealth registry contains only
// explicitly configured source IDs for authenticated tenant-node diagnostics.
package telemetry

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type Kind uint8

const (
	KindQuery Kind = iota
	KindRefresh
	kindCount
)

type Outcome uint8

const (
	OutcomeSuccess Outcome = iota
	OutcomeError
	OutcomeCanceled
	outcomeCount
)

type Rejection uint8

const (
	RejectionCapacity Rejection = iota
	RejectionDraining
	rejectionCount
)

var kindNames = [...]string{"query", "refresh"}
var outcomeNames = [...]string{"success", "error", "canceled"}
var rejectionNames = [...]string{"capacity", "draining"}

// Boundaries are seconds, fixed to keep recording and export memory bounded.
var boundaries = [...]float64{.001, .005, .01, .05, .1, .5, 1, 5, 10, 30, 60, 300}

type Histogram struct {
	// Buckets are cumulative; Count also includes values above the last boundary.
	Buckets    [12]uint64
	Count      uint64
	SumSeconds float64
}

type Snapshot struct {
	Outcomes          [2][3]uint64
	Rejections        [2][2]uint64
	QueueWait         [2]Histogram
	Duration          [2]Histogram
	Phases            [2][PhaseCount]Histogram
	FirstBatch        [2]Histogram
	SinkDuration      [2]Histogram
	ChildReports      [2][childTimingStatusCount]uint64
	ChildUnknown      [2][ChildStageCount][childTimingStatusCount]uint64
	ChildWorkerStages [2][ChildEngineSetup]Histogram
	ChildDuckDBStages [2][ChildStageCount - ChildEngineSetup]Histogram
}

// Registry's zero value is usable. All state has a fixed size, regardless of
// request volume. Snapshots are copied before rendering so a stalled scraper
// cannot hold the recording lock. A Registry must not be copied after first use.
type Registry struct {
	mu    sync.Mutex
	state Snapshot
}

func New() *Registry { return &Registry{} }

// Observe records one terminal lifecycle event. Call exactly once per completed
// job, including errors and cancellations. queueWait and duration are distinct
// measured intervals: duration should exclude queueWait. Negative intervals are
// clamped to zero. Unknown enums are ignored, never turned into dynamic labels.
// A nil Registry disables collection.
func (r *Registry) Observe(kind Kind, outcome Outcome, queueWait, duration time.Duration) {
	if r == nil || kind >= kindCount || outcome >= outcomeCount {
		return
	}
	r.mu.Lock()
	r.state.Outcomes[kind][outcome]++
	observe(&r.state.QueueWait[kind], queueWait)
	observe(&r.state.Duration[kind], duration)
	r.mu.Unlock()
}

func observe(h *Histogram, d time.Duration) {
	if d < 0 {
		d = 0
	}
	seconds := d.Seconds()
	h.Count++
	h.SumSeconds += seconds
	for i, boundary := range boundaries {
		if seconds <= boundary {
			h.Buckets[i]++
		}
	}
}

// Reject counts jobs refused before execution, separately from completed jobs.
func (r *Registry) Reject(kind Kind, reason Rejection) {
	if r == nil || kind >= kindCount || reason >= rejectionCount {
		return
	}
	r.mu.Lock()
	r.state.Rejections[kind][reason]++
	r.mu.Unlock()
}

func (r *Registry) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}
	r.mu.Lock()
	snapshot := r.state
	r.mu.Unlock()
	return snapshot
}

// ServeHTTP exposes aggregate process metrics only. The caller must restrict
// access (for example using its existing authenticated administration route).
// These are not tenant-scoped metrics and must not be served to tenant clients.
func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if req.Method == http.MethodHead {
		return
	}
	snapshot := r.Snapshot()
	// No request processing or locks depend on a successful metrics export.
	_ = writeMetrics(w, snapshot)
}

func writeMetrics(w io.Writer, s Snapshot) error {
	// Build one bounded payload, avoiding locks and repeated network writes.
	var out metricBuffer
	out.printf("# HELP kelvo_jobs_completed_total Terminal job outcomes in this process.\n# TYPE kelvo_jobs_completed_total counter\n")
	for kind, name := range kindNames {
		for outcome, label := range outcomeNames {
			out.printf("kelvo_jobs_completed_total{kind=%q,outcome=%q} %d\n", name, label, s.Outcomes[kind][outcome])
		}
	}
	out.printf("# HELP kelvo_jobs_rejected_total Jobs refused before execution in this process.\n# TYPE kelvo_jobs_rejected_total counter\n")
	for kind, name := range kindNames {
		for reason, label := range rejectionNames {
			out.printf("kelvo_jobs_rejected_total{kind=%q,reason=%q} %d\n", name, label, s.Rejections[kind][reason])
		}
	}
	writeHistogram(&out, "kelvo_job_queue_wait_seconds", "Observed worker admission wait of terminal jobs; excludes distributed dispatch wait.", s.QueueWait)
	writeHistogram(&out, "kelvo_job_duration_seconds", "Observed worker call duration including setup and transfer but excluding admission wait.", s.Duration)
	writePhaseMetrics(&out, s)
	writeChildMetrics(&out, s)
	_, err := w.Write(out.data)
	return err
}

type metricBuffer struct{ data []byte }

func (b *metricBuffer) printf(format string, args ...any) {
	b.data = fmt.Appendf(b.data, format, args...)
}

func writeHistogram(out *metricBuffer, name, help string, histograms [2]Histogram) {
	out.printf("# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	for kind, label := range kindNames {
		h := histograms[kind]
		for i, boundary := range boundaries {
			out.printf("%s_bucket{kind=%q,le=\"%g\"} %d\n", name, label, boundary, h.Buckets[i])
		}
		out.printf("%s_bucket{kind=%q,le=\"+Inf\"} %d\n", name, label, h.Count)
		out.printf("%s_sum{kind=%q} %g\n", name, label, h.SumSeconds)
		out.printf("%s_count{kind=%q} %d\n", name, label, h.Count)
	}
}
