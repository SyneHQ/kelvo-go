// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import "time"

// Phase is a fixed local worker boundary, not a database execution operator.
type Phase uint8

const (
	PhaseValidation Phase = iota
	PhaseNodeAdmission
	PhaseSourceAdmission
	PhasePrepare
	PhaseExecutionDelivery
	PhaseCleanup
	PhaseCount
)

var phaseNames = [...]string{"validation", "node_admission", "source_admission", "prepare", "execution_delivery", "cleanup"}

func (p Phase) Name() string {
	if p >= PhaseCount {
		return ""
	}
	return phaseNames[p]
}

// PhaseInterval uses offsets from one monotonic process clock. Absent intervals
// are not recorded as zero-duration observations. No remote timestamp is used.
type PhaseInterval struct {
	Start, End time.Duration
	Observed   bool
}

// PhaseTimings has a fixed size regardless of batch count. SinkDuration is a
// subset of ExecutionDelivery, not a disjoint execution phase. FirstBatch is
// measured from executor entry until the first decoded batch reaches its sink,
// before that sink's Write call; it is not time to client receipt.
type PhaseTimings struct {
	Intervals     [PhaseCount]PhaseInterval
	FirstBatch    time.Duration
	HasFirstBatch bool
	SinkDuration  time.Duration
	HasSink       bool
}

// Valid rejects malformed instrumentation instead of publishing misleading
// timelines. Real worker calls are bounded; excessively large samples are not
// valid lifecycle measurements. Observed intervals must be ordered and disjoint.
func (p PhaseTimings) Valid(total time.Duration) bool {
	if total < 0 || total > 24*time.Hour {
		return false
	}
	var last time.Duration
	for _, interval := range p.Intervals {
		if !interval.Observed {
			continue
		}
		if interval.Start < last || interval.End < interval.Start || interval.End > total {
			return false
		}
		last = interval.End
	}
	if p.HasFirstBatch && (p.FirstBatch < 0 || p.FirstBatch > total) {
		return false
	}
	stream := p.Intervals[PhaseExecutionDelivery]
	if p.HasFirstBatch && (!stream.Observed || p.FirstBatch < stream.Start || p.FirstBatch > stream.End) {
		return false
	}
	return !p.HasSink || (stream.Observed && p.SinkDuration >= 0 && p.SinkDuration <= stream.End-stream.Start)
}

// ObservePhases records one terminal worker's reached phases. It does not change
// lifecycle outcome counts, which are still recorded exactly once by Observe.
func (r *Registry) ObservePhases(kind Kind, phases PhaseTimings, total time.Duration) {
	if r == nil || kind >= kindCount || !phases.Valid(total) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for phase, interval := range phases.Intervals {
		if interval.Observed {
			observe(&r.state.Phases[kind][phase], interval.End-interval.Start)
		}
	}
	if phases.HasFirstBatch {
		observe(&r.state.FirstBatch[kind], phases.FirstBatch)
	}
	if phases.HasSink {
		observe(&r.state.SinkDuration[kind], phases.SinkDuration)
	}
}

func writePhaseMetrics(out *metricBuffer, s Snapshot) {
	const name = "kelvo_worker_phase_seconds"
	out.printf("# HELP %s Disjoint local worker phases; execution_delivery includes overlapping computation and IPC delivery.\n# TYPE %s histogram\n", name, name)
	for kind, kindName := range kindNames {
		for phase, phaseName := range phaseNames {
			h := s.Phases[kind][phase]
			for i, boundary := range boundaries {
				out.printf("%s_bucket{kind=%q,phase=%q,le=\"%g\"} %d\n", name, kindName, phaseName, boundary, h.Buckets[i])
			}
			out.printf("%s_bucket{kind=%q,phase=%q,le=\"+Inf\"} %d\n", name, kindName, phaseName, h.Count)
			out.printf("%s_sum{kind=%q,phase=%q} %g\n", name, kindName, phaseName, h.SumSeconds)
			out.printf("%s_count{kind=%q,phase=%q} %d\n", name, kindName, phaseName, h.Count)
		}
	}
	writeHistogram(out, "kelvo_worker_first_batch_seconds", "Executor entry to first decoded Arrow record before sink delivery; no observation for results without records.", s.FirstBatch)
	writeHistogram(out, "kelvo_worker_sink_seconds", "Cumulative synchronous schema and record sink callbacks; a subset of execution_delivery, not pure network transfer.", s.SinkDuration)
}
