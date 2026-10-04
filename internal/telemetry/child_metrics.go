// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import "time"

type ChildTimingStatus uint8

const (
	ChildTimingObserved ChildTimingStatus = iota
	ChildTimingMissing
	ChildTimingMalformed
	// ChildTimingTerminated means no usable report after OS-signaled exit. A
	// canceled context alone is not evidence of signaled process termination.
	ChildTimingTerminated
	childTimingStatusCount
)

var childStatusNames = [...]string{"observed", "missing", "malformed", "terminated"}
var childUnknownNames = [...]string{"unobserved", "missing", "malformed", "terminated"}
var childStageNames = [...]string{"setup", "executor_call", "ipc_finalize", "cleanup", "engine_setup", "materialization", "arrow_drain"}

// ObserveChildTiming records a report once per started subprocess, including
// failures. Durations are diagnostic wall durations, not pure compute/network
// costs. Missing stages increment fixed unknown counters instead of histograms.
func (r *Registry) ObserveChildTiming(kind Kind, report *ChildTiming, status ChildTimingStatus, parentBound time.Duration) {
	if r == nil || kind >= kindCount || status >= childTimingStatusCount {
		return
	}
	if status == ChildTimingObserved && (report == nil || !report.Valid(parentBound)) {
		status = ChildTimingMalformed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.ChildReports[kind][status]++
	for stage := ChildWorkerSetup; stage < ChildStageCount; stage++ {
		if status != ChildTimingObserved || !report.Stages[stage].Observed {
			r.state.ChildUnknown[kind][stage][status]++
			continue
		}
		interval := report.Stages[stage]
		duration := time.Duration(interval.EndNS - interval.StartNS)
		if stage < ChildEngineSetup {
			observe(&r.state.ChildWorkerStages[kind][stage], duration)
		} else {
			observe(&r.state.ChildDuckDBStages[kind][stage-ChildEngineSetup], duration)
		}
	}
}

func writeChildMetrics(out *metricBuffer, s Snapshot) {
	const reports = "kelvo_child_timing_reports_total"
	out.printf("# HELP %s Child timing availability after process wait; terminated means an unavailable report after signaled exit.\n# TYPE %s counter\n", reports, reports)
	for kind, kindName := range kindNames {
		for status, statusName := range childStatusNames {
			out.printf("%s{kind=%q,status=%q} %d\n", reports, kindName, statusName, s.ChildReports[kind][status])
		}
	}
	const unknown = "kelvo_child_stage_unknown_total"
	out.printf("# HELP %s Child stages without duration observations; unknown is never recorded as zero.\n# TYPE %s counter\n", unknown, unknown)
	for kind, kindName := range kindNames {
		for stage, stageName := range childStageNames {
			lane := "worker"
			if stage >= int(ChildEngineSetup) {
				lane = "duckdb"
			}
			for reason, reasonName := range childUnknownNames {
				out.printf("%s{kind=%q,lane=%q,stage=%q,reason=%q} %d\n", unknown, kindName, lane, stageName, reasonName, s.ChildUnknown[kind][stage][reason])
			}
		}
	}
	writeChildStageMetrics(out, "kelvo_child_worker_stage_seconds", "Disjoint child worker wall durations; executor_call includes engine cleanup and unattributed work.", childStageNames[:ChildEngineSetup], func(kind, stage int) Histogram {
		return s.ChildWorkerStages[kind][stage]
	})
	writeChildStageMetrics(out, "kelvo_child_duckdb_stage_seconds", "Nested DuckDB wall durations inside child executor_call; never sum with worker stages, and not pure compute or network time.", childStageNames[ChildEngineSetup:], func(kind, stage int) Histogram {
		return s.ChildDuckDBStages[kind][stage]
	})
}

func writeChildStageMetrics(out *metricBuffer, name, help string, stages []string, histogram func(int, int) Histogram) {
	out.printf("# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	for kind, kindName := range kindNames {
		for stage, stageName := range stages {
			h := histogram(kind, stage)
			for i, boundary := range boundaries {
				out.printf("%s_bucket{kind=%q,stage=%q,le=\"%g\"} %d\n", name, kindName, stageName, boundary, h.Buckets[i])
			}
			out.printf("%s_bucket{kind=%q,stage=%q,le=\"+Inf\"} %d\n", name, kindName, stageName, h.Count)
			out.printf("%s_sum{kind=%q,stage=%q} %g\n", name, kindName, stageName, h.SumSeconds)
			out.printf("%s_count{kind=%q,stage=%q} %d\n", name, kindName, stageName, h.Count)
		}
	}
}
