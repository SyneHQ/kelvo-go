// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import (
	"context"
	"sync"
	"time"
)

const ChildTimingVersion uint8 = 1

type ChildStage uint8

const (
	ChildWorkerSetup ChildStage = iota
	ChildExecutorCall
	ChildIPCFinalize
	ChildWorkerCleanup
	ChildEngineSetup
	ChildMaterialization
	ChildArrowDrain
	ChildStageCount
)

// ChildInterval contains offsets from one child-local monotonic origin. An
// unobserved interval must contain zeros; it is never a zero-duration sample.
type ChildInterval struct {
	StartNS  int64 `json:"start_ns"`
	EndNS    int64 `json:"end_ns"`
	Observed bool  `json:"observed"`
}

// ChildTiming is diagnostic only. The worker lane is disjoint; the DuckDB lane
// is nested inside executor_call and must never be added to the worker lane.
// No child timestamps are aligned with parent timestamps or trace spans.
type ChildTiming struct {
	Version uint8                          `json:"version"`
	TotalNS int64                          `json:"total_ns"`
	Stages  [ChildStageCount]ChildInterval `json:"stages"`
}

// Valid uses the parent's independently observed Start-to-Wait duration. Bounds
// are checked before subtraction, so malformed offsets cannot overflow.
func (r ChildTiming) Valid(parentBound time.Duration) bool {
	if r.Version != ChildTimingVersion || r.TotalNS < 0 || r.TotalNS > int64(24*time.Hour) || parentBound < 0 || r.TotalNS > int64(parentBound) {
		return false
	}
	for _, interval := range r.Stages {
		if !interval.Observed {
			if interval.StartNS != 0 || interval.EndNS != 0 {
				return false
			}
			continue
		}
		if interval.StartNS < 0 || interval.EndNS < interval.StartNS || interval.EndNS > r.TotalNS {
			return false
		}
	}
	if !r.Stages[ChildWorkerSetup].Observed || !r.Stages[ChildWorkerCleanup].Observed {
		return false
	}
	var end int64
	for _, interval := range r.Stages[:ChildEngineSetup] {
		if interval.Observed {
			if interval.StartNS < end {
				return false
			}
			end = interval.EndNS
		}
	}
	executor := r.Stages[ChildExecutorCall]
	if r.Stages[ChildIPCFinalize].Observed && !executor.Observed {
		return false
	}
	if r.Stages[ChildMaterialization].Observed && !r.Stages[ChildEngineSetup].Observed {
		return false
	}
	if r.Stages[ChildArrowDrain].Observed && !r.Stages[ChildMaterialization].Observed {
		return false
	}
	end = executor.StartNS
	for _, interval := range r.Stages[ChildEngineSetup:] {
		if interval.Observed {
			if !executor.Observed || interval.StartNS < end || interval.EndNS > executor.EndNS {
				return false
			}
			end = interval.EndNS
		}
	}
	return true
}

// ChildRecorder retains only seven intervals, independent of rows and batches.
// Nil methods do not read clocks. Its lock permits diagnostic snapshots while
// a sink is blocked; it is not used in per-record delivery loops.
type ChildRecorder struct {
	mu       sync.Mutex
	origin   time.Time
	report   ChildTiming
	starts   [ChildStageCount]int64
	active   [ChildStageCount]bool
	used     [ChildStageCount]bool
	invalid  bool
	finished bool
}

func NewChildRecorder() *ChildRecorder {
	return &ChildRecorder{origin: time.Now(), report: ChildTiming{Version: ChildTimingVersion}}
}

func (r *ChildRecorder) Begin(stage ChildStage) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished || stage >= ChildStageCount || r.used[stage] {
		r.invalid = true
		return
	}
	r.starts[stage] = time.Since(r.origin).Nanoseconds()
	r.active[stage], r.used[stage] = true, true
}

func (r *ChildRecorder) End(stage ChildStage) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished || stage >= ChildStageCount || !r.active[stage] {
		r.invalid = true
		return
	}
	r.report.Stages[stage] = ChildInterval{StartNS: r.starts[stage], EndNS: time.Since(r.origin).Nanoseconds(), Observed: true}
	r.active[stage] = false
}

// Snapshot contains completed intervals only. TotalNS stays zero until Finish.
func (r *ChildRecorder) Snapshot() ChildTiming {
	if r == nil {
		return ChildTiming{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.report
}

// Finish is called after worker cleanup returns. Unclosed nested stages (for
// example failed engine preparation) stay unknown inside executor_call.
func (r *ChildRecorder) Finish() *ChildTiming {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.finished {
		r.report.TotalNS = time.Since(r.origin).Nanoseconds()
		r.finished = true
	}
	for _, active := range r.active[:ChildEngineSetup] {
		if active {
			r.invalid = true
		}
	}
	if r.invalid || !r.report.Valid(time.Duration(r.report.TotalNS)) {
		return nil
	}
	copy := r.report
	return &copy
}

type childRecorderKey struct{}

// WithChildRecorder adds no wrapper in disabled mode.
func WithChildRecorder(ctx context.Context, recorder *ChildRecorder) context.Context {
	if recorder == nil {
		return ctx
	}
	return context.WithValue(ctx, childRecorderKey{}, recorder)
}

func ChildRecorderFromContext(ctx context.Context) *ChildRecorder {
	if ctx == nil {
		return nil
	}
	recorder, _ := ctx.Value(childRecorderKey{}).(*ChildRecorder)
	return recorder
}
