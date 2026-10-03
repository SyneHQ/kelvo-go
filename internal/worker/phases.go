// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/apache/arrow-go/v18/arrow"
)

// executionPhases is call-owned, fixed-size instrumentation. It retains no
// records or request data. Disabled calls do not wrap the sink or read clocks.
type executionPhases struct {
	enabled bool
	start   time.Time
	current telemetry.Phase
	timings telemetry.PhaseTimings
}

func newExecutionPhases(start time.Time, enabled bool) executionPhases {
	p := executionPhases{enabled: enabled, start: start}
	if enabled {
		p.timings.Intervals[telemetry.PhaseValidation].Observed = true
	}
	return p
}

func (p *executionPhases) enter(phase telemetry.Phase) {
	if p.enabled {
		p.enterAt(phase, time.Now())
	}
}

func (p *executionPhases) enterAt(phase telemetry.Phase, now time.Time) {
	if !p.enabled || phase >= telemetry.PhaseCount || phase <= p.current {
		return
	}
	offset := now.Sub(p.start)
	p.timings.Intervals[p.current].End = offset
	p.current = phase
	p.timings.Intervals[phase] = telemetry.PhaseInterval{Start: offset, End: offset, Observed: true}
}

func (p *executionPhases) finish(now time.Time) telemetry.PhaseTimings {
	if p != nil && p.enabled {
		p.timings.Intervals[p.current].End = now.Sub(p.start)
		return p.timings
	}
	return telemetry.PhaseTimings{}
}

func (p *executionPhases) sink(sink query.Sink) query.Sink {
	if !p.enabled {
		return sink
	}
	return &phaseSink{sink: sink, phases: p}
}

type phaseSink struct {
	sink   query.Sink
	phases *executionPhases
}

func (s *phaseSink) Schema(schema *arrow.Schema) error {
	start := time.Now()
	defer func() { s.recordSink(time.Since(start)) }()
	return s.sink.Schema(schema)
}

func (s *phaseSink) Write(batch arrow.RecordBatch) error {
	start := time.Now()
	if !s.phases.timings.HasFirstBatch {
		s.phases.timings.HasFirstBatch = true
		s.phases.timings.FirstBatch = start.Sub(s.phases.start)
	}
	defer func() { s.recordSink(time.Since(start)) }()
	// The borrowed batch is passed synchronously, with no Retain or storage.
	return s.sink.Write(batch)
}

func (s *phaseSink) recordSink(duration time.Duration) {
	s.phases.timings.HasSink = true
	s.phases.timings.SinkDuration += duration
}
