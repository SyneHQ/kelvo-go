// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/apache/arrow-go/v18/arrow"
)

func TestPhaseClockPartitionsWorkerLifecycle(t *testing.T) {
	start := time.Now()
	p := newExecutionPhases(start, true)
	for i, phase := range []telemetry.Phase{telemetry.PhaseNodeAdmission, telemetry.PhaseSourceAdmission, telemetry.PhasePrepare, telemetry.PhaseExecutionDelivery, telemetry.PhaseCleanup} {
		p.enterAt(phase, start.Add(time.Duration(i+1)*time.Second))
	}
	timings := p.finish(start.Add(6 * time.Second))
	if !timings.Valid(6 * time.Second) {
		t.Fatalf("invalid monotonic timeline: %+v", timings)
	}
	var sum time.Duration
	for _, interval := range timings.Intervals {
		if !interval.Observed || interval.End-interval.Start != time.Second {
			t.Fatalf("overlap or missing phase: %+v", interval)
		}
		sum += interval.End - interval.Start
	}
	if sum != 6*time.Second || timings.HasFirstBatch || timings.HasSink {
		t.Fatal("invented first batch, sink time, or overlapping total")
	}
}

func TestDisabledPhasesDoNotWrapSink(t *testing.T) {
	p := newExecutionPhases(time.Time{}, false)
	sink := &workerTestSink{}
	p.enter(telemetry.PhasePrepare)
	if p.sink(sink) != sink || p.finish(time.Now()) != (telemetry.PhaseTimings{}) {
		t.Fatal("disabled instrumentation wrapped or retained the sink")
	}
	var absent *executionPhases
	if absent.finish(time.Now()) != (telemetry.PhaseTimings{}) {
		t.Fatal("legacy caller fabricated phase observations")
	}
}

type failedPhaseSink struct{ err error }

func (s failedPhaseSink) Schema(*arrow.Schema) error    { return nil }
func (s failedPhaseSink) Write(arrow.RecordBatch) error { return s.err }

func TestExecutorPhasesSuccessSinkErrorAndCancellation(t *testing.T) {
	for _, name := range []string{"success", "sink_error", "cancellation", "invalid"} {
		t.Run(name, func(t *testing.T) {
			e, err := New(catalog.Config{}, query.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			e.Metrics = telemetry.New()
			ctx := context.Background()
			req := query.Request{SQL: "SELECT 1"}
			var sink query.Sink = &workerTestSink{}
			sinkErr := errors.New("private downstream failure")
			switch name {
			case "sink_error":
				sink = failedPhaseSink{sinkErr}
			case "cancellation":
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 200*time.Millisecond)
				defer cancel()
				req.SQL = "SELECT wait"
			case "invalid":
				req.SQL = ""
			}
			_, err = e.Execute(ctx, req, sink)
			if (name == "success") != (err == nil) {
				t.Fatalf("unexpected result: %v", err)
			}
			s := e.Metrics.Snapshot()
			p := s.Phases[telemetry.KindQuery]
			if p[telemetry.PhaseValidation].Count != 1 || p[telemetry.PhaseNodeAdmission].Count != 0 || p[telemetry.PhaseSourceAdmission].Count != 0 {
				t.Fatalf("wrong reached phases: %+v", p)
			}
			if name == "invalid" {
				if p[telemetry.PhasePrepare].Count != 0 || p[telemetry.PhaseExecutionDelivery].Count != 0 || p[telemetry.PhaseCleanup].Count != 0 {
					t.Fatal("validation error invented execution phases")
				}
			} else if p[telemetry.PhasePrepare].Count != 1 || p[telemetry.PhaseExecutionDelivery].Count != 1 || p[telemetry.PhaseCleanup].Count != 1 {
				t.Fatalf("terminal result lost reached phases: %+v", p)
			}
			wantFirst := uint64(0)
			if name == "success" || name == "sink_error" {
				wantFirst = 1
			}
			if s.FirstBatch[telemetry.KindQuery].Count != wantFirst {
				t.Fatalf("first-batch counts included no-record failure: %+v", s.FirstBatch)
			}
			if s.SinkDuration[telemetry.KindQuery].SumSeconds > p[telemetry.PhaseExecutionDelivery].SumSeconds {
				t.Fatal("sink subset exceeds enclosing execution/delivery")
			}
			var sum float64
			for _, phase := range p {
				sum += phase.SumSeconds
			}
			if total := s.QueueWait[telemetry.KindQuery].SumSeconds + s.Duration[telemetry.KindQuery].SumSeconds; sum < total-1e-9 || sum > total+1e-9 {
				t.Fatalf("phases overlap or omit elapsed time: phases=%g total=%g", sum, total)
			}
		})
	}
}

func TestRejectedAdmissionStillRecordsReachedPhases(t *testing.T) {
	start := time.Now().Add(-time.Second)
	p := newExecutionPhases(start, true)
	p.enterAt(telemetry.PhaseNodeAdmission, start.Add(100*time.Millisecond))
	metrics := telemetry.New()
	wait := 900 * time.Millisecond
	err := admission.ErrDraining
	recordExecutionPhases(metrics, context.Background(), start, &wait, &err, &p)
	s := metrics.Snapshot()
	if s.Rejections[telemetry.KindQuery][telemetry.RejectionDraining] != 1 || s.Phases[telemetry.KindQuery][telemetry.PhaseNodeAdmission].Count != 1 || s.Outcomes != ([2][3]uint64{}) {
		t.Fatal("rejected admission was lost or counted as execution")
	}
}
