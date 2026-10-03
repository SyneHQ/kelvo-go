// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package tracing

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

func TestExactPhaseSpansKeepMonotonicOffsetsAndPrivacy(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "sql=private,tenant=private")
	exporter := &captureExporter{}
	r := newRecorder(Config{SampleRatio: 1, QueueSize: 16}, exporter)
	start := time.Now().Add(-6 * time.Second)
	var timings telemetry.PhaseTimings
	for phase := telemetry.Phase(0); phase < telemetry.PhaseCount; phase++ {
		timings.Intervals[phase] = telemetry.PhaseInterval{Start: time.Duration(phase) * time.Second, End: time.Duration(phase+1) * time.Second, Observed: true}
	}
	r.Record(Event{Kind: telemetry.KindQuery, StartedAt: start, AdmissionWait: 2 * time.Second, Duration: 4 * time.Second, Phases: timings})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if len(exporter.spans) != 7 {
		t.Fatalf("expected parent and exact phases, got %d", len(exporter.spans))
	}
	seen := map[string]bool{}
	for _, span := range exporter.spans {
		seen[span.Name()] = true
		if span.Name() == "kelvo.query" {
			continue
		}
		if !strings.HasPrefix(span.Name(), "kelvo.phase.") || len(span.Attributes()) != 0 || len(span.Events()) != 0 || len(span.Links()) != 0 || len(span.Resource().Attributes()) != 1 || !span.Parent().IsValid() {
			t.Fatal("phase trace contains dynamic data or missing parent")
		}
		found := false
		for phase, interval := range timings.Intervals {
			if span.Name() == "kelvo.phase."+telemetry.Phase(phase).Name() {
				found = true
				if !span.StartTime().Equal(start.Add(interval.Start)) || !span.EndTime().Equal(start.Add(interval.End)) {
					t.Fatal("exact phase offset was replaced by aggregate admission position")
				}
			}
		}
		if !found {
			t.Fatal("unexpected phase name")
		}
	}
	if seen["kelvo.admission"] {
		t.Fatal("aggregate admission child overlaps exact phase timeline")
	}
}

func TestMalformedPhaseTimelineFallsBackToAggregate(t *testing.T) {
	exporter := &captureExporter{}
	r := newRecorder(Config{SampleRatio: 1, QueueSize: 8}, exporter)
	var phases telemetry.PhaseTimings
	phases.Intervals[telemetry.PhaseValidation] = telemetry.PhaseInterval{Start: -time.Second, Observed: true}
	r.Record(Event{Kind: telemetry.KindQuery, StartedAt: time.Now(), AdmissionWait: time.Second, Duration: time.Second, Phases: phases})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if len(exporter.spans) != 2 {
		t.Fatal("invalid phase changed valid lifecycle span count")
	}
	for _, span := range exporter.spans {
		if strings.HasPrefix(span.Name(), "kelvo.phase.") {
			t.Fatal("invalid phase exported")
		}
	}
}
