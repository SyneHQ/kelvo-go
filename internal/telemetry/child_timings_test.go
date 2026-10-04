// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import (
	"bytes"
	"context"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func childTimingFixture() ChildTiming {
	return ChildTiming{Version: ChildTimingVersion, TotalNS: 100, Stages: [ChildStageCount]ChildInterval{
		{0, 10, true}, {10, 80, true}, {80, 90, true}, {90, 100, true},
		{10, 20, true}, {20, 50, true}, {50, 70, true},
	}}
}

func TestChildTimingValidatesSeparateLanesAndParentBound(t *testing.T) {
	valid := childTimingFixture()
	if !valid.Valid(100) || valid.Valid(99) || valid.Valid(-1) {
		t.Fatal("child timing escaped parent subprocess bound")
	}
	cases := map[string]func(*ChildTiming){
		"unknown version":               func(r *ChildTiming) { r.Version++ },
		"negative total":                func(r *ChildTiming) { r.TotalNS = -1 },
		"negative offset":               func(r *ChildTiming) { r.Stages[ChildWorkerSetup].StartNS = -1 },
		"reverse interval":              func(r *ChildTiming) { r.Stages[ChildWorkerSetup].EndNS = -1 },
		"outer overlap":                 func(r *ChildTiming) { r.Stages[ChildIPCFinalize].StartNS = 79 },
		"inner overlap":                 func(r *ChildTiming) { r.Stages[ChildMaterialization].StartNS = 19 },
		"inner precedes call":           func(r *ChildTiming) { r.Stages[ChildEngineSetup].StartNS = 9 },
		"inner exceeds call":            func(r *ChildTiming) { r.Stages[ChildArrowDrain].EndNS = 81 },
		"cleanup absent":                func(r *ChildTiming) { r.Stages[ChildWorkerCleanup] = ChildInterval{} },
		"setup absent":                  func(r *ChildTiming) { r.Stages[ChildWorkerSetup] = ChildInterval{} },
		"cleanup after total":           func(r *ChildTiming) { r.Stages[ChildWorkerCleanup].EndNS = 101 },
		"noncanonical absent":           func(r *ChildTiming) { r.Stages[ChildEngineSetup].Observed = false },
		"inner without executor":        func(r *ChildTiming) { r.Stages[ChildExecutorCall] = ChildInterval{} },
		"materialization without setup": func(r *ChildTiming) { r.Stages[ChildEngineSetup] = ChildInterval{} },
		"drain without materialization": func(r *ChildTiming) { r.Stages[ChildMaterialization] = ChildInterval{} },
		"offset overflow":               func(r *ChildTiming) { r.Stages[ChildArrowDrain] = ChildInterval{math.MinInt64, math.MaxInt64, true} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			report := valid
			mutate(&report)
			if report.Valid(100) {
				t.Fatal("malformed child timing accepted")
			}
		})
	}
	// Native and failed-preparation calls retain the combined executor duration.
	for stage := ChildEngineSetup; stage < ChildStageCount; stage++ {
		valid.Stages[stage] = ChildInterval{}
	}
	if !valid.Valid(100) {
		t.Fatal("unobserved nested stages invalidated combined executor scope")
	}
	valid.Stages[ChildExecutorCall], valid.Stages[ChildIPCFinalize] = ChildInterval{}, ChildInterval{}
	if !valid.Valid(100) {
		t.Fatal("constructor failure cannot report setup and cleanup")
	}
	valid.TotalNS = math.MaxInt64
	valid.Stages[ChildWorkerCleanup].EndNS = math.MaxInt64
	if valid.Valid(time.Duration(math.MaxInt64)) {
		t.Fatal("unbounded child duration accepted")
	}
}

func TestChildRecorderClosedIntervalsAndCleanup(t *testing.T) {
	r := NewChildRecorder()
	r.Begin(ChildWorkerSetup)
	r.End(ChildWorkerSetup)
	r.Begin(ChildExecutorCall)
	r.Begin(ChildEngineSetup)
	// Failed setup remains unknown; Finish must not close it through cleanup.
	r.End(ChildExecutorCall)
	r.Begin(ChildWorkerCleanup)
	partial := r.Snapshot()
	if !partial.Stages[ChildExecutorCall].Observed || partial.Stages[ChildEngineSetup] != (ChildInterval{}) || partial.Stages[ChildWorkerCleanup] != (ChildInterval{}) {
		t.Fatal("recorder invented completion for an active stage")
	}
	r.End(ChildWorkerCleanup)
	report := r.Finish()
	if report == nil || !report.Valid(time.Duration(report.TotalNS)) || report.Stages[ChildEngineSetup] != (ChildInterval{}) {
		t.Fatal("cleanup-complete report lost combined executor scope")
	}
	if again := r.Finish(); again == report || *again != *report {
		t.Fatal("Finish must return independent copies of a stable report")
	}
}

func TestChildRecorderRejectsIncompleteAndRepeatedBoundaries(t *testing.T) {
	for _, repeat := range []bool{false, true} {
		r := NewChildRecorder()
		r.Begin(ChildWorkerSetup)
		if repeat {
			r.End(ChildWorkerSetup)
			r.End(ChildWorkerSetup)
		}
		r.Begin(ChildWorkerCleanup)
		r.End(ChildWorkerCleanup)
		if r.Finish() != nil {
			t.Fatal("incomplete or repeated worker boundary accepted")
		}
	}
}

func TestChildRecorderDisabledAndConcurrentSnapshots(t *testing.T) {
	ctx := context.Background()
	var disabled *ChildRecorder
	if got := testing.AllocsPerRun(100, func() {
		disabled.Begin(ChildWorkerSetup)
		disabled.End(ChildWorkerSetup)
		if disabled.Finish() != nil || WithChildRecorder(ctx, disabled) != ctx {
			t.Fatal("disabled recorder changed context or emitted timing")
		}
	}); got != 0 {
		t.Fatalf("disabled recorder allocated: %g", got)
	}
	if ChildRecorderFromContext(nil) != nil || ChildRecorderFromContext(ctx) != nil {
		t.Fatal("disabled context supplied a child recorder")
	}
	r := NewChildRecorder()
	if ChildRecorderFromContext(WithChildRecorder(ctx, r)) != r {
		t.Fatal("private recorder did not survive context transport")
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; i < 1000; i++ {
			_ = r.Snapshot()
		}
	})
	r.Begin(ChildWorkerSetup)
	r.End(ChildWorkerSetup)
	r.Begin(ChildWorkerCleanup)
	r.End(ChildWorkerCleanup)
	if r.Finish() == nil {
		t.Fatal("concurrent snapshot changed recording")
	}
	wg.Wait()
}

func TestChildMetricsSeparateNestedSamplesFromUnknowns(t *testing.T) {
	r := New()
	report := childTimingFixture()
	report.Stages[ChildArrowDrain] = ChildInterval{}
	report.Stages[ChildIPCFinalize].EndNS = report.Stages[ChildIPCFinalize].StartNS
	r.ObserveChildTiming(KindQuery, &report, ChildTimingObserved, 100)
	r.ObserveChildTiming(KindQuery, nil, ChildTimingMissing, 100)
	r.ObserveChildTiming(KindQuery, nil, ChildTimingMalformed, 100)
	r.ObserveChildTiming(KindQuery, nil, ChildTimingTerminated, 100)
	r.ObserveChildTiming(KindRefresh, &report, ChildTimingObserved, 99)
	s := r.Snapshot()
	if s.ChildWorkerStages[KindQuery][ChildExecutorCall].Count != 1 || s.ChildDuckDBStages[KindQuery][ChildMaterialization-ChildEngineSetup].Count != 1 {
		t.Fatal("worker and nested DuckDB observations were not separate")
	}
	if s.ChildDuckDBStages[KindQuery][ChildArrowDrain-ChildEngineSetup].Count != 0 || s.ChildUnknown[KindQuery][ChildArrowDrain][ChildTimingObserved] != 1 {
		t.Fatal("unknown Arrow drain became a zero-duration observation")
	}
	if h := s.ChildWorkerStages[KindQuery][ChildIPCFinalize]; h.Count != 1 || h.SumSeconds != 0 {
		t.Fatal("observed zero-duration stage was treated as unknown")
	}
	for status := ChildTimingObserved; status < childTimingStatusCount; status++ {
		if s.ChildReports[KindQuery][status] != 1 {
			t.Fatalf("missing fixed report status %d", status)
		}
	}
	if s.ChildReports[KindRefresh][ChildTimingMalformed] != 1 || s.ChildWorkerStages[KindRefresh][ChildExecutorCall].Count != 0 {
		t.Fatal("invalid parent bound produced child measurements")
	}
	r.ObserveChildTiming(Kind(255), nil, ChildTimingMissing, 100)
	r.ObserveChildTiming(KindQuery, nil, ChildTimingStatus(255), 100)
	if r.Snapshot() != s {
		t.Fatal("arbitrary enums entered metric state")
	}
	var out bytes.Buffer
	if err := writeMetrics(&out, s); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# TYPE kelvo_child_worker_stage_seconds histogram\n",
		"# TYPE kelvo_child_duckdb_stage_seconds histogram\n",
		`kelvo_child_stage_unknown_total{kind="query",lane="duckdb",stage="arrow_drain",reason="unobserved"} 1`,
		`kelvo_child_timing_reports_total{kind="query",status="terminated"} 1`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing fixed metric %q", want)
		}
	}
}
