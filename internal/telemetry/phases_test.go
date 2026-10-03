// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func validPhases() PhaseTimings {
	var p PhaseTimings
	p.Intervals[PhaseValidation] = PhaseInterval{Start: 0, End: time.Second, Observed: true}
	p.Intervals[PhasePrepare] = PhaseInterval{Start: time.Second, End: 2 * time.Second, Observed: true}
	p.Intervals[PhaseExecutionDelivery] = PhaseInterval{Start: 2 * time.Second, End: 5 * time.Second, Observed: true}
	p.Intervals[PhaseCleanup] = PhaseInterval{Start: 5 * time.Second, End: 6 * time.Second, Observed: true}
	p.HasFirstBatch, p.FirstBatch = true, 3*time.Second
	p.HasSink, p.SinkDuration = true, time.Second
	return p
}

func TestPhaseMetricsReachedIntervalsAndBoundedLabels(t *testing.T) {
	r := New()
	p := validPhases()
	r.ObservePhases(KindQuery, p, 6*time.Second)
	s := r.Snapshot()
	if s.Phases[KindQuery][PhaseNodeAdmission].Count != 0 || s.Phases[KindQuery][PhaseSourceAdmission].Count != 0 {
		t.Fatal("unreached admission phases fabricated zero-duration observations")
	}
	if s.Phases[KindQuery][PhaseExecutionDelivery].SumSeconds != 3 || s.FirstBatch[KindQuery].SumSeconds != 3 || s.SinkDuration[KindQuery].SumSeconds != 1 {
		t.Fatalf("wrong interval semantics: %+v", s)
	}
	if s.Outcomes != ([2][3]uint64{}) || s.Duration != ([2]Histogram{}) {
		t.Fatal("phase recording duplicated terminal lifecycle observations")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		`kelvo_worker_phase_seconds_sum{kind="query",phase="execution_delivery"} 3`,
		`kelvo_worker_phase_seconds_count{kind="query",phase="node_admission"} 0`,
		`kelvo_worker_first_batch_seconds_count{kind="query"} 1`,
		`kelvo_worker_sink_seconds_sum{kind="query"} 1`,
	} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("missing %q", want)
		}
	}
	for _, disallowed := range []string{"query_id=", "tenant=", "source=", "sql=", "request="} {
		if strings.Contains(w.Body.String(), disallowed) {
			t.Fatalf("unbounded metric label %s", disallowed)
		}
	}
}

func TestInvalidPhaseSamplesAreIgnored(t *testing.T) {
	for _, change := range []func(*PhaseTimings){
		func(p *PhaseTimings) { p.Intervals[PhasePrepare].Start = 0 },
		func(p *PhaseTimings) { p.Intervals[PhasePrepare].End = -1 },
		func(p *PhaseTimings) { p.Intervals[PhaseCleanup].End = 7 * time.Second },
		func(p *PhaseTimings) { p.FirstBatch = time.Second },
		func(p *PhaseTimings) { p.FirstBatch = 7 * time.Second },
		func(p *PhaseTimings) { p.SinkDuration = -1 },
		func(p *PhaseTimings) { p.SinkDuration = 4 * time.Second },
	} {
		r := New()
		p := validPhases()
		change(&p)
		r.ObservePhases(KindQuery, p, 6*time.Second)
		if r.Snapshot() != (Snapshot{}) {
			t.Fatal("malformed phase timeline was recorded")
		}
	}
	r := New()
	r.ObservePhases(Kind(255), validPhases(), 6*time.Second)
	r.ObservePhases(KindQuery, validPhases(), -1)
	r.ObservePhases(KindQuery, validPhases(), 25*time.Hour)
	var disabled *Registry
	disabled.ObservePhases(KindQuery, validPhases(), 6*time.Second)
	if r.Snapshot() != (Snapshot{}) || Phase(255).Name() != "" {
		t.Fatal("invalid kind, timing or phase became a metric")
	}
}

func TestConcurrentPhaseObserveAndScrape(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 50 {
				r.ObservePhases(KindQuery, validPhases(), 6*time.Second)
				_ = r.Snapshot()
			}
		})
	}
	wg.Wait()
	if got := r.Snapshot().Phases[KindQuery][PhaseExecutionDelivery].Count; got != 800 {
		t.Fatalf("lost concurrent observations: %d", got)
	}
}

func BenchmarkObservePhases(b *testing.B) {
	r := New()
	p := validPhases()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.ObservePhases(KindQuery, p, 6*time.Second)
		}
	})
}
