// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConcurrentObserveAndSnapshot(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	for n := 0; n < 20; n++ {
		wg.Go(func() {
			for i := 0; i < 100; i++ {
				r.Observe(KindQuery, OutcomeSuccess, time.Millisecond, 2*time.Second)
				r.Reject(KindRefresh, RejectionCapacity)
				_ = r.Snapshot()
			}
		})
	}
	wg.Wait()
	s := r.Snapshot()
	if s.Outcomes[KindQuery][OutcomeSuccess] != 2000 || s.Rejections[KindRefresh][RejectionCapacity] != 2000 {
		t.Fatalf("lost counts: %+v", s)
	}
	if s.QueueWait[KindQuery].Buckets[0] != 2000 || s.Duration[KindQuery].Buckets[6] != 0 || s.Duration[KindQuery].Buckets[7] != 2000 {
		t.Fatalf("incorrect cumulative histograms: %+v", s)
	}
	if s.Duration[KindQuery].SumSeconds != 4000 {
		t.Fatalf("duration sum: %v", s.Duration[KindQuery].SumSeconds)
	}
	s.Outcomes[KindQuery][OutcomeSuccess] = 0
	if r.Snapshot().Outcomes[KindQuery][OutcomeSuccess] != 2000 {
		t.Fatal("snapshot aliased registry")
	}
}

func TestNilInvalidAndNegative(t *testing.T) {
	var disabled *Registry
	disabled.Observe(KindQuery, OutcomeSuccess, 0, 0)
	disabled.Reject(KindQuery, RejectionCapacity)
	if disabled.Snapshot() != (Snapshot{}) {
		t.Fatal("nil registry not empty")
	}
	r := New()
	r.Observe(Kind(255), OutcomeSuccess, 0, 0)
	r.Observe(KindQuery, Outcome(255), 0, 0)
	r.Reject(Kind(255), RejectionCapacity)
	r.Reject(KindQuery, Rejection(255))
	if r.Snapshot() != (Snapshot{}) {
		t.Fatal("invalid enum recorded")
	}
	r.Observe(KindRefresh, OutcomeCanceled, -time.Second, -time.Second)
	s := r.Snapshot()
	if s.QueueWait[KindRefresh].SumSeconds != 0 || s.Duration[KindRefresh].Buckets[0] != 1 {
		t.Fatalf("negative intervals: %+v", s)
	}
}

func TestPrometheusResponse(t *testing.T) {
	r := New()
	r.Observe(KindQuery, OutcomeError, time.Second, 301*time.Second)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != 200 || w.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("headers: %+v", w.Result())
	}
	for _, want := range []string{
		"# TYPE kelvo_jobs_completed_total counter\n",
		"kelvo_jobs_completed_total{kind=\"query\",outcome=\"error\"} 1\n",
		"# TYPE kelvo_job_duration_seconds histogram\n",
		"kelvo_job_duration_seconds_bucket{kind=\"query\",le=\"300\"} 0\n",
		"kelvo_job_duration_seconds_bucket{kind=\"query\",le=\"+Inf\"} 1\n",
		"kelvo_job_duration_seconds_sum{kind=\"query\"} 301\n",
		"kelvo_job_duration_seconds_count{kind=\"query\"} 1\n",
	} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("missing %q", want)
		}
	}
	if len(w.Body.Bytes()) > 65536 {
		t.Fatal("unexpected export size")
	}
	for _, method := range []string{http.MethodHead, http.MethodPost} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(method, "/metrics", nil))
		if method == http.MethodHead && (w.Code != 200 || w.Body.Len() != 0) {
			t.Fatal("invalid HEAD")
		}
		if method == http.MethodPost && (w.Code != 405 || w.Header().Get("Allow") != "GET, HEAD") {
			t.Fatal("invalid method accepted")
		}
	}
}

type stalledWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w *stalledWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return 0, errors.New("disconnected scraper")
}

func TestStalledExportDoesNotBlockRecording(t *testing.T) {
	r := New()
	w := &stalledWriter{make(chan struct{}), make(chan struct{})}
	done := make(chan struct{})
	go func() { defer close(done); _ = writeMetrics(w, r.Snapshot()) }()
	defer func() { close(w.release); <-done }()
	<-w.entered
	recorded := make(chan struct{})
	go func() { r.Observe(KindQuery, OutcomeSuccess, 0, time.Second); close(recorded) }()
	select {
	case <-recorded:
	case <-time.After(time.Second):
		t.Fatal("scraper blocked recording")
	}
}

func BenchmarkObserve(b *testing.B) {
	r := New()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Observe(KindQuery, OutcomeSuccess, time.Millisecond, time.Second)
		}
	})
}
