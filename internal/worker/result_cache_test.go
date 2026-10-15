// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter/frame"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func cacheRun(runs *atomic.Int32, data string, delay time.Duration, err error) func(*cappedBuffer) (query.Stats, error, bool) {
	return func(b *cappedBuffer) (query.Stats, error, bool) {
		runs.Add(1)
		time.Sleep(delay)
		if b != nil {
			_, _ = b.Write([]byte(data))
		}
		return query.Stats{Rows: 1}, err, err == nil
	}
}

func replayed(served *atomic.Int32) func(*cacheEntry) (query.Stats, error) {
	return func(e *cacheEntry) (query.Stats, error) { served.Add(1); return e.stats, nil }
}

func TestResultCacheSingleflightTTLAndRefresh(t *testing.T) {
	c := newResultCache(80*time.Millisecond, 16, 64)
	var runs, served atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.do(context.Background(), "k", false, cacheRun(&runs, "result", 30*time.Millisecond, nil), replayed(&served)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if runs.Load() != 1 || served.Load() != 9 {
		t.Fatalf("ten identical reads ran %d times, %d replayed", runs.Load(), served.Load())
	}
	_, _ = c.do(context.Background(), "k", false, cacheRun(&runs, "result", 0, nil), replayed(&served))
	if runs.Load() != 1 {
		t.Fatal("cached result not served within ttl")
	}
	_, _ = c.do(context.Background(), "k", true, cacheRun(&runs, "fresh", 0, nil), replayed(&served))
	if runs.Load() != 2 || string(c.entries["k"].data) != "fresh" {
		t.Fatal("refresh did not bypass and replace the cache")
	}
	time.Sleep(100 * time.Millisecond)
	_, _ = c.do(context.Background(), "k", false, cacheRun(&runs, "result", 0, nil), replayed(&served))
	if runs.Load() != 3 {
		t.Fatal("expired result served")
	}
}

func TestResultCacheBoundsAndFailures(t *testing.T) {
	c := newResultCache(time.Minute, 8, 16)
	var runs, served atomic.Int32
	// Over max entry: served to its caller, not kept.
	_, _ = c.do(context.Background(), "big", false, cacheRun(&runs, "123456789", 0, nil), replayed(&served))
	if c.entries["big"] != nil {
		t.Fatal("oversized result cached")
	}
	// Failures are never kept; waiters run their own read.
	boom := errors.New("boom")
	var wg sync.WaitGroup
	var failures atomic.Int32
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.do(context.Background(), "bad", false, cacheRun(&runs, "x", 20*time.Millisecond, boom), replayed(&served)); errors.Is(err, boom) {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	if c.entries["bad"] != nil || failures.Load() != 3 {
		t.Fatal("failure cached or not reported", failures.Load())
	}
	// Total bound evicts the oldest.
	for _, k := range []string{"a", "b", "c"} {
		_, _ = c.do(context.Background(), k, false, cacheRun(&runs, "12345678", 0, nil), replayed(&served))
	}
	if c.entries["a"] != nil || c.entries["b"] == nil || c.entries["c"] == nil || c.total != 16 || len(c.order) != 2 {
		t.Fatalf("total bound: total=%d order=%v", c.total, c.order)
	}
}

func TestResultCacheKeyNeverCrossesTenants(t *testing.T) {
	r := query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}
	l := query.DefaultLimits()
	base := resultCacheKey("tenant-a", "team", "app", "rev", r, l)
	other := r
	other.Parameters = []query.Parameter{{Type: "int64", Value: json.RawMessage("1")}}
	for name, key := range map[string]string{
		"tenant":     resultCacheKey("tenant-b", "team", "app", "rev", r, l),
		"team":       resultCacheKey("tenant-a", "other", "app", "rev", r, l),
		"connection": resultCacheKey("tenant-a", "team", "app2", "rev", r, l),
		"revision":   resultCacheKey("tenant-a", "team", "app", "rev2", r, l),
		"parameters": resultCacheKey("tenant-a", "team", "app", "rev", other, l),
	} {
		if key == base {
			t.Fatal("key ignores", name)
		}
	}
	refresh := r
	refresh.Refresh = true
	if resultCacheKey("tenant-a", "team", "app", "rev", refresh, l) != base {
		t.Fatal("refresh changed the key")
	}
}

// Through the executor: ten viewers of one panel produce one worker request,
// and every viewer receives the full result.
func TestReadWorkerCacheServesTenViewersOnce(t *testing.T) {
	fs := &fakeSet{}
	var requests atomic.Int32
	fs.serve = func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type == frame.Request {
				requests.Add(1)
				time.Sleep(30 * time.Millisecond)
				replyOK(p.stdoutW, f.Stream, f.Payload)
			}
		})
	}
	e, mgr := readWorkerExecutor(t, fs)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink := &workerTestSink{}
			stats, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}, sink)
			if err != nil || sink.rows != 1 || stats.Rows != 1 {
				t.Error(stats, err, sink.rows)
			}
		}()
	}
	wg.Wait()
	if requests.Load() != 1 || mgr.cache.hits != 9 {
		t.Fatalf("worker requests=%d cache hits=%d", requests.Load(), mgr.cache.hits)
	}
	sink := &workerTestSink{}
	if _, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1", Refresh: true}, sink); err != nil || sink.rows != 1 || requests.Load() != 2 {
		t.Fatal("refresh did not reach the worker", err, requests.Load())
	}
}
