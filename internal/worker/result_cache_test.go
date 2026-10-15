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

// fakeRead is an executeFunc that writes data to the cache buffer after delay
// and counts its executions. block, when set, holds it until closed.
type fakeRead struct {
	runs  atomic.Int32
	data  string
	delay time.Duration
	err   error
	block chan struct{}
	panic bool
}

func (f *fakeRead) execute(ctx context.Context, _ query.Sink, tee *cappedBuffer) (query.Stats, error) {
	f.runs.Add(1)
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return query.Stats{}, ctx.Err()
		}
	}
	time.Sleep(f.delay)
	if f.panic {
		panic("read exploded")
	}
	if tee != nil {
		_, _ = tee.Write([]byte(f.data))
	}
	return query.Stats{Rows: 1}, f.err
}

func replayData(got *[]string, mu *sync.Mutex) replayFunc {
	return func(e *cacheEntry, _ query.Sink) (query.Stats, error) {
		mu.Lock()
		*got = append(*got, string(e.data))
		mu.Unlock()
		return e.stats, nil
	}
}

func noReplay(e *cacheEntry, _ query.Sink) (query.Stats, error) { return e.stats, nil }

func TestResultCacheSingleflightTTLAndRefresh(t *testing.T) {
	c := newResultCache(80*time.Millisecond, 16, 1<<20)
	read := &fakeRead{data: "result", delay: 30 * time.Millisecond}
	var wg sync.WaitGroup
	var cached atomic.Int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats, err := c.do(context.Background(), "k", "t", false, nil, read.execute, noReplay)
			if err != nil {
				t.Error(err)
			}
			if stats.Cached {
				cached.Add(1)
			}
		}()
	}
	wg.Wait()
	if read.runs.Load() != 1 || c.Hits() != 9 || cached.Load() != 9 {
		t.Fatalf("ten identical reads ran %d times, %d hits, %d cached", read.runs.Load(), c.Hits(), cached.Load())
	}
	time.Sleep(100 * time.Millisecond)
	_, _ = c.do(context.Background(), "k", "t", false, nil, read.execute, noReplay)
	if read.runs.Load() != 2 {
		t.Fatal("expired result served")
	}
}

// A cache hit reports Cached with its own replay time, not the execution's.
func TestResultCacheHitReportsOwnTiming(t *testing.T) {
	c := newResultCache(time.Minute, 16, 1<<20)
	slow := &fakeRead{data: "result", delay: 100 * time.Millisecond}
	first, _ := c.do(context.Background(), "k", "t", false, nil, func(ctx context.Context, s query.Sink, tee *cappedBuffer) (query.Stats, error) {
		stats, err := slow.execute(ctx, s, tee)
		stats.DurationNS = (100 * time.Millisecond).Nanoseconds()
		return stats, err
	}, noReplay)
	hit, _ := c.do(context.Background(), "k", "t", false, nil, slow.execute, noReplay)
	if first.Cached || !hit.Cached || hit.DurationNS >= first.DurationNS {
		t.Fatalf("hit reported the original execution: first=%+v hit=%+v", first, hit)
	}
}

// Refresh coalesces only with a read that started at or after it arrived;
// otherwise it leads a new one.
func TestResultCacheRefreshCoalescing(t *testing.T) {
	c := newResultCache(time.Minute, 16, 1<<20)
	old := &fakeRead{data: "old", block: make(chan struct{})}
	go func() { _, _ = c.do(context.Background(), "k", "t", false, nil, old.execute, noReplay) }()
	waitFor(t, func() bool { return old.runs.Load() == 1 })
	fresh := &fakeRead{data: "fresh", block: make(chan struct{})}
	refreshed := make(chan struct{})
	go func() {
		_, _ = c.do(context.Background(), "k", "t", true, nil, fresh.execute, noReplay)
		close(refreshed)
	}()
	waitFor(t, func() bool { return fresh.runs.Load() == 1 })
	// A plain read joins the running fresh read.
	plain := &fakeRead{data: "never"}
	joined := make(chan struct{})
	go func() {
		_, _ = c.do(context.Background(), "k", "t", false, nil, plain.execute, noReplay)
		close(joined)
	}()
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.flights["k"] != nil && c.flights["k"].interest == 2
	})
	close(fresh.block)
	<-refreshed
	<-joined
	close(old.block)
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.flights) == 0
	})
	// The older read finished last but must not replace the newer result.
	c.mu.Lock()
	kept := string(c.entries["k"].Value.(*cacheEntry).data)
	c.mu.Unlock()
	if kept != "fresh" || old.runs.Load() != 1 || fresh.runs.Load() != 1 || plain.runs.Load() != 0 {
		t.Fatalf("kept %q (old runs %d, fresh runs %d, plain runs %d)", kept, old.runs.Load(), fresh.runs.Load(), plain.runs.Load())
	}
}

// When the leader's caller leaves, the read continues for its waiters; one
// execution serves them all.
func TestResultCacheLeaderCancelKeepsWaitersServed(t *testing.T) {
	c := newResultCache(time.Minute, 16, 1<<20)
	read := &fakeRead{data: "result", block: make(chan struct{})}
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leader := make(chan error, 1)
	go func() {
		_, err := c.do(leaderCtx, "k", "t", false, &workerTestSink{}, read.execute, noReplay)
		leader <- err
	}()
	waitFor(t, func() bool { return read.runs.Load() == 1 })
	var got []string
	var mu sync.Mutex
	waiter := make(chan error, 1)
	go func() {
		_, err := c.do(context.Background(), "k", "t", false, nil, read.execute, replayData(&got, &mu))
		waiter <- err
	}()
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		f := c.flights["k"]
		return f != nil && f.interest == 2
	})
	cancelLeader()
	if err := <-leader; !errors.Is(err, context.Canceled) {
		t.Fatal("leader did not return on its own cancellation", err)
	}
	close(read.block)
	if err := <-waiter; err != nil || len(got) != 1 || got[0] != "result" || read.runs.Load() != 1 {
		t.Fatal("waiter not served by the leader's read", err, got, read.runs.Load())
	}
}

// A failed shared read gives its waiters the same error instead of each
// running the query again; a panicking read still closes its flight.
func TestResultCacheFailureAndPanicReachWaiters(t *testing.T) {
	for name, read := range map[string]*fakeRead{
		"error": {err: errors.New("source failed"), block: make(chan struct{})},
		"panic": {panic: true, block: make(chan struct{})},
	} {
		t.Run(name, func(t *testing.T) {
			c := newResultCache(time.Minute, 16, 1<<20)
			results := make(chan error, 3)
			for i := 0; i < 3; i++ {
				go func() {
					_, err := c.do(context.Background(), "k", "t", false, nil, read.execute, noReplay)
					results <- err
				}()
			}
			waitFor(t, func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				f := c.flights["k"]
				return f != nil && f.interest == 3
			})
			close(read.block)
			for i := 0; i < 3; i++ {
				if err := <-results; err == nil {
					t.Fatal("a waiter of a failed read succeeded")
				}
			}
			if read.runs.Load() != 1 || c.lru.Len() != 0 {
				t.Fatal("failure re-run or kept", read.runs.Load(), c.lru.Len())
			}
		})
	}
}

// The store is an LRU: a hit makes an entry recent, so the least recently
// used one goes when the total would overflow.
func TestResultCacheEvictsLeastRecentlyUsed(t *testing.T) {
	one := int64(8 + 1 + cacheEntryOverhead) // 8 data bytes, 1-byte key
	c := newResultCache(time.Minute, 8, 2*one)
	c.perTenant = c.maxTotal // this test bounds the total only
	store := func(key string) {
		_, _ = c.do(context.Background(), key, "t", false, nil, (&fakeRead{data: "12345678"}).execute, noReplay)
	}
	store("a")
	store("b")
	_, _ = c.do(context.Background(), "a", "t", false, nil, (&fakeRead{}).execute, noReplay) // hit: a is recent
	store("c")
	if c.entries["a"] == nil || c.entries["b"] != nil || c.entries["c"] == nil || c.total != 2*one {
		t.Fatalf("not LRU: a=%v b=%v c=%v total=%d", c.entries["a"] != nil, c.entries["b"] != nil, c.entries["c"] != nil, c.total)
	}
}

// Entry count, the per-tenant share and in-flight buffers all bound memory.
func TestResultCacheBounds(t *testing.T) {
	c := newResultCache(time.Minute, 64, 1<<20)
	c.maxEntries = 3
	for _, k := range []string{"a", "b", "c", "d"} {
		_, _ = c.do(context.Background(), k, "t"+k, false, nil, (&fakeRead{data: "x"}).execute, noReplay)
	}
	if c.lru.Len() != 3 || c.entries["a"] != nil {
		t.Fatal("entry count not bounded", c.lru.Len())
	}

	// One tenant cannot take more than a quarter: its own oldest entries go,
	// another tenant's stay.
	c = newResultCache(time.Minute, 4096, 4*(3*(1000+1+cacheEntryOverhead)))
	data := string(make([]byte, 1000))
	_, _ = c.do(context.Background(), "z", "other", false, nil, (&fakeRead{data: data}).execute, noReplay)
	for _, k := range []string{"a", "b", "c", "d"} {
		_, _ = c.do(context.Background(), k, "big", false, nil, (&fakeRead{data: data}).execute, noReplay)
	}
	if c.byTenant["big"] > c.tenantCap() || c.entries["a"] != nil || c.entries["z"] == nil {
		t.Fatal("tenant share not enforced", c.byTenant["big"], c.tenantCap())
	}

	// A running read's buffer counts: with the budget taken by it, nothing
	// else fits until it ends.
	c = newResultCache(time.Minute, 4096, 4*2048)
	buffer := &cappedBuffer{cache: c, tenant: "t"}
	_, _ = buffer.Write(make([]byte, 2000))
	c.mu.Lock()
	inflight := c.total
	c.mu.Unlock()
	if inflight != 2000 || buffer.over {
		t.Fatal("in-flight bytes not counted", inflight)
	}
	_, _ = buffer.Write(make([]byte, 100)) // over the tenant's quarter
	c.mu.Lock()
	defer c.mu.Unlock()
	if !buffer.over || c.total != 0 {
		t.Fatal("overflowing buffer kept its reservation", buffer.over, c.total)
	}
}

func TestResultCacheKeyNeverCrossesTenants(t *testing.T) {
	r := query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}
	l := query.DefaultLimits()
	base := resultCacheKey("tenant-a", "team", "app", "db", "public", "rev", r, l)
	other := r
	other.Parameters = []query.Parameter{{Type: "int64", Value: json.RawMessage("1")}}
	small := l
	small.MaxRows = 10
	for name, key := range map[string]string{
		"tenant":     resultCacheKey("tenant-b", "team", "app", "db", "public", "rev", r, l),
		"team":       resultCacheKey("tenant-a", "other", "app", "db", "public", "rev", r, l),
		"connection": resultCacheKey("tenant-a", "team", "app2", "db", "public", "rev", r, l),
		"database":   resultCacheKey("tenant-a", "team", "app", "db2", "public", "rev", r, l),
		"schema":     resultCacheKey("tenant-a", "team", "app", "db", "sales", "rev", r, l),
		"revision":   resultCacheKey("tenant-a", "team", "app", "db", "public", "rev2", r, l),
		"parameters": resultCacheKey("tenant-a", "team", "app", "db", "public", "rev", other, l),
		"limits":     resultCacheKey("tenant-a", "team", "app", "db", "public", "rev", r, small),
	} {
		if key == base {
			t.Fatal("key ignores", name)
		}
	}
	refresh := r
	refresh.Refresh = true
	if resultCacheKey("tenant-a", "team", "app", "db", "public", "rev", refresh, l) != base {
		t.Fatal("refresh changed the key")
	}
}

// Decision, signed off by the owner: the key leaves out the principal. A
// result depends only on the source it came from; every read is authorized
// for its own principal before the cache is consulted; restricted principals
// (row policies) never reach the cache. Two principals of one tenant reading
// the same connection therefore share an entry. Changing this is a design
// decision, not a fix.
func TestResultCacheKeyIgnoresPrincipal(t *testing.T) {
	l := query.DefaultLimits()
	alice := query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1", Delegation: "grant-for-alice"}
	bob := alice
	bob.Delegation = "grant-for-bob"
	if resultCacheKey("t", "team", "app", "db", "public", "rev", alice, l) != resultCacheKey("t", "team", "app", "db", "public", "rev", bob, l) {
		t.Fatal("the principal entered the cache key")
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
	if requests.Load() != 1 || mgr.cache.Hits() != 9 {
		t.Fatalf("worker requests=%d cache hits=%d", requests.Load(), mgr.cache.Hits())
	}
	sink := &workerTestSink{}
	if _, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1", Refresh: true}, sink); err != nil || sink.rows != 1 || requests.Load() != 2 {
		t.Fatal("refresh did not reach the worker", err, requests.Load())
	}
}
