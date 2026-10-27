// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// snapshotFixture runs reads of one key resolved at generation, against a
// dataset whose current generation is *current.
func snapshotFixture(c *resultCache, generation string, current *string) *snapshotRead {
	gens := []datasetGeneration{{"orders", generation}}
	return &snapshotRead{cache: c, key: resultCacheKey(cacheKey{Tenant: "t", Generations: gens}, query.Request{SQL: "SELECT 1"}, query.DefaultLimits()),
		tenant: "t", ttl: time.Minute, fence: func() string { return *current }, token: generationToken(gens)}
}

// runSnapshot serves s or fills it with data, counting executions.
func runSnapshot(t *testing.T, s *snapshotRead, data string, bytes int64, executions *int) bool {
	t.Helper()
	if _, err, hit := s.serve(func([]byte) (query.Stats, error) { return query.Stats{}, nil }); hit {
		return err == nil
	}
	*executions++
	_, _ = s.tee(strings.NewReader(data)).Read(make([]byte, len(data)))
	s.finish(query.Stats{Bytes: bytes}, nil)
	return false
}

func TestSnapshotCacheHitAndAvoidedExecution(t *testing.T) {
	c := newResultCache(time.Minute, 1<<10, 1<<20)
	current, runs := "g1", 0
	for i := 0; i < 10; i++ {
		runSnapshot(t, snapshotFixture(c, "g1", &current), "rows", 4, &runs)
	}
	if runs != 1 || c.Hits() != 9 || c.fills != 0 {
		t.Fatalf("executions=%d hits=%d fills=%d", runs, c.Hits(), c.fills)
	}
}

// A refresh publication between fill and serve: the kept g1 result is refused.
func TestSnapshotCacheRefreshBetweenFillAndServeMisses(t *testing.T) {
	c := newResultCache(time.Minute, 1<<10, 1<<20)
	current, runs := "g1", 0
	runSnapshot(t, snapshotFixture(c, "g1", &current), "old", 3, &runs)
	current = "g2" // published; this reader still resolved g1 before the publication
	if runSnapshot(t, snapshotFixture(c, "g1", &current), "old", 3, &runs) || runs != 2 {
		t.Fatal("a superseded generation was served", runs)
	}
	c.mu.Lock()
	n := len(c.entries)
	c.mu.Unlock()
	if n != 0 {
		t.Fatal("stale entry kept or refilled", n)
	}
	if runSnapshot(t, snapshotFixture(c, "g2", &current), "new", 3, &runs) || !runSnapshot(t, snapshotFixture(c, "g2", &current), "new", 3, &runs) {
		t.Fatal("the current generation is not cached")
	}
}

// A refresh published while the fill ran: not kept.
func TestSnapshotCacheRefreshDuringFillNotKept(t *testing.T) {
	c := newResultCache(time.Minute, 1<<10, 1<<20)
	current := "g1"
	s := snapshotFixture(c, "g1", &current)
	if _, _, hit := s.serve(nil); hit || s.buffer == nil {
		t.Fatal("no fill started")
	}
	_, _ = s.tee(strings.NewReader("x")).Read(make([]byte, 1))
	current = "g2"
	s.finish(query.Stats{}, nil)
	if len(c.entries) != 0 || c.total != 0 || c.fills != 0 {
		t.Fatal("overtaken fill kept or leaked", len(c.entries), c.total, c.fills)
	}
}

func TestResultCacheDecodedByteBound(t *testing.T) {
	c := newResultCache(time.Minute, 1<<10, 1<<20)
	c.maxDecoded = 100
	current, runs := "g1", 0
	runSnapshot(t, snapshotFixture(c, "g1", &current), "x", 101, &runs)
	runSnapshot(t, snapshotFixture(c, "g1", &current), "x", 101, &runs)
	if runs != 2 || len(c.entries) != 0 {
		t.Fatal("result over cache_max_bytes kept", runs)
	}
	// The shared path applies the same bound.
	big := &fakeRead{data: "x"}
	exec := func(ctx context.Context, s query.Sink, tee *cappedBuffer) (query.Stats, error) {
		st, err := big.execute(ctx, s, tee)
		st.Bytes = 101
		return st, err
	}
	_, _ = c.do(context.Background(), "k", "t", false, nil, exec, noReplay)
	_, _ = c.do(context.Background(), "k", "t", false, nil, exec, noReplay)
	if big.runs.Load() != 2 {
		t.Fatal("shared path kept a result over cache_max_bytes")
	}
}

// Off by default, and never for live sources or live-routed datasets.
func TestSnapshotReadQualification(t *testing.T) {
	r := query.Request{Mode: "federated", Sources: []string{"orders"}, SQL: "SELECT 1"}
	v := []query.AccelerationVersion{{Dataset: "orders", Generation: "g1"}}
	if (&Executor{}).snapshotReadFor(context.Background(), delegation.Execution{}, false, r, v) != nil {
		t.Fatal("cached without a pool")
	}
	if DefaultWarmPoolConfig().CacheSnapshotGenerations {
		t.Fatal("snapshot caching on by default")
	}
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled, cfg.CacheSnapshotGenerations, cfg.CacheDisabled = true, true, true
	if cfg.Validate() == nil {
		t.Fatal("snapshot caching accepted with the cache off")
	}
	cfg = DefaultWarmPoolConfig()
	cfg.Enabled = true
	cfg.CacheMaxBytes = 2 << 30
	if cfg.Validate() == nil {
		t.Fatal("cache_max_bytes over 1 GiB accepted")
	}
	e := &Executor{WarmPool: &WarmPoolManager{cfg: WarmPoolConfig{CacheSnapshotGenerations: true}, cache: newResultCache(time.Minute, 1, 1)}}
	for name, versions := range map[string][]query.AccelerationVersion{
		"live routed":   {{Dataset: "orders", RoutedLive: true}},
		"live source":   nil,
		"mixed sources": v,
	} {
		req := r
		if name == "mixed sources" {
			req.Sources = []string{"orders", "live"}
		}
		if e.snapshotReadFor(context.Background(), delegation.Execution{}, false, req, versions) != nil {
			t.Fatal("cached", name)
		}
	}
}

// Review #1: a parameter whose value is not valid JSON (a null type with an
// empty value passes request validation) is never keyed, so tenants cannot
// collide on one fallback key.
func TestResultCacheKeyRefusesUnencodableParameters(t *testing.T) {
	r := query.Request{Mode: "native", SQL: "SELECT $1", Parameters: []query.Parameter{{Type: "null", Value: json.RawMessage{}}}}
	a := resultCacheKey(cacheKey{Tenant: "a", Policy: "p"}, r, query.DefaultLimits())
	b := resultCacheKey(cacheKey{Tenant: "b", Policy: "p"}, r, query.DefaultLimits())
	if a != "" || b != "" {
		t.Fatal("unencodable parameter produced a key", a, b)
	}
	null := r
	null.Parameters = []query.Parameter{{Type: "null", Value: json.RawMessage("null")}}
	if x, y := resultCacheKey(cacheKey{Tenant: "a"}, null, query.DefaultLimits()), resultCacheKey(cacheKey{Tenant: "b"}, null, query.DefaultLimits()); x == "" || x == y {
		t.Fatal("valid null parameter not keyed per tenant")
	}
}

// Review #2: SQL bytes are hashed raw; invalid UTF-8 does not collapse.
func TestResultCacheKeyRawSQLBytes(t *testing.T) {
	k, l := cacheKey{Tenant: "a"}, query.DefaultLimits()
	if resultCacheKey(k, query.Request{SQL: "SELECT '\xff'"}, l) == resultCacheKey(k, query.Request{SQL: "SELECT '\xfe'"}, l) {
		t.Fatal("invalid UTF-8 SQL collided")
	}
	// Field boundaries are length-prefixed: moving bytes between fields changes the key.
	if resultCacheKey(cacheKey{Tenant: "ab", Team: "c"}, query.Request{}, l) == resultCacheKey(cacheKey{Tenant: "a", Team: "bc"}, query.Request{}, l) {
		t.Fatal("field boundary collision")
	}
}

// Review #3: a refresh replacing a running flight still counts against cache_max_fills.
func TestResultCacheFillBoundSurvivesRefreshOverwrite(t *testing.T) {
	c := newResultCache(time.Minute, 64, 1<<20)
	c.maxFills = 1
	slow := &fakeRead{data: "slow", block: make(chan struct{})}
	done := make(chan struct{})
	go func() { _, _ = c.do(context.Background(), "k", "t", false, nil, slow.execute, noReplay); close(done) }()
	for slow.runs.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	var teed atomic.Bool
	refresh := func(ctx context.Context, s query.Sink, tee *cappedBuffer) (query.Stats, error) {
		teed.Store(tee != nil)
		return query.Stats{}, nil
	}
	_, _ = c.do(context.Background(), "k", "t", true, nil, refresh, noReplay)
	c.mu.Lock()
	fills := c.fills
	c.mu.Unlock()
	close(slow.block)
	<-done
	if teed.Load() || fills != 1 {
		t.Fatal("refresh started a fill beyond the bound", teed.Load(), fills)
	}
	if c.fills != 0 {
		t.Fatal("fill count leaked", c.fills)
	}
}

// Review #4/#5: an incomplete read (cleanup error, panic, early return) is never kept.
func TestSnapshotCacheIncompleteNotKept(t *testing.T) {
	c := newResultCache(time.Minute, 1<<10, 1<<20)
	current := "g1"
	s := snapshotFixture(c, "g1", &current)
	s.serve(nil)
	_, _ = s.tee(strings.NewReader("x")).Read(make([]byte, 1))
	s.finish(query.Stats{}, errCacheIncomplete)
	if len(c.entries) != 0 || c.fills != 0 || c.total != 0 {
		t.Fatal("incomplete fill kept or leaked")
	}
	var none *snapshotRead
	none.finish(query.Stats{}, nil) // reads that never qualified
}

// Review #6: X-Refresh skips the kept snapshot result and refills it.
func TestSnapshotCacheBypassRefills(t *testing.T) {
	c := newResultCache(time.Minute, 1<<10, 1<<20)
	current, runs := "g1", 0
	runSnapshot(t, snapshotFixture(c, "g1", &current), "old", 3, &runs)
	refresh := snapshotFixture(c, "g1", &current)
	refresh.bypass = true
	if runSnapshot(t, refresh, "new", 3, &runs) || runs != 2 {
		t.Fatal("refresh served the kept result")
	}
	var got string
	if _, _, hit := snapshotFixture(c, "g1", &current).serve(func(d []byte) (query.Stats, error) { got = string(d); return query.Stats{}, nil }); !hit || got != "new" {
		t.Fatal("refresh did not replace the entry", got)
	}
}
