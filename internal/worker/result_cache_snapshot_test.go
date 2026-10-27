// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"strings"
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
