// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestResultCacheKeyTypedParameters(t *testing.T) {
	k, l := cacheKey{Tenant: "t", Principal: "p"}, query.DefaultLimits()
	key := func(typ, value string) string {
		return resultCacheKey(k, query.Request{Mode: "native", SQL: "SELECT $1", Parameters: []query.Parameter{{Type: typ, Value: json.RawMessage(value)}}}, l)
	}
	keys := map[string]bool{key("int32", "1"): true, key("int64", "1"): true, key("string", `"1"`): true}
	if len(keys) != 3 {
		t.Fatal("int32 1, int64 1 and \"1\" share a key")
	}
	if key("int64", " 1") != key("int64", "1") {
		t.Fatal("equal compact values differ")
	}
	r := query.Request{Mode: "native", SQL: "SELECT 1"}
	padded := r
	padded.SQL = "  SELECT 1\n"
	if resultCacheKey(k, r, l) != resultCacheKey(k, padded, l) {
		t.Fatal("surrounding whitespace changed the key")
	}
	a, b := k, k
	a.Generations = []datasetGeneration{{"a", "1"}, {"b", "2"}}
	b.Generations = []datasetGeneration{{"b", "2"}, {"a", "1"}}
	if resultCacheKey(a, r, l) != resultCacheKey(b, r, l) {
		t.Fatal("generation order changed the key")
	}
}

func TestPolicyFingerprint(t *testing.T) {
	ctx := context.Background()
	open := policyFingerprint(ctx, "bind", "rev")
	policy := access.Policy{Sources: map[string]access.SourcePolicy{"app": {Tables: map[string]access.TablePolicy{"orders": {Columns: []string{"id"}, AllRows: true}}}}}
	restricted, err := access.WithPolicy(ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	narrower := policy
	narrower.Sources = map[string]access.SourcePolicy{"app": {Tables: map[string]access.TablePolicy{"orders": {Columns: []string{"id", "total"}, AllRows: true}}}}
	other, _ := access.WithPolicy(ctx, narrower)
	seen := map[string]bool{open: true}
	for _, fp := range []string{policyFingerprint(restricted, "bind", "rev"), policyFingerprint(other, "bind", "rev"), policyFingerprint(ctx, "bind2", "rev"), policyFingerprint(ctx, "bind", "rev2")} {
		if seen[fp] {
			t.Fatal("distinct authorization shares a fingerprint")
		}
		seen[fp] = true
	}
	if policyFingerprint(ctx, "bind", "rev") != open {
		t.Fatal("fingerprint is not stable")
	}
}

// Two principals or two tenants reading the same SQL never share a result.
func TestResultCacheIsolatesPrincipalsAndTenants(t *testing.T) {
	c := newResultCache(time.Minute, 64, 1<<20)
	r, l := query.Request{Mode: "native", SQL: "SELECT 1"}, query.DefaultLimits()
	var got []string
	var mu sync.Mutex
	for _, k := range []cacheKey{{Tenant: "a", Principal: "alice"}, {Tenant: "a", Principal: "bob"}, {Tenant: "b", Principal: "alice"}} {
		read := &fakeRead{data: k.Tenant + "/" + k.Principal}
		if _, err := c.do(context.Background(), resultCacheKey(k, r, l), k.Tenant, false, nil, read.execute, replayData(&got, &mu)); err != nil || read.runs.Load() != 1 {
			t.Fatal("read served from another principal or tenant", k, err)
		}
	}
	if len(got) != 0 || c.Hits() != 0 {
		t.Fatal("cross-principal hit", got)
	}
}

// An empty result is kept and replayed as empty, not treated as a miss.
func TestResultCacheEmptyResult(t *testing.T) {
	c := newResultCache(time.Minute, 64, 1<<20)
	read := &fakeRead{}
	var got []string
	var mu sync.Mutex
	for i := 0; i < 3; i++ {
		if _, err := c.do(context.Background(), "k", "t", false, nil, read.execute, replayData(&got, &mu)); err != nil {
			t.Fatal(err)
		}
	}
	if read.runs.Load() != 1 || len(got) != 2 || got[0] != "" || got[1] != "" {
		t.Fatal("empty result not served from cache", read.runs.Load(), got)
	}
}

// Avoided execution is measurable: N identical reads execute once.
func TestResultCacheAvoidsExecution(t *testing.T) {
	c := newResultCache(time.Minute, 64, 1<<20)
	fence := func() string { return "g1" }
	read := &fakeRead{data: "rows"}
	for i := 0; i < 50; i++ {
		if _, err := c.doFenced(context.Background(), "k", "t", time.Minute, false, fence, nil, read.execute, noReplay); err != nil {
			t.Fatal(err)
		}
	}
	if read.runs.Load() != 1 || c.Hits() != 49 {
		t.Fatalf("executions=%d hits=%d, want 1 and 49", read.runs.Load(), c.Hits())
	}
}

// fencedRace starts a fill, moves the fence while it runs, and checks that
// the fill is neither kept nor replayed to a waiter.
func fencedRace(t *testing.T, move func(*atomic.Value)) {
	t.Helper()
	c := newResultCache(time.Minute, 64, 1<<20)
	var state atomic.Value
	state.Store("v1")
	fence := func() string { return state.Load().(string) }
	read := &fakeRead{data: "old", block: make(chan struct{})}
	var got []string
	var mu sync.Mutex
	leader := make(chan error)
	go func() {
		_, err := c.doFenced(context.Background(), "k", "t", time.Minute, false, fence, nil, read.execute, replayData(&got, &mu))
		leader <- err
	}()
	for read.runs.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	waiter := make(chan error)
	go func() {
		_, err := c.doFenced(context.Background(), "k", "t", time.Minute, false, fence, nil, read.execute, replayData(&got, &mu))
		waiter <- err
	}()
	for {
		c.mu.Lock()
		n := len(c.flights["k"].participants)
		c.mu.Unlock()
		if n == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	move(&state)
	close(read.block)
	if err := <-leader; err != nil {
		t.Fatal(err)
	}
	if err := <-waiter; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 0 || read.runs.Load() != 2 {
		t.Fatal("waiter replayed a fenced fill", got, read.runs.Load())
	}
	c.mu.Lock()
	kept := c.entries["k"]
	c.mu.Unlock()
	if kept != nil && kept.Value.(*cacheEntry).token != fence() {
		t.Fatal("fenced fill was kept")
	}
	if fence() != "" {
		// A later read under the new state is served only what was filled under it.
		before := read.runs.Load()
		_, _ = c.doFenced(context.Background(), "k", "t", time.Minute, false, fence, nil, read.execute, noReplay)
		if kept == nil && read.runs.Load() != before+1 {
			t.Fatal("read after the fence moved did not execute")
		}
	}
}

// A revocation during a fill (authorization gone: unknown fence) publishes nothing.
func TestResultCacheRevocationDuringFill(t *testing.T) {
	fencedRace(t, func(s *atomic.Value) { s.Store("") })
}

// A refresh publication (new generation) during a fill publishes nothing.
func TestResultCacheRefreshDuringFill(t *testing.T) {
	fencedRace(t, func(s *atomic.Value) { s.Store("v2") })
}

// A kept result is served only while its fence holds.
func TestResultCacheRechecksFenceBeforeServing(t *testing.T) {
	c := newResultCache(time.Minute, 64, 1<<20)
	state := "rev1"
	fence := func() string { return state }
	read := &fakeRead{data: "x"}
	run := func() {
		_, _ = c.doFenced(context.Background(), "k", "t", time.Minute, false, fence, nil, read.execute, noReplay)
	}
	run()
	run()
	state = "rev2"
	run()
	state = ""
	run()
	run()
	if read.runs.Load() != 4 || c.Hits() != 1 {
		t.Fatalf("executions=%d hits=%d, want 4 and 1", read.runs.Load(), c.Hits())
	}
}

// Least recently used entries go when the byte bound is reached.
func TestResultCacheEvictsUnderByteBound(t *testing.T) {
	one := int64(100 + 1 + cacheEntryOverhead) // data, key "a".."z", overhead
	c := newResultCache(time.Minute, 100, 4*2*one)
	data := string(make([]byte, 100))
	for _, key := range []string{"a", "b", "c"} {
		read := &fakeRead{data: data}
		_, _ = c.do(context.Background(), key, "t", false, nil, read.execute, noReplay)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries["a"] != nil || c.entries["b"] == nil || c.entries["c"] == nil || c.total > c.maxTotal || c.byTenant["t"] > c.tenantCap() {
		t.Fatal("byte bound not enforced by LRU eviction", len(c.entries), c.total)
	}
}

func TestResultCacheEntryAndFillBounds(t *testing.T) {
	c := newResultCache(time.Minute, 64, 1<<20)
	c.maxEntries = 2
	for _, key := range []string{"a", "b", "c"} {
		_, _ = c.do(context.Background(), key, "t", false, nil, (&fakeRead{data: key}).execute, noReplay)
	}
	if len(c.entries) != 2 || c.entries["a"] != nil {
		t.Fatal("entry bound not enforced", len(c.entries))
	}
	c.maxFills = 1
	block := &fakeRead{data: "slow", block: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		_, _ = c.do(context.Background(), "slow", "t", false, nil, block.execute, noReplay)
		close(done)
	}()
	for block.runs.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	extra := &fakeRead{data: "extra"}
	_, _ = c.do(context.Background(), "extra", "t", false, nil, extra.execute, noReplay)
	c.mu.Lock()
	kept := c.entries["extra"] != nil
	c.mu.Unlock()
	close(block.block)
	<-done
	if kept || extra.runs.Load() != 1 {
		t.Fatal("a read beyond the fill bound was cached")
	}
}

func TestWarmPoolConfigCacheBounds(t *testing.T) {
	base := DefaultWarmPoolConfig()
	base.Enabled = true
	for name, mutate := range map[string]func(*WarmPoolConfig){
		"entries": func(c *WarmPoolConfig) { c.CacheMaxEntries = 65537 },
		"fills":   func(c *WarmPoolConfig) { c.CacheMaxFills = -1 },
	} {
		c := base
		mutate(&c)
		if c.Validate() == nil {
			t.Fatal("accepted", name)
		}
	}
	c := base
	c.CacheMaxEntries, c.CacheMaxFills = 10, 3
	cache := c.resultCache()
	if cache.maxEntries != 10 || cache.maxFills != 3 || cache.ttl != 10*time.Second {
		t.Fatal("bounds not applied or default TTL changed", cache.maxEntries, cache.maxFills, cache.ttl)
	}
}
