// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

// Bounds that do not depend on configuration.
const (
	cacheMaxEntries    = 4096
	cacheEntryOverhead = 512 // bytes charged per entry beyond its data: key, list and map
)

// resultCache serves identical dashboard reads once. Concurrent identical
// reads share one execution; its Arrow IPC result is kept for ttl and
// replayed to each caller. Only complete, verified results are kept.
//
// Memory is bounded by maxTotal bytes across kept entries and in-flight
// buffers, at most cacheMaxEntries entries, and a quarter of maxTotal per
// tenant; least recently used entries go first.
type resultCache struct {
	ttl                time.Duration
	rules              []CacheRule // first match overrides ttl
	maxEntry, maxTotal int64
	perTenant          int64 // a quarter of maxTotal
	maxEntries         int

	mu       sync.Mutex
	lru      *list.List // of *cacheEntry, most recent at the front
	entries  map[string]*list.Element
	total    int64            // kept entries and in-flight buffers
	byTenant map[string]int64 // the same, per tenant
	flights  map[string]*cacheFlight
	hits     int64
}

type cacheEntry struct {
	key, tenant string
	data        []byte
	stats       query.Stats
	started     time.Time // when the execution that produced it started
	stored      time.Time // when it was kept
	expires     time.Time
}

func (e *cacheEntry) size() int64 { return int64(len(e.data)+len(e.key)) + cacheEntryOverhead }

// cacheFlight is one shared execution. It runs detached from any one caller
// and is cancelled only when every caller interested in it has gone.
type cacheFlight struct {
	started time.Time
	done    chan struct{}
	cancel  context.CancelFunc

	// Guarded by the cache's mu. Each participant (leader and waiters) is
	// listed with its deadline (zero: none). The execution's deadline is the
	// latest of them; it is cancelled when the last one leaves.
	participants map[int]time.Time
	nextID       int
	timer        *time.Timer
	dead         bool

	// Set before done is closed.
	entry    *cacheEntry // the kept result
	oversize bool        // completed but too large to keep
	stats    query.Stats
	err      error
}

func newResultCache(ttl time.Duration, maxEntry, maxTotal int64) *resultCache {
	return &resultCache{ttl: ttl, maxEntry: maxEntry, maxTotal: maxTotal, perTenant: maxTotal / 4, maxEntries: cacheMaxEntries,
		lru: list.New(), entries: map[string]*list.Element{}, byTenant: map[string]int64{}, flights: map[string]*cacheFlight{}}
}

// ttlFor is the TTL of a read: the first matching rule's, else the default.
func (c *resultCache) ttlFor(tenant, team, connection string) time.Duration {
	for _, r := range c.rules {
		if (r.Tenant == "" || r.Tenant == tenant) && (r.Team == "" || r.Team == team) && (r.Connection == "" || r.Connection == connection) {
			return r.TTL
		}
	}
	return c.ttl
}

func (c *resultCache) tenantCap() int64 { return c.perTenant }

// Hits is the number of reads served from a kept result or a shared execution.
func (c *resultCache) Hits() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits
}

// resultCacheKey binds everything a result depends on: tenant (cluster tenant
// and app team), connection, database, schema, source revision (definition
// and credentials), SQL, parameters and result limits.
//
// It deliberately leaves out the principal. A result depends only on the
// source it was read from; every read is authorized before the cache is
// consulted; and restricted principals, whose results could differ, never
// reach the cache. TestResultCacheKeyIgnoresPrincipal pins this.
func resultCacheKey(tenant, team, connection, database, schema, revision string, r query.Request, limits query.Limits) string {
	raw, _ := json.Marshal([]any{"kelvo.result-cache.v2", tenant, team, connection, database, schema, revision, r.Mode, r.SQL, r.Parameters, limits.MaxRows, limits.MaxBytes, limits.ResultCompression})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// executeFunc runs a read, delivering to sink and copying its Arrow IPC
// result to tee when tee is not nil.
type executeFunc func(ctx context.Context, sink query.Sink, tee *cappedBuffer) (query.Stats, error)

// replayFunc serves a kept result to sink.
type replayFunc func(entry *cacheEntry, sink query.Sink) (query.Stats, error)

// do serves key from a kept result, joins a running identical read, or leads
// a new one. A refresh skips kept results; see joinableLocked. Waiters of a read that failed get its
// error; a read too large to keep makes each waiter run its own.
func (c *resultCache) do(ctx context.Context, key, tenant string, refresh bool, sink query.Sink, execute executeFunc, replay replayFunc) (query.Stats, error) {
	return c.doTTL(ctx, key, tenant, c.ttl, refresh, sink, execute, replay)
}

// doTTL is do keeping the result for ttl; 0 keeps nothing, though concurrent
// identical reads still share one execution.
func (c *resultCache) doTTL(ctx context.Context, key, tenant string, ttl time.Duration, refresh bool, sink query.Sink, execute executeFunc, replay replayFunc) (query.Stats, error) {
	c.mu.Lock()
	if !refresh {
		if entry := c.freshLocked(key); entry != nil {
			c.hits++
			c.mu.Unlock()
			return c.replay(entry, sink, replay)
		}
	}
	if f := c.flights[key]; f != nil && c.joinableLocked(f, key, refresh) {
		id := c.joinLocked(f, ctx)
		c.mu.Unlock()
		select {
		case <-f.done:
		case <-ctx.Done():
			c.leave(f, key, id)
			return query.Stats{}, ctx.Err()
		}
		c.leave(f, key, id)
		switch {
		case f.entry != nil:
			c.mu.Lock()
			c.hits++
			c.mu.Unlock()
			return c.replay(f.entry, sink, replay)
		case f.oversize:
			return execute(ctx, sink, nil)
		default:
			return f.stats, f.err
		}
	}
	f := &cacheFlight{started: time.Now(), done: make(chan struct{}), participants: map[int]time.Time{}}
	c.flights[key] = f
	id := c.joinLocked(f, ctx)
	c.mu.Unlock()
	return c.lead(ctx, key, tenant, ttl, f, id, sink, execute)
}

// joinableLocked reports whether a read may join f. A plain read joins any
// running read. A refresh joins none: every running read, refresh or not,
// started before the refresh arrived and may predate the change the user is
// refreshing for. A refresh leads its own execution, which later plain reads
// may join.
func (c *resultCache) joinableLocked(f *cacheFlight, key string, refresh bool) bool {
	return !f.dead && !refresh
}

// joinLocked lists a participant and moves the execution's deadline to the
// latest participant deadline.
func (c *resultCache) joinLocked(f *cacheFlight, ctx context.Context) int {
	f.nextID++
	deadline, _ := ctx.Deadline()
	f.participants[f.nextID] = deadline
	c.retimeLocked(f)
	return f.nextID
}

// retimeLocked sets the execution's timer to the latest participant deadline;
// a participant without one keeps the execution untimed.
func (c *resultCache) retimeLocked(f *cacheFlight) {
	var latest time.Time
	for _, deadline := range f.participants {
		if deadline.IsZero() {
			latest = time.Time{}
			break
		}
		if deadline.After(latest) {
			latest = deadline
		}
	}
	if f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}
	if !latest.IsZero() && f.cancel != nil {
		f.timer = time.AfterFunc(time.Until(latest), f.cancel)
	}
}

// lead runs f detached from the leader's cancellation: if the leader leaves
// while others wait, the read continues for them. The leader streams to its
// own sink only while it is still waiting.
func (c *resultCache) lead(ctx context.Context, key, tenant string, ttl time.Duration, f *cacheFlight, id int, sink query.Sink, execute executeFunc) (query.Stats, error) {
	// Detached from the leader; ended by retimeLocked's timer or the last
	// participant leaving.
	execCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c.mu.Lock()
	f.cancel = cancel
	c.retimeLocked(f)
	c.mu.Unlock()
	leader := &leaderSink{sink: sink, gone: make(chan struct{})}
	buffer := &cappedBuffer{cache: c, tenant: tenant}
	go func() {
		defer cancel()
		var stats query.Stats
		var err error
		// The flight always closes, a panicking read included.
		defer func() {
			if recovered := recover(); recovered != nil {
				err = query.NewError("QUERY_FAILED", fmt.Sprint("Read failed: ", recovered))
			}
			c.finish(key, tenant, ttl, f, buffer, stats, err)
		}()
		stats, err = execute(execCtx, leader, buffer)
	}()
	select {
	case <-f.done:
		c.leave(f, key, id)
		if sinkErr := leader.err(); sinkErr != nil {
			return f.stats, sinkErr
		}
		return f.stats, f.err
	case <-ctx.Done():
		leader.detach()
		c.leave(f, key, id)
		return query.Stats{}, ctx.Err()
	case <-leader.gone:
		// The leader's client went away; the read goes on only for waiters.
		c.leave(f, key, id)
		return query.Stats{}, leader.err()
	}
}

// leave removes a participant. The last one to leave a running flight marks
// it dead and removes it, so later identical reads lead a fresh one rather
// than join a cancelled one; otherwise the deadline moves to the latest of
// those still waiting.
func (c *resultCache) leave(f *cacheFlight, key string, id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(f.participants, id)
	if len(f.participants) > 0 {
		c.retimeLocked(f)
		return
	}
	if f.timer != nil {
		f.timer.Stop()
	}
	select {
	case <-f.done:
		return // finished; finish already removed it
	default:
	}
	f.dead = true
	if c.flights[key] == f {
		delete(c.flights, key)
	}
	if f.cancel != nil {
		f.cancel()
	}
}

func (c *resultCache) finish(key, tenant string, ttl time.Duration, f *cacheFlight, buffer *cappedBuffer, stats query.Stats, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	buffer.releaseLocked()
	f.stats, f.err = stats, err
	if err == nil {
		if buffer.over {
			f.oversize = true
		} else {
			// Waiters replay a complete result even when it is not kept (a newer
			// generation is kept, or the tenant's share is full).
			f.entry = &cacheEntry{key: key, tenant: tenant, data: buffer.data, stats: stats, started: f.started, expires: time.Now().Add(ttl)}
			if ttl > 0 {
				c.storeLocked(f.entry)
			}
		}
	}
	if f.timer != nil {
		f.timer.Stop()
	}
	if c.flights[key] == f {
		delete(c.flights, key)
	}
	close(f.done)
}

func (c *resultCache) replay(entry *cacheEntry, sink query.Sink, replay replayFunc) (query.Stats, error) {
	start := time.Now()
	stats, err := replay(entry, sink)
	stats.Cached = true
	stats.DurationNS = time.Since(start).Nanoseconds()
	return stats, err
}

func (c *resultCache) freshLocked(key string) *cacheEntry {
	element := c.entries[key]
	if element == nil {
		return nil
	}
	entry := element.Value.(*cacheEntry)
	if !time.Now().Before(entry.expires) {
		c.removeLocked(element)
		return nil
	}
	c.lru.MoveToFront(element)
	return entry
}

// storeLocked keeps entry unless it is too large, or a kept entry for its key
// came from a read that started later. It evicts least recently used entries
// to fit the entry count, the total and the tenant's share.
func (c *resultCache) storeLocked(entry *cacheEntry) bool {
	size := entry.size()
	if size > c.maxEntry+int64(len(entry.key))+cacheEntryOverhead || size > c.tenantCap() {
		return false
	}
	if old := c.entries[entry.key]; old != nil {
		if old.Value.(*cacheEntry).started.After(entry.started) {
			return false // never replace a newer execution's result
		}
		c.removeLocked(old)
	}
	for c.lru.Len() >= c.maxEntries {
		c.removeLocked(c.lru.Back())
	}
	if !c.makeRoomLocked(entry.tenant, size) {
		return false
	}
	entry.stored = time.Now()
	c.entries[entry.key] = c.lru.PushFront(entry)
	c.total += size
	c.byTenant[entry.tenant] += size
	return true
}

// makeRoomLocked evicts least recently used entries until size more bytes fit
// the total and the tenant's share; the tenant's share evicts only its own.
func (c *resultCache) makeRoomLocked(tenant string, size int64) bool {
	for element := c.lru.Back(); element != nil && c.byTenant[tenant]+size > c.tenantCap(); {
		previous := element.Prev()
		if element.Value.(*cacheEntry).tenant == tenant {
			c.removeLocked(element)
		}
		element = previous
	}
	for c.total+size > c.maxTotal && c.lru.Len() > 0 {
		c.removeLocked(c.lru.Back())
	}
	return c.total+size <= c.maxTotal && c.byTenant[tenant]+size <= c.tenantCap()
}

func (c *resultCache) removeLocked(element *list.Element) {
	entry := c.lru.Remove(element).(*cacheEntry)
	delete(c.entries, entry.key)
	c.total -= entry.size()
	if c.byTenant[entry.tenant] -= entry.size(); c.byTenant[entry.tenant] <= 0 {
		delete(c.byTenant, entry.tenant)
	}
}

// cappedBuffer collects a shared read's result for the cache. Its bytes count
// against the cache's budget while the read runs. Once it cannot grow it only
// records that it overflowed; it never fails a write, so the read is unaffected.
type cappedBuffer struct {
	cache    *resultCache
	tenant   string
	data     []byte
	reserved int64
	over     bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	c := b.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	if b.over {
		return len(p), nil
	}
	need := int64(len(p))
	if int64(len(b.data))+need > c.maxEntry || !c.makeRoomLocked(b.tenant, need) {
		b.over, b.data = true, nil
		b.releaseLocked()
		return len(p), nil
	}
	b.data = append(b.data, p...)
	b.reserved += need
	c.total += need
	c.byTenant[b.tenant] += need
	return len(p), nil
}

func (b *cappedBuffer) releaseLocked() {
	c := b.cache
	c.total -= b.reserved
	if c.byTenant[b.tenant] -= b.reserved; c.byTenant[b.tenant] <= 0 {
		delete(c.byTenant, b.tenant)
	}
	b.reserved = 0
}

// leaderSink forwards a shared read to the leader's own sink until the leader
// leaves or its sink fails; the read itself continues for the waiters.
type leaderSink struct {
	mu       sync.Mutex
	sink     query.Sink
	detached bool
	failed   error
	gone     chan struct{} // closed when the leader's sink fails
}

func (s *leaderSink) Schema(schema *arrow.Schema) error {
	return s.forward(func() error { return s.sink.Schema(schema) })
}

func (s *leaderSink) Write(batch arrow.RecordBatch) error {
	return s.forward(func() error { return s.sink.Write(batch) })
}

func (s *leaderSink) forward(deliver func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.detached && s.failed == nil {
		if s.failed = deliver(); s.failed != nil {
			s.detached = true
			close(s.gone)
		}
	}
	return nil
}

func (s *leaderSink) detach() {
	s.mu.Lock()
	s.detached = true
	s.mu.Unlock()
}

func (s *leaderSink) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed
}
