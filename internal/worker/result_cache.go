// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// resultCache serves identical dashboard reads once: concurrent identical
// reads share one execution, and its Arrow IPC result is kept for ttl. Only
// complete, verified results no larger than maxEntry are kept, within
// maxTotal bytes; oldest entries go first.
type resultCache struct {
	ttl                time.Duration
	maxEntry, maxTotal int64

	mu      sync.Mutex
	entries map[string]*cacheEntry
	order   []string // insertion order, oldest first; may hold stale keys
	total   int64
	flights map[string]*cacheFlight
	hits    int64
}

type cacheEntry struct {
	data    []byte
	stats   query.Stats
	expires time.Time
}

type cacheFlight struct {
	done  chan struct{}
	entry *cacheEntry // nil when the leader's result was not cacheable
}

func newResultCache(ttl time.Duration, maxEntry, maxTotal int64) *resultCache {
	return &resultCache{ttl: ttl, maxEntry: maxEntry, maxTotal: maxTotal, entries: map[string]*cacheEntry{}, flights: map[string]*cacheFlight{}}
}

// resultCacheKey binds the tenant (cluster tenant and app team), connection,
// source revision, SQL, parameters and result limits. A key never matches
// another tenant's read: the tenant is part of every key.
func resultCacheKey(tenant, team, connection, revision string, r query.Request, limits query.Limits) string {
	raw, _ := json.Marshal([]any{"kelvo.result-cache.v1", tenant, team, connection, revision, r.SQL, r.Parameters, limits.MaxRows, limits.MaxBytes})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// do serves key from the cache or a running identical read, or runs it. run
// executes the read, writing the result's IPC bytes to the buffer it is given,
// and reports whether the result is complete and may be kept. replay serves a
// kept result. refresh skips the cache and any running read, and replaces the
// kept result when it succeeds.
func (c *resultCache) do(ctx context.Context, key string, refresh bool, run func(*cappedBuffer) (query.Stats, error, bool), replay func(*cacheEntry) (query.Stats, error)) (query.Stats, error) {
	c.mu.Lock()
	if !refresh {
		if entry := c.entries[key]; entry != nil && time.Now().Before(entry.expires) {
			c.hits++
			c.mu.Unlock()
			return replay(entry)
		}
		if flight := c.flights[key]; flight != nil {
			c.mu.Unlock()
			select {
			case <-flight.done:
			case <-ctx.Done():
				return query.Stats{}, ctx.Err()
			}
			if flight.entry != nil {
				c.mu.Lock()
				c.hits++
				c.mu.Unlock()
				return replay(flight.entry)
			}
			// The shared read failed or was too large to keep: run our own.
			stats, err, _ := run(nil)
			return stats, err
		}
	}
	var flight *cacheFlight
	if c.flights[key] == nil {
		flight = &cacheFlight{done: make(chan struct{})}
		c.flights[key] = flight
	}
	c.mu.Unlock()

	buffer := &cappedBuffer{max: c.maxEntry}
	stats, err, complete := run(buffer)
	var entry *cacheEntry
	if err == nil && complete && !buffer.over {
		entry = &cacheEntry{data: buffer.data, stats: stats, expires: time.Now().Add(c.ttl)}
	}
	c.mu.Lock()
	if entry != nil {
		c.storeLocked(key, entry)
	}
	if flight != nil {
		flight.entry = entry
		delete(c.flights, key)
		close(flight.done)
	}
	c.mu.Unlock()
	return stats, err
}

func (c *resultCache) storeLocked(key string, entry *cacheEntry) {
	if old := c.entries[key]; old != nil {
		c.total -= int64(len(old.data))
	}
	c.entries[key] = entry
	c.total += int64(len(entry.data))
	c.order = append(c.order, key)
	now := time.Now()
	// Drop expired entries and then the oldest until the total fits. A key is
	// removed only when its entry is the one this order slot inserted or has
	// expired; a stale slot for a re-stored key is skipped.
	kept := make([]string, 0, len(c.order))
	seen := map[string]bool{}
	for i := len(c.order) - 1; i >= 0; i-- {
		k := c.order[i]
		if seen[k] {
			continue
		}
		seen[k] = true
		if e := c.entries[k]; e != nil && now.Before(e.expires) {
			kept = append(kept, k)
		} else if e != nil {
			c.total -= int64(len(e.data))
			delete(c.entries, k)
		}
	}
	// kept is newest first; restore oldest-first order and trim from the front.
	for i, j := 0, len(kept)-1; i < j; i, j = i+1, j-1 {
		kept[i], kept[j] = kept[j], kept[i]
	}
	for len(kept) > 0 && c.total > c.maxTotal {
		e := c.entries[kept[0]]
		c.total -= int64(len(e.data))
		delete(c.entries, kept[0])
		kept = kept[1:]
	}
	c.order = kept
}

// cappedBuffer keeps up to max bytes and then only records that it overflowed;
// it never fails a write, so the read it copies is unaffected.
type cappedBuffer struct {
	data []byte
	max  int64
	over bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if !b.over {
		if int64(len(b.data)+len(p)) > b.max {
			b.over, b.data = true, nil
		} else {
			b.data = append(b.data, p...)
		}
	}
	return len(p), nil
}
