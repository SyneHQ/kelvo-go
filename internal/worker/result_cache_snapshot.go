// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// errCacheIncomplete ends a fill whose read did not reach its normal return.
var errCacheIncomplete = errors.New("result cache: read did not complete")

// snapshotRead is one federated read over immutable snapshot generations that
// may be served from, or kept in, the result cache. The key holds every
// generation the read resolved; the fence re-reads each dataset's current
// generation, so a hit or a fill that a refresh publication overtook is
// refused.
type snapshotRead struct {
	cache       *resultCache
	key, tenant string
	ttl         time.Duration
	fence       cacheFence
	token       string // the resolved generations; the fence must still report them
	started     time.Time
	buffer      *cappedBuffer // nil: not filling
	bypass      bool          // X-Refresh: never served, still filled
}

// snapshotReadFor returns the cache handle of a read, or nil when the read
// does not qualify: caching is off, or any source is not a snapshot
// generation (live sources, live-routed datasets, native reads).
func (e *Executor) snapshotReadFor(ctx context.Context, execution delegation.Execution, dynamic bool, r query.Request, versions []query.AccelerationVersion) *snapshotRead {
	pool := e.WarmPool
	if pool == nil || pool.cache == nil || !pool.Config().CacheSnapshotGenerations || r.Mode != "federated" || r.ScanDiagnostics || r.Inspect != nil ||
		len(versions) == 0 || len(versions) != len(r.Sources) {
		return nil
	}
	generations := make([]datasetGeneration, len(versions))
	fingerprints := make([]string, len(versions))
	for i, v := range versions {
		fingerprint, err := e.Config.DatasetFingerprint(v.Dataset)
		if v.Generation == "" || v.RoutedLive || v.RefreshedOnDemand || err != nil {
			return nil
		}
		generations[i], fingerprints[i] = datasetGeneration{v.Dataset, v.Generation}, fingerprint
	}
	id, _, principal := readPrincipal(execution, dynamic, r.ConnectionID)
	policy := policyFingerprint(ctx, catalogBindingDigest(e), "")
	if policy == "" {
		return nil
	}
	key := resultCacheKey(cacheKey{Tenant: id.tenant, Team: id.team, Principal: principal, Revision: strings.Join(fingerprints, ","),
		Policy: policy, Generations: generations}, r, e.Limits)
	if key == "" {
		return nil
	}
	return &snapshotRead{bypass: r.BypassCache, cache: pool.cache, key: key, tenant: id.tenant, ttl: pool.cache.ttlFor(id.tenant, id.team, ""), fence: e.generationFence(generations), token: generationToken(generations)}
}

// generationFence reports the current generation of each dataset; "" when
// any is unknown.
func (e *Executor) generationFence(generations []datasetGeneration) cacheFence {
	return func() string {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		status := func(dataset string) (acceleration.Snapshot, error) { return e.ObjectRuntime.Status(ctx, dataset) }
		if !acceleration.ProtectedObjects(e.Config) {
			if e.Config.Acceleration == nil {
				return ""
			}
			backend, err := acceleration.OpenBackend(*e.Config.Acceleration)
			if err != nil {
				return ""
			}
			defer backend.Close()
			status = func(dataset string) (acceleration.Snapshot, error) { return backend.Status(ctx, dataset) }
		}
		current := make([]string, len(generations))
		for i, g := range generations {
			snapshot, err := status(g.Dataset)
			if err != nil || snapshot.Generation == "" {
				return ""
			}
			current[i] = snapshot.Generation
		}
		return strings.Join(current, "\x00")
	}
}

// generationToken is the fence token of the generations this read resolved.
func generationToken(generations []datasetGeneration) string {
	parts := make([]string, len(generations))
	for i, g := range generations {
		parts[i] = g.Generation
	}
	return strings.Join(parts, "\x00")
}

// serve replays a kept result when its generations are still current. When
// it is not a hit, it starts a fill unless the fill bound is reached.
func (s *snapshotRead) serve(replay func(data []byte) (query.Stats, error)) (query.Stats, error, bool) {
	current := s.fence()
	c := s.cache
	c.mu.Lock()
	if current != s.token { // unknown, or a refresh published after this read resolved
		c.fenced++
		if element := c.entries[s.key]; element != nil {
			c.removeLocked(element) // superseded generation: never servable again
		}
		c.mu.Unlock()
		return query.Stats{}, nil, false
	}
	if entry := c.freshLocked(s.key); entry != nil && !s.bypass {
		if entry.token == s.token {
			c.hits++
			c.mu.Unlock()
			start := time.Now()
			stats, err := replay(entry.data)
			stats.Cached, stats.DurationNS = true, time.Since(start).Nanoseconds()
			return stats, err, true
		}
		c.removeLocked(c.entries[s.key]) // a refresh published a newer generation
		c.fenced++
	}
	if c.fills < c.maxFills {
		c.fills++
		s.started, s.buffer = time.Now(), &cappedBuffer{cache: c, tenant: s.tenant}
	}
	c.mu.Unlock()
	return query.Stats{}, nil, false
}

// tee is where the fill copies the read's Arrow IPC stream; nil when not filling.
func (s *snapshotRead) tee(stdout io.Reader) io.Reader {
	if s == nil || s.buffer == nil {
		return stdout
	}
	return io.TeeReader(stdout, s.buffer)
}

// finish keeps a complete result whose generations are still current.
func (s *snapshotRead) finish(stats query.Stats, err error) {
	if s == nil || s.buffer == nil {
		return
	}
	moved := s.fence() != s.token
	c := s.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fills--
	s.buffer.releaseLocked()
	switch {
	case err != nil || s.buffer.over || stats.Bytes > c.maxDecoded:
	case moved:
		c.fenced++
	case s.ttl > 0:
		c.storeLocked(&cacheEntry{key: s.key, tenant: s.tenant, data: s.buffer.data, stats: stats, started: s.started, expires: time.Now().Add(s.ttl), token: s.token})
	}
	s.buffer = nil
}

func replayIPC(ctx context.Context, e *Executor, sink query.Sink) func([]byte) (query.Stats, error) {
	return func(data []byte) (query.Stats, error) { return e.readIPC(ctx, bytes.NewReader(data), e.Limits, sink) }
}
