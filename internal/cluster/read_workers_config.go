// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"errors"
	"path/filepath"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// ReadWorkersConfig sizes the pooled read workers of one node. It is a node
// setting only: it is not part of Policy, so it is never written to or compared
// with the cluster metadata record, and changing it needs a restart of that
// node and nothing else. Zero fields take the defaults below.
//
// read_workers is the single source of read-worker capacity. It sizes the warm
// pool (operations.warm_pool must be enabled, and must not set the same
// capacity fields) and the adapter's connection pool. A node that has no
// read_workers keeps sizing the pool from the legacy operations.warm_pool
// capacity fields.
type ReadWorkersConfig struct {
	MaxWorkers  int `yaml:"max_workers"`   // workers this node admits; one admission reservation each
	MaxPerKey   int `yaml:"max_per_key"`   // workers one connection may scale out to when its workers are full, default 2 (at most max_workers)
	MaxInFlight int `yaml:"max_in_flight"` // concurrent reads per worker, default 16, at most 64
	PoolSize    int `yaml:"pool_size"`     // source connections per worker, 1-32; unset sends no flag and the adapter keeps its own default of 8 (12 for about 80 ms to the source)
	// MaxSourceConnections bounds pool_size x max_per_key: the connections one
	// worker key (tenant, team, saved connection, database, schema, credential
	// revision) can hold against its source. Workers of different keys serve
	// different sources, so the product over all workers is not what any one
	// source sees; keys that differ only in team, schema or revision share a
	// database, and a credential rotation briefly runs the old and new key's
	// workers side by side.
	MaxSourceConnections int           `yaml:"max_source_connections"` // default 64, at most 1024
	IdleTTL              time.Duration `yaml:"idle_ttl"`               // idle worker lifetime, default 30m
	QueueWait            time.Duration `yaml:"queue_wait"`             // bounded admission wait before a retryable refusal, default 2s
	StreamBufferMB       int           `yaml:"stream_buffer_mb"`       // unread result bytes buffered per stream, default 8
	EvictMinIdle         time.Duration `yaml:"evict_min_idle"`         // a worker must be idle this long before another connection may evict it, default 2s

	// Adapter names the binary native reads run on: "relational" (default), a
	// slim build with only the relational engines read workers serve, or "full".
	// RelationalAdapter pins the slim binary like operations.adapter pins the
	// full one. Without it, or when the file is missing or does not match its
	// digest, the node uses the full adapter: a read never fails for that.
	Adapter           string                         `yaml:"adapter"`
	RelationalAdapter *worker.OperationProcessConfig `yaml:"relational_adapter,omitempty"`
}

const (
	defaultReadMaxInFlight = 16
	defaultReadMaxPerKey   = 2
	defaultReadPoolSize    = 8 // the adapter's own default, used for the budget when pool_size is unset
	defaultMaxSourceConns  = 64
	maxMaxSourceConns      = 1024
	defaultReadIdleTTL     = 30 * time.Minute
	defaultReadQueueWait   = 2 * time.Second
	defaultReadEvictIdle   = 2 * time.Second
	defaultReadStreamMB    = 8
	// maxReadQueueWait is the warm pool's worker launch timeout: a queue wait must stay below it.
	maxReadQueueWait  = 30 * time.Second
	adapterRelational = "relational"
	adapterFull       = "full"
)

var (
	errReadWorkersConfig      = errors.New("invalid read_workers configuration")
	errReadWorkersWarmPool    = errors.New("read_workers requires operations.warm_pool enabled, and capacity set only in read_workers")
	errReadWorkersConnections = errors.New("read_workers pool_size × max_per_key exceeds max_source_connections")
)

// withDefaults fills zero values. max_workers has no default: capacity is a
// deliberate operator choice for the node's size.
func (c ReadWorkersConfig) withDefaults() ReadWorkersConfig {
	if c.MaxInFlight == 0 {
		c.MaxInFlight = defaultReadMaxInFlight
	}
	if c.MaxPerKey == 0 {
		c.MaxPerKey = min(defaultReadMaxPerKey, c.MaxWorkers)
	}
	if c.IdleTTL == 0 {
		c.IdleTTL = defaultReadIdleTTL
	}
	if c.QueueWait == 0 {
		c.QueueWait = defaultReadQueueWait
	}
	if c.StreamBufferMB == 0 {
		c.StreamBufferMB = defaultReadStreamMB
	}
	if c.EvictMinIdle == 0 {
		c.EvictMinIdle = defaultReadEvictIdle
	}
	if c.MaxSourceConnections == 0 {
		c.MaxSourceConnections = defaultMaxSourceConns
	}
	if c.Adapter == "" {
		c.Adapter = adapterRelational
	}
	return c
}

func (c ReadWorkersConfig) validate(resources *ResourceConfig) error {
	if c.MaxWorkers < 1 || c.MaxWorkers > 64 || c.MaxInFlight < 1 || c.MaxInFlight > 64 || c.MaxPerKey < 1 || c.MaxPerKey > c.MaxWorkers || c.PoolSize < 0 || c.PoolSize > 32 || c.MaxSourceConnections < 1 || c.MaxSourceConnections > maxMaxSourceConns || c.StreamBufferMB < 1 || c.StreamBufferMB > 256 ||
		c.EvictMinIdle < time.Millisecond || c.EvictMinIdle > 10*time.Minute || c.IdleTTL < 10*time.Millisecond || c.IdleTTL > 24*time.Hour || c.QueueWait < 10*time.Millisecond || c.QueueWait >= maxReadQueueWait {
		return errReadWorkersConfig
	}
	pool := c.PoolSize
	if pool == 0 {
		pool = defaultReadPoolSize // the adapter's own default
	}
	if pool*c.MaxPerKey > c.MaxSourceConnections {
		return errReadWorkersConnections
	}
	if (c.Adapter != adapterRelational && c.Adapter != adapterFull) || (c.RelationalAdapter != nil && !validAdapterPin(*c.RelationalAdapter)) {
		return errReadWorkersConfig
	}
	// One reservation per worker must fit the node's admission pool.
	if resources != nil && c.MaxWorkers > resources.MaxConcurrent {
		return errReadWorkersConfig
	}
	return nil
}

// validateNodeReadWorkers checks read_workers against the operations config it
// sizes. Both errors are static: no configured value is echoed.
func validateNodeReadWorkers(c NodeConfig) error {
	if c.ReadWorkers == nil {
		return nil
	}
	if c.Operations == nil || c.Operations.WarmPool == nil || !c.Operations.WarmPool.Enabled || warmPoolCapacitySet(*c.Operations.WarmPool) {
		return errReadWorkersWarmPool
	}
	return c.ReadWorkers.validate(c.Resources)
}

// warmPoolCapacitySet reports whether operations.warm_pool sets a field that
// read_workers owns. A zero value is unset: yaml applies no defaults.
func warmPoolCapacitySet(w worker.WarmPoolConfig) bool {
	return w.IdleTTL != 0 || w.MaxWorkers != 0 || w.MaxPerKey != 0 || w.EvictMinIdle != 0 || w.MaxInFlight != 0 || w.StreamBufferMB != 0 || w.QueueTimeout != 0
}

// effectiveWarmPool is the warm pool configuration the node runs: the
// operations.warm_pool settings with read_workers capacity laid over them.
// Nil when the node has no warm pool.
func (c NodeConfig) effectiveWarmPool() *worker.WarmPoolConfig {
	if c.Operations == nil || c.Operations.WarmPool == nil {
		return nil
	}
	w := *c.Operations.WarmPool
	if rw := c.ReadWorkers; rw != nil {
		w.MaxWorkers, w.MaxPerKey, w.MaxInFlight, w.IdleTTL, w.QueueTimeout, w.StreamBufferMB, w.EvictMinIdle = rw.MaxWorkers, rw.MaxPerKey, rw.MaxInFlight, rw.IdleTTL, rw.QueueWait, rw.StreamBufferMB, rw.EvictMinIdle
	}
	return &w
}

// readPoolSize is the adapter connection pool size to request, 0 when pool_size
// is not set. Then no --pool-size is sent, so an adapter or launcher built
// before the flag keeps serving warm reads.
func (c NodeConfig) readPoolSize() int {
	if c.ReadWorkers == nil {
		return 0
	}
	return c.ReadWorkers.PoolSize
}

// validAdapterPin checks the shape of a pin without touching the file: a
// missing or changed slim binary is a fallback at startup, not a config error.
func validAdapterPin(a worker.OperationProcessConfig) bool {
	return filepath.IsAbs(a.Binary) && filepath.Clean(a.Binary) == a.Binary && len(a.Binary) <= 4096 && operations.ValidDigest(a.SHA256) && a.JDBC == nil
}

// readAdapter is the adapter binary native reads run on. The slim relational
// build is used only when it is configured and usable now; otherwise, and
// whenever it is not, the full adapter. Operations always use the full one: they
// may open engines the slim build does not carry. usable reports a binary the
// launcher would accept.
func readAdapter(rw *ReadWorkersConfig, full worker.OperationProcessConfig, usable func(worker.OperationProcessConfig) bool) (adapter worker.OperationProcessConfig, slim bool) {
	if rw == nil || rw.Adapter != adapterRelational || rw.RelationalAdapter == nil {
		return full, false
	}
	candidate := *rw.RelationalAdapter
	candidate.PrivateOpenDiagnostics = full.PrivateOpenDiagnostics
	if !usable(candidate) {
		return full, false
	}
	return candidate, true
}
