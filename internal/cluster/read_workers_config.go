// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/worker"
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
	MaxWorkers     int           `yaml:"max_workers"`      // workers this node admits; one admission reservation each
	MaxInFlight    int           `yaml:"max_in_flight"`    // concurrent reads per worker, default 16, at most 64
	PoolSize       int           `yaml:"pool_size"`        // source connections per worker, default 4, at most 32
	IdleTTL        time.Duration `yaml:"idle_ttl"`         // idle worker lifetime, default 30m
	QueueWait      time.Duration `yaml:"queue_wait"`       // bounded admission wait before a retryable refusal, default 2s
	StreamBufferMB int           `yaml:"stream_buffer_mb"` // unread result bytes buffered per stream, default 8
}

const (
	defaultReadMaxInFlight = 16
	defaultReadPoolSize    = 4
	defaultReadIdleTTL     = 30 * time.Minute
	defaultReadQueueWait   = 2 * time.Second
	defaultReadStreamMB    = 8
	// maxSourceConnections bounds max_workers × pool_size: what one node may
	// hold open against its sources.
	maxSourceConnections = 256
)

var (
	errReadWorkersConfig      = errors.New("invalid read_workers configuration")
	errReadWorkersWarmPool    = errors.New("read_workers requires operations.warm_pool enabled, and capacity set only in read_workers")
	errReadWorkersConnections = errors.New("read_workers max_workers × pool_size exceeds the source connection budget")
)

// withDefaults fills zero values. max_workers has no default: capacity is a
// deliberate operator choice for the node's size.
func (c ReadWorkersConfig) withDefaults() ReadWorkersConfig {
	if c.MaxInFlight == 0 {
		c.MaxInFlight = defaultReadMaxInFlight
	}
	if c.PoolSize == 0 {
		c.PoolSize = defaultReadPoolSize
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
	return c
}

func (c ReadWorkersConfig) validate(resources *ResourceConfig) error {
	if c.MaxWorkers < 1 || c.MaxWorkers > 64 || c.MaxInFlight < 1 || c.MaxInFlight > 64 || c.PoolSize < 1 || c.PoolSize > 32 || c.StreamBufferMB < 1 || c.StreamBufferMB > 256 ||
		c.IdleTTL < 10*time.Millisecond || c.IdleTTL > 24*time.Hour || c.QueueWait < 10*time.Millisecond || c.QueueWait > time.Minute {
		return errReadWorkersConfig
	}
	if c.MaxWorkers*c.PoolSize > maxSourceConnections {
		return errReadWorkersConnections
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
	return w.IdleTTL != 0 || w.MaxWorkers != 0 || w.MaxInFlight != 0 || w.StreamBufferMB != 0 || w.QueueTimeout != 0
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
		w.MaxWorkers, w.MaxInFlight, w.IdleTTL, w.QueueTimeout, w.StreamBufferMB = rw.MaxWorkers, rw.MaxInFlight, rw.IdleTTL, rw.QueueWait, rw.StreamBufferMB
	}
	return &w
}

// readPoolSize is the adapter connection pool size to request, 0 when the node
// leaves it to the adapter's default.
func (c NodeConfig) readPoolSize() int {
	if c.ReadWorkers == nil {
		return 0
	}
	return c.ReadWorkers.PoolSize
}
