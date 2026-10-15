// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"errors"
	"time"
)

// ReadWorkersConfig sizes the pooled read workers of one node. It is a node
// setting only: it is not part of Policy, so it is never written to or compared
// with the cluster metadata record, and changing it needs a restart of that
// node and nothing else. Zero fields take the defaults below.
//
// The settings are validated here and exposed on NodeConfig. Wiring them into
// the warm pool belongs to the read-path work (see the Phase 4 follow-up in
// docs/design/pooled-read-worker.md) and is deliberately not done here.
type ReadWorkersConfig struct {
	MaxWorkers  int           `yaml:"max_workers"`   // workers this node admits; one admission reservation each
	MaxInFlight int           `yaml:"max_in_flight"` // concurrent reads per worker, default 16
	PoolSize    int           `yaml:"pool_size"`     // source connections per worker, default 4
	IdleTTL     time.Duration `yaml:"idle_ttl"`      // idle worker lifetime, default 30m
	QueueWait   time.Duration `yaml:"queue_wait"`    // bounded admission wait before a retryable refusal, default 2s
}

const (
	defaultReadMaxInFlight = 16
	defaultReadPoolSize    = 4
	defaultReadIdleTTL     = 30 * time.Minute
	defaultReadQueueWait   = 2 * time.Second
)

var errReadWorkersConfig = errors.New("invalid read_workers configuration")

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
	return c
}

func (c ReadWorkersConfig) validate(resources *ResourceConfig) error {
	if c.MaxWorkers < 1 || c.MaxWorkers > 64 || c.MaxInFlight < 1 || c.MaxInFlight > 256 || c.PoolSize < 1 || c.PoolSize > 64 ||
		c.IdleTTL < 10*time.Millisecond || c.IdleTTL > 24*time.Hour || c.QueueWait < 10*time.Millisecond || c.QueueWait > time.Minute {
		return errReadWorkersConfig
	}
	// One reservation per worker must fit the node's admission pool.
	if resources != nil && c.MaxWorkers > resources.MaxConcurrent {
		return errReadWorkersConfig
	}
	return nil
}
