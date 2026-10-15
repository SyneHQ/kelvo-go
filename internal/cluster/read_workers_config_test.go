// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func readWorkersNode(rw *ReadWorkersConfig, warm worker.WarmPoolConfig) NodeConfig {
	return NodeConfig{ReadWorkers: rw, Resources: &ResourceConfig{MaxConcurrent: 8}, Operations: &OperationNodeConfig{WarmPool: &warm}}
}

// read_workers is the one source of read-worker capacity: it lays over the
// non-capacity warm pool settings, and the adapter pool size comes from it.
func TestReadWorkersSizeTheWarmPoolAndAdapter(t *testing.T) {
	rw := ReadWorkersConfig{MaxWorkers: 2, PoolSize: 8}.withDefaults()
	node := readWorkersNode(&rw, worker.WarmPoolConfig{Enabled: true, MaxPerKey: 1, QueueDepth: 64, WorkerMemoryMB: 64, MaxPerTenant: 7})
	if err := validateNodeReadWorkers(node); err != nil {
		t.Fatal(err)
	}
	got := node.effectiveWarmPool()
	if got.MaxWorkers != 2 || got.MaxInFlight != 16 || got.IdleTTL != 30*time.Minute || got.QueueTimeout != 2*time.Second || got.StreamBufferMB != 8 ||
		got.MaxPerTenant != 7 || got.QueueDepth != 64 || !got.Enabled {
		t.Fatalf("effective warm pool: %+v", got)
	}
	if node.readPoolSize() != 8 {
		t.Fatalf("pool size %d", node.readPoolSize())
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	if node.Operations.WarmPool.MaxWorkers != 0 {
		t.Fatal("the configured warm pool was modified")
	}
}

// A node without read_workers keeps the legacy capacity fields and the
// adapter's own pool default.
func TestWarmPoolWithoutReadWorkersIsUnchanged(t *testing.T) {
	warm := worker.WarmPoolConfig{Enabled: true, MaxWorkers: 3, MaxInFlight: 9, IdleTTL: time.Minute, QueueTimeout: time.Second, MaxPerKey: 1, QueueDepth: 64, WorkerMemoryMB: 64}
	node := readWorkersNode(nil, warm)
	if got := node.effectiveWarmPool(); *got != warm || node.readPoolSize() != 0 || validateNodeReadWorkers(node) != nil {
		t.Fatalf("legacy configuration changed: %+v", got)
	}
}

func TestReadWorkersValidation(t *testing.T) {
	base := worker.WarmPoolConfig{Enabled: true, MaxPerKey: 1, QueueDepth: 64, WorkerMemoryMB: 64}
	for name, tc := range map[string]struct {
		rw   ReadWorkersConfig
		warm worker.WarmPoolConfig
		err  error
	}{
		"valid":                       {ReadWorkersConfig{MaxWorkers: 4, PoolSize: 32}, base, nil},
		"connection budget":           {ReadWorkersConfig{MaxWorkers: 9, PoolSize: 32}, base, errReadWorkersConnections},
		"connection budget boundary":  {ReadWorkersConfig{MaxWorkers: 8, PoolSize: 32}, base, nil},
		"no capacity":                 {ReadWorkersConfig{}, base, errReadWorkersConfig},
		"pool size":                   {ReadWorkersConfig{MaxWorkers: 1, PoolSize: 33}, base, errReadWorkersConfig},
		"in flight":                   {ReadWorkersConfig{MaxWorkers: 1, MaxInFlight: 65}, base, errReadWorkersConfig},
		"above admission":             {ReadWorkersConfig{MaxWorkers: 9}, base, errReadWorkersConfig},
		"warm pool disabled":          {ReadWorkersConfig{MaxWorkers: 1}, worker.WarmPoolConfig{}, errReadWorkersWarmPool},
		"max_workers in both":         {ReadWorkersConfig{MaxWorkers: 1}, func() worker.WarmPoolConfig { w := base; w.MaxWorkers = 1; return w }(), errReadWorkersWarmPool},
		"idle_ttl in both":            {ReadWorkersConfig{MaxWorkers: 1}, func() worker.WarmPoolConfig { w := base; w.IdleTTL = time.Minute; return w }(), errReadWorkersWarmPool},
		"max_in_flight in both":       {ReadWorkersConfig{MaxWorkers: 1}, func() worker.WarmPoolConfig { w := base; w.MaxInFlight = 4; return w }(), errReadWorkersWarmPool},
		"stream_buffer_mb in both":    {ReadWorkersConfig{MaxWorkers: 1}, func() worker.WarmPoolConfig { w := base; w.StreamBufferMB = 4; return w }(), errReadWorkersWarmPool},
		"queue wait and timeout both": {ReadWorkersConfig{MaxWorkers: 1}, func() worker.WarmPoolConfig { w := base; w.QueueTimeout = time.Second; return w }(), errReadWorkersWarmPool},
	} {
		rw := tc.rw.withDefaults()
		err := validateNodeReadWorkers(readWorkersNode(&rw, tc.warm))
		if err != tc.err {
			t.Errorf("%s: got %v, want %v", name, err, tc.err)
		}
		if err != nil && (strings.ContainsAny(err.Error(), "0123456789")) {
			t.Errorf("%s: error echoes a value: %v", name, err)
		}
	}
}

// pool_size is sent to the adapter only when the operator sets it: a node whose
// adapter or sandbox launcher predates --pool-size must keep its warm reads
// when read_workers sets only capacity.
func TestPoolSizeFlagIsSentOnlyWhenSet(t *testing.T) {
	base := worker.WarmPoolConfig{Enabled: true, MaxPerKey: 1, QueueDepth: 64, WorkerMemoryMB: 64}
	unset := ReadWorkersConfig{MaxWorkers: 2}.withDefaults()
	if n := readWorkersNode(&unset, base).readPoolSize(); n != 0 {
		t.Fatalf("pool_size unset requests %d", n)
	}
	if err := validateNodeReadWorkers(readWorkersNode(&unset, base)); err != nil {
		t.Fatal(err)
	}
	set := ReadWorkersConfig{MaxWorkers: 2, PoolSize: 6}.withDefaults()
	if n := readWorkersNode(&set, base).readPoolSize(); n != 6 {
		t.Fatalf("pool_size 6 requests %d", n)
	}
	if n := readWorkersNode(nil, base).readPoolSize(); n != 0 {
		t.Fatalf("no read_workers requests %d", n)
	}
	// The connection budget still counts the adapter's default of 4.
	big := ReadWorkersConfig{MaxWorkers: 65}.withDefaults()
	if err := validateNodeReadWorkers(readWorkersNode(&big, base)); err == nil {
		t.Fatal("65 workers accepted")
	}
	negative := ReadWorkersConfig{MaxWorkers: 1, PoolSize: -1}.withDefaults()
	if validateNodeReadWorkers(readWorkersNode(&negative, base)) == nil {
		t.Fatal("negative pool_size accepted")
	}
}
