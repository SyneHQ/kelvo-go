//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// This gate runs a node sized the way read_workers.max_workers=2 and
// pool_size=8 size it: two warm workers, each launched under the real sandbox
// launcher with `--pool-size 8`, against real PostgreSQL. Sixteen slow reads
// on each of two connections must start exactly two workers and open more than
// the old fixed four, and at most eight, backends per worker.
func TestContainedReadWorkersSizeWorkersAndBackends(t *testing.T) {
	const poolSize, workers, reads = 8, 2, 16
	g := newExplorerGate(t, admission.Limits{MaxConcurrent: 4, MemoryBytes: 1 << 30, ScratchBytes: 128 << 20}, false)
	if err := g.mgr.Drain(context.Background()); err != nil { // the gate's default pool never started a worker
		t.Fatal(err)
	}
	warm := DefaultWarmPoolConfig()
	warm.Enabled, warm.CacheDisabled = true, true
	warm.MaxWorkers, warm.MaxPerKey, warm.MaxInFlight = workers, 1, reads
	mgr, err := NewWarmPoolManager(warm, NewServeLauncher(poolSize))
	if err != nil {
		t.Fatal(err)
	}
	mgr.Adapter = g.adapter
	g.mgr, g.executor.WarmPool = mgr, mgr
	t.Cleanup(func() { _ = mgr.Drain(context.Background()) })
	connections := []string{"explorer", "explorer_b"}
	g.executor.Config = catalog.Config{Sources: []catalog.Source{
		{ID: connections[0], Type: "postgres", DSNEnv: "KELVO_TEST_WARM_POSTGRES_DSN"},
		{ID: connections[1], Type: "postgres", DSNEnv: "KELVO_TEST_WARM_POSTGRES_DSN"},
	}}

	var mu sync.Mutex
	backends := map[string]map[int64]bool{connections[0]: {}, connections[1]: {}}
	var wg sync.WaitGroup
	failures := make(chan error, workers*reads)
	for _, connection := range connections {
		for range reads {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sink := &workerTestSink{write: func(b arrow.RecordBatch) error {
					mu.Lock()
					defer mu.Unlock()
					for _, pid := range b.Column(0).(*array.Int64).Int64Values() {
						backends[connection][pid] = true
					}
					return nil
				}}
				_, err := g.executor.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: connection,
					SQL: "SELECT pg_backend_pid()::bigint AS pid FROM (SELECT pg_sleep(0.5)) s"}, sink)
				if err != nil {
					failures <- fmt.Errorf("%s: %w", connection, err)
				}
			}()
		}
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}

	metrics, active := mgr.Metrics(), g.pool.Snapshot().Active
	mgr.mu.Lock()
	live := len(mgr.workers)
	mgr.mu.Unlock()
	per := map[string]int{}
	union := map[int64]bool{}
	for connection, pids := range backends {
		per[connection] = len(pids)
		for pid := range pids {
			union[pid] = true
		}
	}
	report, _ := json.Marshal(map[string]any{"workers": live, "spawns": metrics.Spawns.Load(), "fallbacks": metrics.Fallbacks.Load(),
		"reservations_while_idle": active, "backends_per_worker": per, "backends_total": len(union), "pool_size": poolSize})
	t.Logf("GATE_METRICS %s", report)
	if live != workers || metrics.Spawns.Load() != workers || metrics.Fallbacks.Load() != 0 || active != workers {
		t.Fatal("node did not run exactly max_workers workers", string(report))
	}
	for connection, n := range per {
		if n <= 4 || n > poolSize { // 4 was the adapter's fixed pool
			t.Fatalf("%s used %d backends, want 5 to %d", connection, n, poolSize)
		}
	}
	if len(union) != per[connections[0]]+per[connections[1]] {
		t.Fatal("workers shared backends", string(report))
	}
	g.drain()
}
