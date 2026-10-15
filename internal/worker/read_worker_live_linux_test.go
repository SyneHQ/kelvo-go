//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jackc/pgx/v5"
)

const explorerTables = 8

// explorerPage is what an explorer page open sends at once: the schema tree,
// the table list, and for each visible table its columns, keys, indexes, a
// 50-row preview and a count. Eight more reads report pg_backend_pid().
func explorerPage() (requests []query.Request, pidReads map[int]bool) {
	inspect := func(object string, target operations.ObjectRef, limit int) query.Request {
		return query.Request{Mode: "native", ConnectionID: "explorer", Inspect: &operations.MetadataSpec{Object: object, Target: target, Limit: limit}}
	}
	read := func(sql string) query.Request {
		return query.Request{Mode: "native", ConnectionID: "explorer", SQL: sql}
	}
	requests = append(requests, inspect("schemas", operations.ObjectRef{}, 100), inspect("tables", operations.ObjectRef{Schema: "public"}, 500))
	pidReads = map[int]bool{}
	for i := 0; i < explorerTables; i++ {
		table := fmt.Sprintf("explorer_t%d", i)
		target := operations.ObjectRef{Schema: "public", Name: table}
		requests = append(requests, inspect("columns", target, 500), inspect("primary_keys", target, 100), inspect("indexes", target, 100),
			read("SELECT * FROM public."+table+" ORDER BY id LIMIT 50"), read("SELECT count(*)::bigint AS n FROM public."+table))
		pidReads[len(requests)] = true
		requests = append(requests, read("SELECT pg_backend_pid()::bigint AS pid"))
	}
	return requests, pidReads
}

type burstResult struct {
	N        int     `json:"n"`
	Failures int     `json:"failures"`
	P50MS    float64 `json:"p50_ms"`
	P95MS    float64 `json:"p95_ms"`
	MaxMS    float64 `json:"max_ms"`
	WallMS   float64 `json:"wall_ms"`
}

// percentile is nearest-rank over sorted durations.
func percentile(sorted []time.Duration, p int) float64 {
	i := (len(sorted)*p + 99) / 100
	return float64(sorted[max(i, 1)-1].Microseconds()) / 1000
}

// This gate replays an explorer page open, 50 parallel native reads and
// metadata inspections, through Executor.Execute (the node's query entry)
// to one real adapter worker under the real launcher against real
// PostgreSQL. Every request must succeed, all on one worker holding one
// reservation and at most pool_size backends; p50 and p95 are reported.
func TestContainedReadWorkerServesExplorerPageOpen(t *testing.T) {
	adapterPath, dsn := os.Getenv("KELVO_TEST_WARM_ADAPTER_BINARY"), os.Getenv("KELVO_TEST_WARM_POSTGRES_DSN")
	if adapterPath == "" || dsn == "" {
		t.Skip("set KELVO_TEST_WARM_ADAPTER_BINARY and KELVO_TEST_WARM_POSTGRES_DSN for the explorer replay gate")
	}
	setup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(setup, dsn)
	if err != nil {
		t.Fatal("fixture connection failed")
	}
	for i := 0; i < explorerTables; i++ {
		table := fmt.Sprintf("public.explorer_t%d", i)
		for _, statement := range []string{
			"DROP TABLE IF EXISTS " + table,
			"CREATE TABLE " + table + " (id bigint PRIMARY KEY, name text NOT NULL, amount numeric(12,2), created_at timestamptz NOT NULL DEFAULT now())",
			"CREATE INDEX ON " + table + " (created_at)",
			"INSERT INTO " + table + " (id, name, amount) SELECT g, 'row ' || g, g * 1.25 FROM generate_series(1, 500) g",
		} {
			if _, err := conn.Exec(setup, statement); err != nil {
				t.Fatal("fixture setup failed:", err)
			}
		}
	}
	_ = conn.Close(setup)

	adapterPath, err = filepath.EvalSymlinks(adapterPath)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(adapterPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)

	executor, manager, _ := containedExecutor(t)
	pool, err := admission.New(admission.Limits{MaxConcurrent: 2, MemoryBytes: 512 << 20, ScratchBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	executor.ResourcePool = pool
	executor.Limits.MaxBytes = readWorkerMaxBytes
	executor.Config = catalog.Config{Sources: []catalog.Source{{ID: "explorer", Type: "postgres", DSNEnv: "KELVO_TEST_WARM_POSTGRES_DSN"}}}
	warm := DefaultWarmPoolConfig()
	warm.Enabled = true
	mgr, err := NewWarmPoolManager(warm, nil)
	if err != nil {
		t.Fatal(err)
	}
	mgr.Adapter = OperationProcessConfig{Binary: adapterPath, SHA256: hex.EncodeToString(sum[:])}
	executor.WarmPool = mgr
	drained := false
	defer func() {
		if !drained {
			_ = mgr.Drain(context.Background())
		}
	}()

	var pidMu sync.Mutex
	pids := map[int64]bool{}
	burst := func(label string) burstResult {
		requests, pidReads := explorerPage()
		durations := make([]time.Duration, len(requests))
		failures := make([]error, len(requests))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, r := range requests {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				sink := &workerTestSink{}
				if pidReads[i] {
					sink.write = func(b arrow.RecordBatch) error {
						pidMu.Lock()
						defer pidMu.Unlock()
						for _, pid := range b.Column(0).(*array.Int64).Int64Values() {
							pids[pid] = true
						}
						return nil
					}
				}
				<-start
				began := time.Now()
				_, err := executor.Execute(ctx, r, sink)
				durations[i] = time.Since(began)
				if err == nil && sink.rows == 0 {
					err = fmt.Errorf("no rows")
				}
				failures[i] = err
			}()
		}
		began := time.Now()
		close(start)
		wg.Wait()
		wall := time.Since(began)
		result := burstResult{N: len(requests), WallMS: float64(wall.Microseconds()) / 1000}
		for i, err := range failures {
			if err != nil {
				result.Failures++
				kind := "read"
				if requests[i].Inspect != nil {
					kind = "inspect " + requests[i].Inspect.Object
				}
				t.Errorf("%s request %d (%s) failed: %v", label, i, kind, err)
			}
		}
		slices.Sort(durations)
		result.P50MS, result.P95MS, result.MaxMS = percentile(durations, 50), percentile(durations, 95), percentile(durations, 100)
		return result
	}

	// The first page open also starts the worker; the second finds it warm.
	cold := burst("cold")
	warmBurst := burst("warm")
	metrics := mgr.Metrics()
	active := pool.Snapshot().Active
	report, _ := json.Marshal(map[string]any{"cold": cold, "warm": warmBurst, "spawns": metrics.Spawns.Load(), "fallbacks": metrics.Fallbacks.Load(),
		"backends": len(pids), "reservations_while_idle": active, "max_in_flight": warm.MaxInFlight})
	t.Logf("GATE_METRICS %s", report)
	// relational.PoolSize in the adapter module is 4.
	if metrics.Spawns.Load() != 1 || metrics.Fallbacks.Load() != 0 || len(pids) < 1 || len(pids) > 4 || active != 1 {
		t.Fatal("explorer page was not served by one pooled worker", string(report))
	}

	drained = true
	if err := mgr.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("drain leaked the read worker", pool.Snapshot().Active, manager.Status().Active)
	}
}
