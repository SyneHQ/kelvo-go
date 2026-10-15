//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
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

// explorerGate is one contained executor with a read worker pool and the
// explorer fixture tables, for the gates below.
type explorerGate struct {
	t        *testing.T
	executor *Executor
	pool     *admission.Pool
	mgr      *WarmPoolManager
	manager  interface{ Status() containment.Status }
	adapter  OperationProcessConfig
	database string

	pidMu sync.Mutex
	pids  map[int64]bool
}

// cache turns on the dashboard result cache; the explorer gates measure the
// worker path itself, so they run without it.
func newExplorerGate(t *testing.T, limits admission.Limits, cache bool) *explorerGate {
	t.Helper()
	adapterPath, dsn := os.Getenv("KELVO_TEST_WARM_ADAPTER_BINARY"), os.Getenv("KELVO_TEST_WARM_POSTGRES_DSN")
	if adapterPath == "" || dsn == "" {
		t.Skip("set KELVO_TEST_WARM_ADAPTER_BINARY and KELVO_TEST_WARM_POSTGRES_DSN for the explorer replay gate")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || strings.Trim(parsed.Path, "/") == "" {
		t.Fatal("invalid PostgreSQL fixture address")
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
	pool, err := admission.New(limits)
	if err != nil {
		t.Fatal(err)
	}
	executor.ResourcePool = pool
	executor.Limits.MaxBytes = readWorkerMaxBytes
	executor.Config = catalog.Config{Sources: []catalog.Source{{ID: "explorer", Type: "postgres", DSNEnv: "KELVO_TEST_WARM_POSTGRES_DSN"}}}
	warm := DefaultWarmPoolConfig()
	warm.Enabled = true
	warm.CacheDisabled = !cache
	mgr, err := NewWarmPoolManager(warm, nil)
	if err != nil {
		t.Fatal(err)
	}
	g := &explorerGate{t: t, executor: executor, pool: pool, mgr: mgr, manager: manager, database: strings.Trim(parsed.Path, "/"),
		adapter: OperationProcessConfig{Binary: adapterPath, SHA256: hex.EncodeToString(sum[:])}, pids: map[int64]bool{}}
	mgr.Adapter = g.adapter
	executor.WarmPool = mgr
	t.Cleanup(func() { _ = mgr.Drain(context.Background()) })
	return g
}

// burst sends one explorer page open at once and returns its sorted latencies.
func (g *explorerGate) burst(label string) ([]time.Duration, burstResult) {
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
					g.pidMu.Lock()
					defer g.pidMu.Unlock()
					for _, pid := range b.Column(0).(*array.Int64).Int64Values() {
						g.pids[pid] = true
					}
					return nil
				}
			}
			<-start
			began := time.Now()
			_, err := g.executor.Execute(ctx, r, sink)
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
	result := burstResult{N: len(requests), WallMS: float64(time.Since(began).Microseconds()) / 1000}
	for i, err := range failures {
		if err != nil {
			result.Failures++
			kind := "read"
			if requests[i].Inspect != nil {
				kind = "inspect " + requests[i].Inspect.Object
			}
			g.t.Errorf("%s request %d (%s) failed: %v", label, i, kind, err)
		}
	}
	slices.Sort(durations)
	result.P50MS, result.P95MS, result.MaxMS = percentile(durations, 50), percentile(durations, 95), percentile(durations, 100)
	return durations, result
}

// bursts sends n page opens one after another and summarizes all of them.
func (g *explorerGate) bursts(label string, n int) burstResult {
	var all []time.Duration
	var result burstResult
	for i := 0; i < n; i++ {
		durations, one := g.burst(fmt.Sprintf("%s-%d", label, i))
		all = append(all, durations...)
		result.N += one.N
		result.Failures += one.Failures
		result.WallMS += one.WallMS
	}
	slices.Sort(all)
	result.P50MS, result.P95MS, result.MaxMS = percentile(all, 50), percentile(all, 95), percentile(all, 100)
	return result
}

func (g *explorerGate) drain() {
	g.t.Helper()
	if err := g.mgr.Drain(context.Background()); err != nil {
		g.t.Fatal(err)
	}
	if g.pool.Snapshot().Active != 0 || g.manager.Status().Active != 0 {
		g.t.Fatal("drain leaked the read worker", g.pool.Snapshot().Active, g.manager.Status().Active)
	}
}

// This gate replays an explorer page open, 50 parallel native reads and
// metadata inspections, through Executor.Execute (the node's query entry)
// to one real adapter worker under the real launcher against real
// PostgreSQL. Every request must succeed, all on one worker holding one
// reservation and at most pool_size backends; p50 and p95 are reported.
func TestContainedReadWorkerServesExplorerPageOpen(t *testing.T) {
	g := newExplorerGate(t, admission.Limits{MaxConcurrent: 2, MemoryBytes: 512 << 20, ScratchBytes: 64 << 20}, false)
	// The first page open also starts the worker; the second finds it warm.
	_, cold := g.burst("cold")
	_, warmBurst := g.burst("warm")
	metrics := g.mgr.Metrics()
	active := g.pool.Snapshot().Active
	report, _ := json.Marshal(map[string]any{"cold": cold, "warm": warmBurst, "spawns": metrics.Spawns.Load(), "fallbacks": metrics.Fallbacks.Load(),
		"backends": len(g.pids), "reservations_while_idle": active, "max_in_flight": g.mgr.Config().maxInFlight()})
	t.Logf("GATE_METRICS %s", report)
	// relational.PoolSize in the adapter module is 4.
	if metrics.Spawns.Load() != 1 || metrics.Fallbacks.Load() != 0 || len(g.pids) < 1 || len(g.pids) > 4 || active != 1 {
		t.Fatal("explorer page was not served by one pooled worker", string(report))
	}
	g.drain()
}

// This gate keeps the background class full of durable operations, with one
// more queued, while explorer pages open. The read worker must still start in
// its own interactive slot, every explorer request must succeed, and explorer
// p95 under load must stay within noise of p95 with the background idle.
func TestContainedExplorerUnaffectedByFullBackgroundClass(t *testing.T) {
	const workerReservation = (64 + 160) << 20 // worker_memory_mb + the executor's overhead
	g := newExplorerGate(t, admission.Limits{MaxConcurrent: 3, MemoryBytes: 3 * workerReservation, ScratchBytes: 96 << 20,
		ReservedSlots: 1, ReservedMemoryBytes: workerReservation, ReservedScratchBytes: 32 << 20, SeparateClasses: true}, false)
	// Durable operations run on the same executor, warm pool enabled: they
	// must take the background class and never a read worker.
	background := g.executor
	var analysisIDs atomic.Int64
	analysis := func(ctx context.Context) error {
		request := operations.Request{Version: operations.Version, Kind: operations.QueryRead,
			Connection: operations.ConnectionRef{ID: "analysis", Database: g.database, Schema: "public"},
			Spec:       operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT count(*)::bigint AS n FROM (SELECT pg_sleep(2)) s"}}}
		digest, err := operations.Digest(request)
		if err != nil {
			return err
		}
		now := time.Now().Unix()
		input := adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: fmt.Sprintf("operation-analysis-%d", analysisIDs.Add(1)), RequestSHA256: digest, Request: request,
			Limits: adapter.ProcessLimits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 10, TimeoutMS: 20000},
			Source: adapter.ConnectionSpec{Engine: "postgres", DSN: os.Getenv("KELVO_TEST_WARM_POSTGRES_DSN"), TenantID: "tenant-analysis", ConnectionID: request.Connection.ID,
				Revision: "revision-1", Database: g.database, Schema: "public"},
			CredentialsValidUntil: now + 5, ExpiresAt: now + 60}
		receipt, err := background.ExecuteOperation(ctx, g.adapter, input, &workerTestSink{})
		if err == nil && receipt.Outcome != operations.Completed {
			err = fmt.Errorf("analysis job %s", receipt.ErrorCode)
		}
		return err
	}

	// Three looping jobs for two background slots: the class stays full with
	// one waiting, which would take a free interactive slot if it could.
	stop, cancelLoad := context.WithCancel(context.Background())
	var loaders sync.WaitGroup
	var analysisRuns, analysisFailures atomic.Int64
	for i := 0; i < 3; i++ {
		loaders.Add(1)
		go func() {
			defer loaders.Done()
			for stop.Err() == nil {
				if err := analysis(stop); err != nil && stop.Err() == nil {
					analysisFailures.Add(1)
					t.Error("analysis job failed:", err)
				}
				analysisRuns.Add(1)
			}
		}()
	}
	defer func() { cancelLoad(); loaders.Wait() }()
	full := func() bool {
		s := g.pool.Snapshot()
		return s.Classes[admission.ClassOperation].Active == 2 && s.Classes[admission.ClassOperation].Waiting >= 1
	}
	waitFor(t, full)

	_, cold := g.burst("cold-under-load")
	var samples, fullSamples int
	sample := func() {
		samples++
		if full() {
			fullSamples++
		}
	}
	sample()
	loaded := g.bursts("warm-under-load", 3)
	sample()
	cancelLoad()
	loaders.Wait()
	idle := g.bursts("warm-idle", 3)

	metrics := g.mgr.Metrics()
	report, _ := json.Marshal(map[string]any{"cold_under_load": cold, "loaded": loaded, "idle": idle, "spawns": metrics.Spawns.Load(), "fallbacks": metrics.Fallbacks.Load(),
		"background_slots": 2, "background_full_samples": fmt.Sprintf("%d/%d", fullSamples, samples), "analysis_runs": analysisRuns.Load(), "analysis_failures": analysisFailures.Load()})
	t.Logf("GATE_METRICS %s", report)
	// 1.5x plus 25 ms absorbs scheduler noise at these latencies on a shared
	// runner; a blocked or queued explorer would add seconds, not milliseconds.
	if metrics.Spawns.Load() != 1 || metrics.Fallbacks.Load() != 0 || fullSamples != samples || loaded.Failures+idle.Failures+cold.Failures != 0 ||
		loaded.P95MS > idle.P95MS*1.5+25 {
		t.Fatal("explorer was affected by a full background class", string(report))
	}
	g.drain()
}

// This gate opens one six-panel dashboard in ten viewers at once, twice, a
// cache_ttl apart, then refreshes one panel explicitly. pg_stat_statements on
// the real PostgreSQL counts what reached the source: one execution per panel
// per refresh, and one more for the explicit refresh.
func TestContainedDashboardViewersShareOneSourceQueryPerPanel(t *testing.T) {
	g := newExplorerGate(t, admission.Limits{MaxConcurrent: 2, MemoryBytes: 512 << 20, ScratchBytes: 64 << 20}, true)
	setup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(setup, os.Getenv("KELVO_TEST_WARM_POSTGRES_DSN"))
	if err != nil {
		t.Fatal("fixture connection failed")
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(setup, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"); err != nil {
		t.Fatal("pg_stat_statements unavailable:", err)
	}
	if _, err := conn.Exec(setup, "SELECT pg_stat_statements_reset()"); err != nil {
		t.Fatal(err)
	}
	const panels, viewers = 6, 10
	panel := func(i int) string {
		return fmt.Sprintf("SELECT count(*)::bigint AS n, sum(amount)::float8 AS total FROM public.explorer_t%d WHERE id > 0", i)
	}
	calls := func() []int64 {
		out := make([]int64, panels)
		for i := range out {
			if err := conn.QueryRow(setup, "SELECT coalesce(sum(calls), 0)::bigint FROM pg_stat_statements WHERE query LIKE $1",
				fmt.Sprintf("%%sum(amount)%%explorer_t%d %%", i)).Scan(&out[i]); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	open := func(label string, refresh bool) burstResult {
		durations := make([]time.Duration, 0, panels*viewers)
		var mu sync.Mutex
		var failures atomic.Int64
		start := make(chan struct{})
		var wg sync.WaitGroup
		for v := 0; v < viewers; v++ {
			for p := 0; p < panels; p++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					sink := &workerTestSink{}
					<-start
					began := time.Now()
					_, err := g.executor.Execute(ctx, query.Request{Mode: "native", ConnectionID: "explorer", SQL: panel(p), Refresh: refresh}, sink)
					if err == nil && sink.rows != 1 {
						err = fmt.Errorf("rows %d", sink.rows)
					}
					if err != nil {
						failures.Add(1)
						t.Errorf("%s viewer %d panel %d: %v", label, v, p, err)
					}
					mu.Lock()
					durations = append(durations, time.Since(began))
					mu.Unlock()
				}()
			}
		}
		close(start)
		wg.Wait()
		slices.Sort(durations)
		return burstResult{N: len(durations), Failures: int(failures.Load()), P50MS: percentile(durations, 50), P95MS: percentile(durations, 95), MaxMS: percentile(durations, 100)}
	}

	first := open("refresh-1", false)
	afterFirst := calls()
	time.Sleep(g.mgr.cache.ttl + 500*time.Millisecond)
	second := open("refresh-2", false)
	afterSecond := calls()
	sink := &workerTestSink{}
	if _, err := g.executor.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "explorer", SQL: panel(0), Refresh: true}, sink); err != nil {
		t.Fatal("explicit refresh failed:", err)
	}
	afterRefresh := calls()

	report, _ := json.Marshal(map[string]any{"viewers": viewers, "panels": panels, "refresh_1": first, "refresh_2": second,
		"source_calls_after_refresh_1": afterFirst, "source_calls_after_refresh_2": afterSecond, "source_calls_after_explicit_refresh": afterRefresh,
		"cache_hits": g.mgr.cache.hits, "cache_ttl_ms": g.mgr.cache.ttl.Milliseconds(), "spawns": g.mgr.Metrics().Spawns.Load()})
	t.Logf("GATE_METRICS %s", report)
	for i := 0; i < panels; i++ {
		wantRefresh := int64(2)
		if i == 0 {
			wantRefresh = 3
		}
		if afterFirst[i] != 1 || afterSecond[i] != 2 || afterRefresh[i] != wantRefresh {
			t.Fatal("dashboard viewers did not share one source query per panel per refresh", string(report))
		}
	}
	if first.Failures+second.Failures != 0 {
		t.Fatal("dashboard reads failed", string(report))
	}
	g.drain()
}
