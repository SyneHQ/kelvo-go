//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
// median of a small sample.
func median(values []float64) float64 {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return sorted[len(sorted)/2]
}

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
	// Compare the median per-burst p95, over several bursts each, so one noisy
	// burst on a shared runner cannot decide the gate.
	const rounds = 7
	var loadedP95, idleP95 []float64
	var loaded, idle burstResult
	for i := 0; i < rounds; i++ {
		sample()
		_, one := g.burst(fmt.Sprintf("warm-under-load-%d", i))
		loadedP95 = append(loadedP95, one.P95MS)
		loaded.N, loaded.Failures = loaded.N+one.N, loaded.Failures+one.Failures
	}
	sample()
	cancelLoad()
	loaders.Wait()
	for i := 0; i < rounds; i++ {
		_, one := g.burst(fmt.Sprintf("warm-idle-%d", i))
		idleP95 = append(idleP95, one.P95MS)
		idle.N, idle.Failures = idle.N+one.N, idle.Failures+one.Failures
	}
	loadedMedian, idleMedian := median(loadedP95), median(idleP95)
	loaded.P95MS, idle.P95MS = loadedMedian, idleMedian

	metrics := g.mgr.Metrics()
	report, _ := json.Marshal(map[string]any{"cold_under_load": cold, "loaded": loaded, "idle": idle, "loaded_p95_per_burst": loadedP95, "idle_p95_per_burst": idleP95,
		"median_ratio": loadedMedian / idleMedian, "spawns": metrics.Spawns.Load(), "fallbacks": metrics.Fallbacks.Load(),
		"background_slots": 2, "background_full_samples": fmt.Sprintf("%d/%d", fullSamples, samples), "analysis_runs": analysisRuns.Load(), "analysis_failures": analysisFailures.Load()})
	t.Logf("GATE_METRICS %s", report)
	// Separate admission classes reserve slots, not CPU: background children
	// run with a lower cpu.weight (containment.CPUWeightBackground), so under
	// load the explorer's median p95 stays within 1.5x plus 25 ms of idle. A
	// blocked or queued explorer would add seconds, not milliseconds.
	if metrics.Spawns.Load() != 1 || metrics.Fallbacks.Load() != 0 || fullSamples != samples || loaded.Failures+idle.Failures+cold.Failures != 0 ||
		loadedMedian > idleMedian*1.5+25 {
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
	// pg_stat_statements counts a statement when its portal is destroyed, which
	// for a read inside a read-only transaction is when ROLLBACK runs, and the
	// adapter finishes that ROLLBACK after it has returned the result. Wait until
	// no source backend is mid-transaction or running before reading the counts.
	settle := func() {
		deadline := time.Now().Add(15 * time.Second)
		for {
			var busy int
			if err := conn.QueryRow(setup, "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND (state = 'active' OR state LIKE 'idle in transaction%')").Scan(&busy); err != nil {
				t.Fatal(err)
			}
			if busy == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d source backends still busy after the reads returned", busy)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	calls := func() []int64 {
		settle()
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
					_, err := g.executor.Execute(ctx, query.Request{Mode: "native", ConnectionID: "explorer", SQL: panel(p), BypassCache: refresh}, sink)
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
	if _, err := g.executor.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "explorer", SQL: panel(0), BypassCache: true}, sink); err != nil {
		t.Fatal("explicit refresh failed:", err)
	}
	afterRefresh := calls()

	report, _ := json.Marshal(map[string]any{"viewers": viewers, "panels": panels, "refresh_1": first, "refresh_2": second,
		"source_calls_after_refresh_1": afterFirst, "source_calls_after_refresh_2": afterSecond, "source_calls_after_explicit_refresh": afterRefresh,
		"cache_hits": g.mgr.cache.Hits(), "cache_ttl_ms": g.mgr.cache.ttl.Milliseconds(), "spawns": g.mgr.Metrics().Spawns.Load()})
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

// gateSecrets is a source secret store the gate can rotate, lose (the
// credential is not found: a definitive denial) or hold (a slow store).
type gateSecrets struct {
	mu      sync.Mutex
	value   string
	missing bool
	hold    chan struct{}
}

func (s *gateSecrets) Resolve(ctx context.Context, name string) (string, bool, error) {
	s.mu.Lock()
	hold, value, missing := s.hold, s.value, s.missing
	s.mu.Unlock()
	if name != gateSourceSecret {
		return "", false, nil
	}
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return "", true, ctx.Err()
		}
	}
	if missing {
		return "", false, nil
	}
	return value, true, nil
}

func (s *gateSecrets) set(update func(*gateSecrets)) {
	s.mu.Lock()
	update(s)
	s.mu.Unlock()
}

// gateSourceSecret is not in the process environment, so a lost credential is
// not found rather than read from the environment.
const gateSourceSecret = "KELVO_GATE_SOURCE_DSN"

// newAuthorizationGate serves the explorer source from gateSecrets with
// reauthorize_every at window.
func newAuthorizationGate(t *testing.T, window time.Duration) (*explorerGate, *gateSecrets, func() (int64, error)) {
	g := newExplorerGate(t, admission.Limits{MaxConcurrent: 2, MemoryBytes: 512 << 20, ScratchBytes: 64 << 20}, false)
	g.mgr.cfg.ReauthorizeEvery = window
	secrets := &gateSecrets{value: os.Getenv("KELVO_TEST_WARM_POSTGRES_DSN")}
	g.executor.Config = catalog.Config{Sources: []catalog.Source{{ID: "explorer", Type: "postgres", DSNEnv: gateSourceSecret}}}
	g.executor.Secrets = secrets
	pid := func() (int64, error) {
		var got int64
		sink := &workerTestSink{write: func(b arrow.RecordBatch) error {
			got = b.Column(0).(*array.Int64).Value(0)
			return nil
		}}
		_, err := g.executor.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "explorer", SQL: "SELECT pg_backend_pid()::bigint AS pid"}, sink)
		return got, err
	}
	return g, secrets, pid
}

// This gate shows the phase 3 trade-off on a real adapter and PostgreSQL:
// one authorization serves many secretless reads; a revoked source (its
// credential gone) and a source changed to credentials that no longer work
// are refused within reauthorize_every (5s here, 60s by default); a rotated,
// working source moves to a new worker and backend within the same bound,
// draining the old worker rather than killing it.
func TestContainedRevokedOrChangedSourceRefusedWithinReauthorization(t *testing.T) {
	const window = 5 * time.Second
	g, secrets, pid := newAuthorizationGate(t, window)
	dsn := secrets.value
	untilRefused := func(label string) (time.Duration, int) {
		changed, served := time.Now(), 0
		for time.Since(changed) < window+5*time.Second {
			if _, err := pid(); err != nil {
				return time.Since(changed), served
			}
			served++
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("%s source still served %v after the change", label, time.Since(changed))
		return 0, 0
	}

	first, err := pid()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 9; i++ {
		if again, err := pid(); err != nil || again < 1 {
			t.Fatal(again, err)
		}
	}
	authorizationsForTen := g.mgr.Metrics().Authorizations.Load()

	secrets.set(func(s *gateSecrets) { s.missing = true })
	revokedAfter, servedAfterRevoke := untilRefused("revoked")
	revokedKills := g.mgr.Metrics().EvictionsAuth.Load()

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	values := parsed.Query()
	values.Set("connect_timeout", "7")
	parsed.RawQuery = values.Encode()
	rotated := parsed.String()
	secrets.set(func(s *gateSecrets) { s.missing, s.value = false, rotated })
	second, err := pid()
	if err != nil {
		t.Fatal("restored source refused:", err)
	}
	// Rotate again to working credentials: the old worker drains.
	values.Set("connect_timeout", "8")
	parsed.RawQuery = values.Encode()
	secrets.set(func(s *gateSecrets) { s.value = parsed.String() })
	time.Sleep(window + 200*time.Millisecond)
	third, err := pid()
	if err != nil {
		t.Fatal("rotated source refused:", err)
	}

	parsed.User = url.UserPassword(parsed.User.Username(), "not-the-password")
	secrets.set(func(s *gateSecrets) { s.value = parsed.String() })
	changedAfter, servedAfterChange := untilRefused("changed")

	metrics := g.mgr.Metrics()
	report, _ := json.Marshal(map[string]any{"reauthorize_every_ms": window.Milliseconds(), "authorizations_for_first_10_reads": authorizationsForTen,
		"revoked_refused_after_ms": revokedAfter.Milliseconds(), "reads_served_after_revoke": servedAfterRevoke,
		"changed_refused_after_ms": changedAfter.Milliseconds(), "reads_served_after_change": servedAfterChange,
		"evictions_auth": metrics.EvictionsAuth.Load(), "rotations": metrics.Rotations.Load(), "spawns": metrics.Spawns.Load(), "fallbacks": metrics.Fallbacks.Load(),
		"backend_before": first, "backend_after_restore": second, "backend_after_rotation": third})
	t.Logf("GATE_METRICS %s", report)
	if metrics.Fallbacks.Load() != 0 || authorizationsForTen != 1 || revokedAfter > window+time.Second || changedAfter > window+time.Second ||
		revokedKills != 1 || metrics.Rotations.Load() < 1 || first == second || second == third {
		t.Fatal("source change not enforced within reauthorize_every", string(report))
	}
	g.drain()
}

// Concurrent reads that find the authorization stale share one
// re-authorization; a caller cancelling during it does not disturb the
// others or the shared worker.
func TestContainedConcurrentReauthorizationKeepsWorker(t *testing.T) {
	const window = 2 * time.Second
	g, secrets, pid := newAuthorizationGate(t, window)
	first, err := pid()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(window + 200*time.Millisecond)
	hold := make(chan struct{})
	secrets.set(func(s *gateSecrets) { s.hold = hold })
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leader := make(chan error, 1)
	go func() {
		_, err := g.executor.Execute(leaderCtx, query.Request{Mode: "native", ConnectionID: "explorer", SQL: "SELECT 1"}, &workerTestSink{})
		leader <- err
	}()
	time.Sleep(100 * time.Millisecond)
	var wg sync.WaitGroup
	pids := make([]int64, 8)
	errs := make([]error, 8)
	for i := range pids {
		wg.Add(1)
		go func() { defer wg.Done(); pids[i], errs[i] = pid() }()
	}
	time.Sleep(100 * time.Millisecond)
	cancelLeader()
	leaderErr := <-leader
	secrets.set(func(s *gateSecrets) { s.hold = nil })
	close(hold)
	wg.Wait()
	metrics := g.mgr.Metrics()
	failures := 0
	for _, err := range errs {
		if err != nil {
			failures++
		}
	}
	report, _ := json.Marshal(map[string]any{"concurrent_reads": len(pids), "failures": failures, "leader_cancelled": errors.Is(leaderErr, context.Canceled),
		"authorizations": metrics.Authorizations.Load(), "spawns": metrics.Spawns.Load(), "evictions_auth": metrics.EvictionsAuth.Load(), "backend_before": first, "backends_after": pids})
	t.Logf("GATE_METRICS %s", report)
	if failures != 0 || !errors.Is(leaderErr, context.Canceled) || metrics.Spawns.Load() != 1 || metrics.EvictionsAuth.Load() != 0 || metrics.Authorizations.Load() > 3 {
		t.Fatal("a cancelled re-authorization disturbed other callers", string(report))
	}
	g.drain()
}
