//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kelvo "github.com/SYNEHQ/kelvo-go/client"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
)

// memoryJobStore is a complete in-process job store: the gateway submits,
// the node dispatches and both read and update the same jobs. It stands in
// for NATS only; every job transition still goes through compare-and-swap.
type memoryJobStore struct {
	policy Policy
	mu     sync.Mutex
	jobs   map[string]Snapshot
	queue  chan string
	next   atomic.Int64
}

type memoryDelivery struct{ id string }

func (d memoryDelivery) ID() string                  { return d.id }
func (d memoryDelivery) Ack(context.Context) error   { return nil }
func (d memoryDelivery) Retry(context.Context) error { return nil }

func newMemoryJobStore(p Policy) *memoryJobStore {
	return &memoryJobStore{policy: p, jobs: map[string]Snapshot{}, queue: make(chan string, 1024)}
}

func (s *memoryJobStore) Policy() Policy { return s.policy }

func (s *memoryJobStore) Submit(ctx context.Context, r query.Request) (Snapshot, error) {
	if err := query.ValidateRequest(r); err != nil {
		return Snapshot{}, err
	}
	authority, err := submissionAuthority(ctx, s.policy, r)
	if err != nil {
		return Snapshot{}, err
	}
	now := time.Now().UTC()
	id, err := newID(int(s.next.Add(1)) % s.policy.MaxQueries)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{Job: Job{Authority: authority, ID: id, TenantID: s.policy.TenantID, State: Queued, Request: r, CreatedAt: now, ExpiresAt: now.Add(s.policy.JobTTL), HeartbeatAt: now}, Revision: 1}
	s.mu.Lock()
	s.jobs[id] = snapshot
	s.mu.Unlock()
	return snapshot, nil
}

func (s *memoryJobStore) Get(_ context.Context, id string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.jobs[id]
	if !ok {
		return v, ErrNotFound
	}
	return v, nil
}

func (s *memoryJobStore) CompareAndSwap(_ context.Context, old Snapshot, next Job) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.jobs[old.Job.ID]
	if !ok || v.Revision != old.Revision {
		return Snapshot{}, ErrConflict
	}
	v = Snapshot{Job: next, Revision: v.Revision + 1}
	s.jobs[next.ID] = v
	return v, nil
}

func (s *memoryJobStore) Enqueue(ctx context.Context, id string) error {
	select {
	case s.queue <- id:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *memoryJobStore) Next(ctx context.Context) (Delivery, error) {
	select {
	case id := <-s.queue:
		return memoryDelivery{id}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(50 * time.Millisecond):
		return nil, ErrNoJob
	}
}

func (s *memoryJobStore) Reconcile(context.Context) error                       { return nil }
func (s *memoryJobStore) ClaimWorker(context.Context, string, string) error     { return nil }
func (s *memoryJobStore) HeartbeatWorker(context.Context, string, string) error { return nil }
func (s *memoryJobStore) Close() error                                          { return nil }

// countingClientSink is the caller's view of a result.
type countingClientSink struct{ rows, batches int64 }

func (s *countingClientSink) Schema(*arrow.Schema) error { return nil }
func (s *countingClientSink) Write(b arrow.RecordBatch) error {
	s.rows += b.NumRows()
	s.batches++
	return nil
}

// This gate drives native reads through the real Go client, gateway and
// node to one contained read worker against real PostgreSQL. The client
// checks the job's rows, batches and wire bytes against what it decoded, so a
// read-worker path that misreports them fails here (PROTOCOL_ERROR).
func TestContainedReadWorkerThroughGoClient(t *testing.T) {
	adapterPath, dsn := os.Getenv("KELVO_TEST_WARM_ADAPTER_BINARY"), os.Getenv("KELVO_TEST_WARM_POSTGRES_DSN")
	root, state, launcher := os.Getenv("KELVO_TEST_CGROUP_ROOT"), os.Getenv("KELVO_TEST_CGROUP_STATE"), os.Getenv("KELVO_TEST_SANDBOX")
	binary := os.Getenv("KELVO_TEST_BINARY") // the query child, for the node's startup probe
	if adapterPath == "" || dsn == "" || root == "" || state == "" || launcher == "" || binary == "" {
		t.Skip("set the warm adapter, PostgreSQL, cgroup and sandbox variables for the client gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal("fixture connection failed")
	}
	for _, statement := range []string{
		"DROP TABLE IF EXISTS public.client_gate",
		"CREATE TABLE public.client_gate (id bigint PRIMARY KEY, label text NOT NULL)",
		"INSERT INTO public.client_gate SELECT g, 'row ' || g FROM generate_series(1, 5000) g",
	} {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.Close(ctx)

	manager, err := containment.Open(containment.Config{Root: root, StateDirectory: state})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := manager.Close(cleanup); err != nil {
			t.Error("containment cleanup", err)
		}
	})
	pool, err := admission.New(admission.Limits{MaxConcurrent: 2, MemoryBytes: 512 << 20, ScratchBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	config := catalog.Config{Sources: []catalog.Source{{ID: "pg", Type: "postgres", DSNEnv: "KELVO_TEST_WARM_POSTGRES_DSN"}}}
	p := testPolicy()
	p.Workers, p.MaxQueries = map[string]int{"a1": 8}, 64
	p.JobTTL, p.LeaseDuration = 2*time.Minute, 10*time.Second
	p.Limits.Timeout, p.Limits.MemoryMB, p.Limits.MaxTempMB, p.Limits.Threads = 25*time.Second, 64, 16, 1
	p.Limits.MaxBytes, p.Limits.MaxRows, p.Limits.ResultCompression = 64<<20, 1_000_000, "none"
	if err := ValidatePolicy(p); err != nil {
		t.Fatal(err)
	}
	scratchDir := t.TempDir()
	if err := os.Chmod(scratchDir, 0700); err != nil {
		t.Fatal(err)
	}
	scratch, err := worker.OpenScratchRoot(scratchDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scratch.Close() })
	e, err := worker.New(config, p.Limits)
	if err != nil {
		t.Fatal(err)
	}
	e.Binary, e.SandboxPath, e.ScratchRoot = binary, launcher, scratch
	e.Containment, e.ContainmentBudget = manager, containment.Budget{NativeOverheadMB: 64, ParentOverheadMB: 32, MaxProcesses: 64}
	e.ResourcePool, e.ResourceOverheadBytes = pool, 160<<20
	adapterPath, err = filepath.EvalSymlinks(adapterPath)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(adapterPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	warm := worker.DefaultWarmPoolConfig()
	warm.Enabled = true
	mgr, err := worker.NewWarmPoolManager(warm, nil)
	if err != nil {
		t.Fatal(err)
	}
	mgr.Adapter = worker.OperationProcessConfig{Binary: adapterPath, SHA256: hex.EncodeToString(sum[:])}
	e.WarmPool = mgr

	store := newMemoryJobStore(p)
	node, err := NewNode(NodeConfig{Policy: p, WorkerID: "a1", RuntimeResources: pool, SandboxPath: launcher}, store, e)
	if err != nil {
		t.Fatal("start node:", err)
	}
	gatewayTLS, workersTLS := protectedQueryTLS(t)
	serverTLS, err := BuildServerTLS(workersTLS["a"], WorkerIdentity("a", "a1"), GatewayIdentity)
	if err != nil {
		t.Fatal(err)
	}
	nodeServer := httptest.NewUnstartedServer(node)
	nodeServer.TLS = serverTLS
	nodeServer.StartTLS()
	const token = "read-worker-client-gate-token-0123456789abcdef"
	t.Setenv("KELVO_CLIENT_GATE_TOKEN", token)
	gateway, err := NewGateway(GatewayConfig{WorkerTLS: gatewayTLS, MaxHTTPRequests: 64,
		Tenants: []TenantConfig{{Policy: p, TokenEnv: "KELVO_CLIENT_GATE_TOKEN", Workers: []Endpoint{{ID: "a1", URL: nodeServer.URL}}}}}, map[string]Store{"a": store})
	if err != nil {
		t.Fatal("start gateway:", err)
	}
	public := httptest.NewTLSServer(gateway)
	// Shut down in order: client side, gateway, node, then the pool's workers.
	t.Cleanup(func() {
		public.Close()
		_ = gateway.Close()
		nodeServer.Close()
		_ = node.Close()
		_ = mgr.Drain(context.Background())
	})
	client, err := kelvo.New(kelvo.Config{URL: public.URL, BearerToken: token, TLSConfig: public.Client().Transport.(*http.Transport).TLSClientConfig, MaxConcurrent: 16})
	if err != nil {
		t.Fatal(err)
	}

	read := func(sql string, wantRows int64) error {
		sink := &countingClientSink{}
		stats, err := client.Query(ctx, query.Request{Mode: "native", ConnectionID: "pg", SQL: sql}, kelvo.Authority{}, sink)
		if err != nil {
			return err
		}
		if stats.Rows != wantRows || sink.rows != wantRows || stats.Batches != sink.batches || sink.batches < 1 {
			return fmt.Errorf("client saw rows=%d batches=%d, stats rows=%d batches=%d", sink.rows, sink.batches, stats.Rows, stats.Batches)
		}
		return nil
	}
	// Sequential reads, a multi-batch read, then a parallel burst.
	for i := 0; i < 5; i++ {
		if err := read("SELECT id, label FROM public.client_gate ORDER BY id LIMIT 50", 50); err != nil {
			t.Fatal("sequential read:", err)
		}
	}
	if err := read("SELECT id, label FROM public.client_gate ORDER BY id", 5000); err != nil {
		t.Fatal("multi-batch read:", err)
	}
	var wg sync.WaitGroup
	var failures atomic.Int64
	var firstErr atomic.Value
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := read(fmt.Sprintf("SELECT id, label FROM public.client_gate WHERE id > %d ORDER BY id LIMIT 20", i), 20); err != nil {
				failures.Add(1)
				firstErr.CompareAndSwap(nil, err.Error())
			}
		}()
	}
	wg.Wait()
	metrics := mgr.Metrics()
	report, _ := json.Marshal(map[string]any{"reads": 22, "failures": failures.Load(), "first_error": firstErr.Load(),
		"spawns": metrics.Spawns.Load(), "fallbacks": metrics.Fallbacks.Load()})
	t.Logf("GATE_METRICS %s", report)
	if failures.Load() != 0 || metrics.Spawns.Load() != 1 || metrics.Fallbacks.Load() != 0 {
		t.Fatal("reads through the Go client failed or did not use the read worker", string(report))
	}
}
