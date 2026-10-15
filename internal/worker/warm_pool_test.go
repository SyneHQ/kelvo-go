// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapter/frame"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type fakeWorkerProcess struct {
	stdinR  *io.PipeReader
	stdinW  *io.PipeWriter
	stdoutR *io.PipeReader
	stdoutW *io.PipeWriter

	killed atomic.Bool
	closed atomic.Bool
	done   chan struct{}
}

func newFakeWorkerProcess() *fakeWorkerProcess {
	sinR, sinW := io.Pipe()
	soutR, soutW := io.Pipe()
	p := &fakeWorkerProcess{
		stdinR:  sinR,
		stdinW:  sinW,
		stdoutR: soutR,
		stdoutW: soutW,
		done:    make(chan struct{}),
	}
	return p
}

func (p *fakeWorkerProcess) Stdin() io.Writer { return p.stdinW }
func (p *fakeWorkerProcess) Stdout() io.Reader { return p.stdoutR }
func (p *fakeWorkerProcess) Kill() error {
	p.killed.Store(true)
	_ = p.stdinW.Close()
	_ = p.stdoutW.Close()
	return nil
}
func (p *fakeWorkerProcess) Wait() error {
	_ = p.stdinR.Close()
	_ = p.stdoutR.Close()
	return nil
}

type mockLauncher struct {
	mu       sync.Mutex
	spawns   int
	onLaunch func(ctx context.Context) (WorkerProcess, func() error, error)
}

func (l *mockLauncher) Launch(ctx context.Context, e *Executor, cfg OperationProcessConfig, limits query.Limits) (WorkerProcess, func() error, error) {
	l.mu.Lock()
	l.spawns++
	l.mu.Unlock()
	if l.onLaunch != nil {
		return l.onLaunch(ctx)
	}
	proc := newFakeWorkerProcess()
	go serveFake(proc, nil)
	cleanup := func() error {
		return proc.Kill()
	}
	return proc, cleanup, nil
}

func testProcessRequest(opID, tenant, team, conn, rev string) adapter.ProcessRequest {
	now := time.Now()
	req := operations.Request{
		Version: operations.Version,
		Kind:    operations.QueryRead,
		Connection: operations.ConnectionRef{
			ID: conn, Database: "testdb", Schema: "public",
		},
		Spec: operations.Spec{
			Query: &operations.QuerySpec{SQL: "SELECT 1"},
		},
	}
	digest, _ := operations.Digest(req)
	return adapter.ProcessRequest{
		Version:       adapter.ProcessVersion,
		OperationID:   opID,
		RequestSHA256: digest,
		Request:       req,
		Limits: adapter.ProcessLimits{
			MaxRows:   100,
			MaxBytes:  1 << 20,
			BatchRows: 10,
			TimeoutMS: 5000,
		},
		Source: adapter.ConnectionSpec{
			Engine:       "postgres",
			TenantID:     tenant,
			ConnectionID: conn,
			Revision:     rev,
			Database:     "testdb",
			Schema:       "public",
		},
		AppTeam:               team,
		CredentialsValidUntil: now.Unix() + 30,
		ExpiresAt:             now.Unix() + 60,
	}
}

func TestWarmPool_ConfigValidation(t *testing.T) {
	// Disabled config passes
	disabled := WarmPoolConfig{Enabled: false}
	if err := disabled.Validate(); err != nil {
		t.Fatalf("disabled config should pass validation, got %v", err)
	}

	valid := DefaultWarmPoolConfig()
	valid.Enabled = true
	if err := valid.Validate(); err != nil {
		t.Fatalf("default enabled config should pass validation, got %v", err)
	}

	// Invalid cases
	cases := []struct {
		name string
		cfg  WarmPoolConfig
	}{
		{"idle_ttl_too_low", func() WarmPoolConfig { c := valid; c.IdleTTL = 1 * time.Millisecond; return c }()},
		{"idle_ttl_too_high", func() WarmPoolConfig { c := valid; c.IdleTTL = 48 * time.Hour; return c }()},
		{"max_workers_zero", func() WarmPoolConfig { c := valid; c.MaxWorkers = 0; return c }()},
		{"max_workers_excessive", func() WarmPoolConfig { c := valid; c.MaxWorkers = 1000; return c }()},
		{"max_per_key_greater_than_max_workers", func() WarmPoolConfig { c := valid; c.MaxWorkers = 2; c.MaxPerKey = 5; return c }()},
		{"queue_depth_zero", func() WarmPoolConfig { c := valid; c.QueueDepth = 0; return c }()},
		{"worker_memory_too_low", func() WarmPoolConfig { c := valid; c.WorkerMemoryMB = 8; return c }()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); err == nil {
				t.Fatalf("expected validation error for %s, got nil", tc.name)
			}
		})
	}
}

func TestWarmPool_AcquireAndWarmHit(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	launcher := &mockLauncher{}

	mgr, err := NewWarmPoolManager(cfg, launcher)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()

	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	req1 := testProcessRequest("op-1", "tenant-a", "team-a", "conn-1", strings.Repeat("1", 64))

	// Acquire 1 -> miss, spawns worker
	w1, release1, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req1)
	if err != nil {
		t.Fatalf("acquire 1 failed: %v", err)
	}
	if mgr.Metrics().Spawns.Load() != 1 {
		t.Fatalf("expected 1 spawn, got %d", mgr.Metrics().Spawns.Load())
	}
	if mgr.Metrics().Misses.Load() != 1 {
		t.Fatalf("expected 1 miss, got %d", mgr.Metrics().Misses.Load())
	}

	// Release with nil err -> keeps warm
	release1(nil)

	// Acquire 2 with same key -> hit!
	req2 := testProcessRequest("op-2", "tenant-a", "team-a", "conn-1", strings.Repeat("1", 64))
	w2, release2, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req2)
	if err != nil {
		t.Fatalf("acquire 2 failed: %v", err)
	}
	if w2.id != w1.id {
		t.Fatalf("expected same worker reused, got %s vs %s", w1.id, w2.id)
	}
	if mgr.Metrics().Hits.Load() != 1 {
		t.Fatalf("expected 1 hit, got %d", mgr.Metrics().Hits.Load())
	}
	if mgr.Metrics().Spawns.Load() != 1 {
		t.Fatalf("expected spawns to remain 1, got %d", mgr.Metrics().Spawns.Load())
	}
	release2(nil)
}

func TestWarmPool_TTLExpiry(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	cfg.IdleTTL = 50 * time.Millisecond
	launcher := &mockLauncher{}

	mgr, err := NewWarmPoolManager(cfg, launcher)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()

	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	req := testProcessRequest("op-ttl", "tenant-a", "team-a", "conn-1", strings.Repeat("1", 64))

	_, release, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req)
	if err != nil {
		t.Fatal(err)
	}
	release(nil)

	// Sleep longer than IdleTTL
	time.Sleep(150 * time.Millisecond)

	// Trigger reap or verify metric
	mgr.reapExpired()

	if mgr.Metrics().EvictionsTTL.Load() < 1 {
		t.Fatalf("expected at least 1 TTL eviction, got %d", mgr.Metrics().EvictionsTTL.Load())
	}
}

func TestWarmPool_RevisionChangeEviction(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	launcher := &mockLauncher{}

	mgr, err := NewWarmPoolManager(cfg, launcher)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()

	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	req1 := testProcessRequest("op-rev1", "tenant-a", "team-a", "conn-1", strings.Repeat("1", 64))

	_, release1, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req1)
	if err != nil {
		t.Fatal(err)
	}
	release1(nil)

	// Now request arrives with revision 2 for same connection
	req2 := testProcessRequest("op-rev2", "tenant-a", "team-a", "conn-1", strings.Repeat("2", 64))
	_, release2, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req2)
	if err != nil {
		t.Fatal(err)
	}
	release2(nil)

	// Give async evict goroutine a moment
	time.Sleep(20 * time.Millisecond)

	if mgr.Metrics().EvictionsRev.Load() < 1 {
		t.Fatalf("expected revision eviction count >= 1, got %d", mgr.Metrics().EvictionsRev.Load())
	}
}

// Source credentials live for seconds. A lapsed window must not evict an idle
// worker; the next authorized request for the same revision reuses it and
// brings its own window.
func TestWarmPool_IdleWorkerSurvivesCredentialWindow(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	mgr, err := NewWarmPoolManager(cfg, &mockLauncher{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()

	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	req := testProcessRequest("op-cred", "tenant-a", "team-a", "conn-1", strings.Repeat("1", 64))
	w, release, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req)
	if err != nil {
		t.Fatal(err)
	}
	w.credentialsValidUntil = time.Now().Unix() - 1
	release(nil)
	mgr.reapExpired()

	req2 := testProcessRequest("op-cred2", "tenant-a", "team-a", "conn-1", strings.Repeat("1", 64))
	w2, release2, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req2)
	if err != nil {
		t.Fatal(err)
	}
	release2(nil)
	if w2 != w || w2.credentialsValidUntil != req2.CredentialsValidUntil || mgr.Metrics().Hits.Load() != 1 || mgr.Metrics().EvictionsTTL.Load() != 0 {
		t.Fatalf("idle worker was not reused after its credential window: hits=%d evictions=%d", mgr.Metrics().Hits.Load(), mgr.Metrics().EvictionsTTL.Load())
	}
}

func TestWarmPool_KillOnFailure(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	launcher := &mockLauncher{}

	mgr, err := NewWarmPoolManager(cfg, launcher)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()

	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	req := testProcessRequest("op-fail", "tenant-a", "team-a", "conn-1", strings.Repeat("1", 64))

	w, release, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req)
	if err != nil {
		t.Fatal(err)
	}

	// Release with an error -> worker must be killed and evicted
	simulatedErr := errors.New("query failed: connection reset by peer")
	release(simulatedErr)

	if mgr.Metrics().EvictionsErr.Load() != 1 {
		t.Fatalf("expected 1 error eviction, got %d", mgr.Metrics().EvictionsErr.Load())
	}

	// Verify worker is no longer in manager
	mgr.mu.Lock()
	_, found := mgr.workers[w.id]
	mgr.mu.Unlock()
	if found {
		t.Fatal("poisoned/failed worker should not remain in pool")
	}
}

func TestWarmPool_QueueBoundAndFailFast(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	cfg.MaxWorkers = 1
	cfg.MaxPerKey = 1
	cfg.QueueDepth = 2
	cfg.QueueTimeout = 200 * time.Millisecond
	launcher := &mockLauncher{}

	mgr, err := NewWarmPoolManager(cfg, launcher)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()

	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	req1 := testProcessRequest("op-1", "t", "team", "c", strings.Repeat("1", 64))

	// Acquire 1 (holds the 1 slot)
	_, release1, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req1)
	if err != nil {
		t.Fatal(err)
	}
	defer release1(nil)

	// Queue waiter 1
	req2 := testProcessRequest("op-2", "t", "team", "c", strings.Repeat("1", 64))
	done2 := make(chan error, 1)
	go func() {
		_, rel2, err2 := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req2)
		if err2 == nil {
			rel2(nil)
		}
		done2 <- err2
	}()

	// Queue waiter 2
	req3 := testProcessRequest("op-3", "t", "team", "c", strings.Repeat("1", 64))
	done3 := make(chan error, 1)
	go func() {
		_, rel3, err3 := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req3)
		if err3 == nil {
			rel3(nil)
		}
		done3 <- err3
	}()

	time.Sleep(30 * time.Millisecond)

	// Request 4 arrives when queue is full (depth 2) -> fail-fast!
	req4 := testProcessRequest("op-4", "t", "team", "c", strings.Repeat("1", 64))
	_, _, err4 := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req4)
	if err4 == nil {
		t.Fatal("expected fail-fast RESOURCE_EXHAUSTED error when queue is full, got nil")
	}
	if !strings.Contains(err4.Error(), "RESOURCE_EXHAUSTED") && !strings.Contains(err4.Error(), "queue depth exceeded") {
		t.Fatalf("unexpected error message: %v", err4)
	}

	// Wait for queue timeout on req2 and req3
	errWait2 := <-done2
	if errWait2 == nil {
		t.Fatal("expected queue timeout for req2")
	}
	errWait3 := <-done3
	if errWait3 == nil {
		t.Fatal("expected queue timeout for req3")
	}
}

func TestWarmPool_ConcurrentSameKeySerialization(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	cfg.MaxWorkers = 4
	cfg.MaxPerKey = 1 // Only 1 worker per connection key!
	launcher := &mockLauncher{}

	mgr, err := NewWarmPoolManager(cfg, launcher)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()

	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	reqKey := testProcessRequest("op-conc", "t", "team", "c", strings.Repeat("1", 64))

	var activeCount atomic.Int32
	var maxObservedConcurrent atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := reqKey
			req.OperationID = reqKey.OperationID + string(rune('0'+idx))
			_, release, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req)
			if err != nil {
				t.Errorf("acquire concurrent failed: %v", err)
				return
			}
			curr := activeCount.Add(1)
			for {
				old := maxObservedConcurrent.Load()
				if curr <= old || maxObservedConcurrent.CompareAndSwap(old, curr) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			activeCount.Add(-1)
			release(nil)
		}(i)
	}

	wg.Wait()

	if maxObservedConcurrent.Load() > 1 {
		t.Fatalf("expected MaxPerKey=1 to serialize requests (max concurrent 1), but observed %d", maxObservedConcurrent.Load())
	}
}

func TestWarmPool_ShutdownDrain(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	cfg.MaxWorkers = 4
	launcher := &mockLauncher{}

	mgr, err := NewWarmPoolManager(cfg, launcher)
	if err != nil {
		t.Fatal(err)
	}

	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	req1 := testProcessRequest("op-d1", "t", "team", "c1", strings.Repeat("1", 64))
	req2 := testProcessRequest("op-d2", "t", "team", "c2", strings.Repeat("1", 64))

	_, rel1, _ := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req1)
	_, rel2, _ := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req2)
	rel1(nil)
	rel2(nil)

	if len(mgr.workers) != 2 {
		t.Fatalf("expected 2 idle workers, got %d", len(mgr.workers))
	}

	// Drain
	if err := mgr.Drain(context.Background()); err != nil {
		t.Fatalf("drain error: %v", err)
	}

	if mgr.Metrics().EvictionsDrain.Load() != 2 {
		t.Fatalf("expected 2 drain evictions, got %d", mgr.Metrics().EvictionsDrain.Load())
	}
	if len(mgr.workers) != 0 {
		t.Fatalf("expected 0 workers after drain, got %d", len(mgr.workers))
	}
}

func TestWarmPool_OneShotUnchangedWhenFlagOff(t *testing.T) {
	// Proves that when WarmPool is disabled or nil, IsEligible returns false
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = false
	mgr, _ := NewWarmPoolManager(cfg, nil)

	req := testProcessRequest("op-off", "t", "team", "c", strings.Repeat("1", 64))
	if mgr.IsEligible(req) {
		t.Fatal("IsEligible must return false when warm pool is disabled")
	}
}

func TestWarmPool_BenchmarkTiming_ColdVsWarm(t *testing.T) {
	// Measures latency of cold connection + process setup vs warm hit reuse
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	cfg.MaxWorkers = 4

	launcher := &mockLauncher{
		onLaunch: func(ctx context.Context) (WorkerProcess, func() error, error) {
			// Simulate cold spawn: Landlock launcher + process spawn + TCP + TLS + connect
			time.Sleep(50 * time.Millisecond)
			proc := newFakeWorkerProcess()
			go serveFake(proc, nil)
			return proc, proc.Kill, nil
		},
	}

	mgr, err := NewWarmPoolManager(cfg, launcher)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()

	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	req := testProcessRequest("op-bench", "bench-tenant", "bench-team", "bench-conn", strings.Repeat("b", 64))

	// Cold execution (spawn + connect)
	coldStart := time.Now()
	wCold, releaseCold, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req)
	if err != nil {
		t.Fatal(err)
	}
	coldDuration := time.Since(coldStart)
	releaseCold(nil)

	// Warm hit execution (session reuse from pool)
	warmStart := time.Now()
	wWarm, releaseWarm, err := mgr.Acquire(context.Background(), exec, OperationProcessConfig{}, req)
	if err != nil {
		t.Fatal(err)
	}
	warmDuration := time.Since(warmStart)
	releaseWarm(nil)

	if wWarm.id != wCold.id {
		t.Fatalf("expected warm worker reuse")
	}

	t.Logf("BENCHMARK PROOF: Cold spawn acquire: %v | Warm hit acquire: %v (speedup: %.1fx)",
		coldDuration, warmDuration, float64(coldDuration)/float64(max(warmDuration, time.Microsecond)))

	if warmDuration >= coldDuration {
		t.Fatalf("expected warm hit (%v) to be significantly faster than cold (%v)", warmDuration, coldDuration)
	}
}


// replyOK answers a request on its stream with one Arrow batch and a Receipt
// whose result reference matches it, as the real adapter does.
func replyOK(w io.Writer, stream uint32, payload []byte) {
	var req adapter.ProcessRequest
	_ = json.Unmarshal(payload, &req)
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.Append(1)
	col := b.NewArray()
	b.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{col}, 1)
	col.Release()
	var wire bytes.Buffer
	iw := ipc.NewWriter(&wire, ipc.WithSchema(schema))
	_ = iw.Write(rec)
	_ = iw.Close()
	rec.Release()
	sum := sha256.Sum256(wire.Bytes())
	raw, _ := json.Marshal(operations.Receipt{
		Version: operations.Version, OperationID: req.OperationID, RequestSHA256: req.RequestSHA256,
		Outcome: operations.Completed, Effect: operations.EffectNone,
		Result: &operations.ResultRef{ID: req.OperationID, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(wire.Len()), Rows: 1, Format: "arrow_ipc"},
	})
	_ = frame.WriteResult(w, stream, wire.Bytes())
	_ = frame.WriteReceipt(w, stream, raw)
}

// serveFake speaks frame protocol v2 on proc: it answers Hello, then calls
// onFrame for each Request/Cancel (nil = reply with a completed Receipt on the
// request's stream). It returns when stdin closes or Close arrives.
func serveFake(proc *fakeWorkerProcess, onFrame func(f frame.Frame)) {
	if f, err := frame.Read(proc.stdinR); err != nil || f.Type != frame.Hello {
		return
	}
	if frame.WriteHello(proc.stdoutW) != nil {
		return
	}
	for {
		f, err := frame.Read(proc.stdinR)
		if err != nil || f.Type == frame.Close {
			return
		}
		if onFrame != nil {
			onFrame(f)
		} else if f.Type == frame.Request {
			replyOK(proc.stdoutW, f.Stream, f.Payload)
		}
	}
}

// fakeSet hands out fake processes, one per launch, serving with onFrame.
type fakeSet struct {
	mu     sync.Mutex
	procs  []*fakeWorkerProcess
	serve  func(proc *fakeWorkerProcess)
	frames []frame.Frame
}

func (fs *fakeSet) launcher() *mockLauncher {
	return &mockLauncher{onLaunch: func(context.Context) (WorkerProcess, func() error, error) {
		proc := newFakeWorkerProcess()
		fs.mu.Lock()
		fs.procs = append(fs.procs, proc)
		fs.mu.Unlock()
		go fs.serve(proc)
		return proc, proc.Kill, nil
	}}
}

func (fs *fakeSet) record(f frame.Frame) {
	fs.mu.Lock()
	fs.frames = append(fs.frames, f)
	fs.mu.Unlock()
}

func newTestPool(t *testing.T, l WorkerLauncher) (*WarmPoolManager, *Executor) {
	t.Helper()
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	mgr, err := NewWarmPoolManager(cfg, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Drain(context.Background()) })
	return mgr, &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
}

func opReq(id string) adapter.ProcessRequest {
	r := testProcessRequest(id, "tenant-a", "team-a", "conn-1", strings.Repeat("1", 64))
	r.CredentialsValidUntil = time.Now().Unix() + 4 // Validate caps the window at 5s
	return r
}

func TestWarmPool_HelloHandshakeSuccess(t *testing.T) {
	fs := &fakeSet{}
	fs.serve = func(p *fakeWorkerProcess) { serveFake(p, nil) }
	mgr, exec := newTestPool(t, fs.launcher())
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-h"), &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	if mgr.metrics.Spawns.Load() != 1 || mgr.metrics.Fallbacks.Load() != 0 {
		t.Fatalf("spawns=%d fallbacks=%d", mgr.metrics.Spawns.Load(), mgr.metrics.Fallbacks.Load())
	}
}

func TestWarmPool_BadHelloFallsBackAndKills(t *testing.T) {
	cases := map[string]func(p *fakeWorkerProcess){
		"wrong version": func(p *fakeWorkerProcess) {
			_, _ = frame.Read(p.stdinR)
			_ = frame.Write(p.stdoutW, frame.Hello, 0, []byte{frame.Version + 1})
			_, _ = io.Copy(io.Discard, p.stdinR)
		},
		"not a hello": func(p *fakeWorkerProcess) {
			_, _ = frame.Read(p.stdinR)
			_ = frame.WritePong(p.stdoutW)
			_, _ = io.Copy(io.Discard, p.stdinR)
		},
		"silent exit": func(p *fakeWorkerProcess) { _, _ = frame.Read(p.stdinR); _ = p.stdoutW.Close() },
	}
	for name, serve := range cases {
		t.Run(name, func(t *testing.T) {
			fs := &fakeSet{serve: serve}
			mgr, exec := newTestPool(t, fs.launcher())
			_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-bad"), &workerTestSink{})
			if !errors.Is(err, errWarmUnavailable) {
				t.Fatalf("expected fallback, got %v", err)
			}
			mgr.mu.Lock()
			n := len(mgr.workers)
			mgr.mu.Unlock()
			if n != 0 || !fs.procs[0].killed.Load() || mgr.metrics.Spawns.Load() != 0 {
				t.Fatalf("workers=%d killed=%v spawns=%d", n, fs.procs[0].killed.Load(), mgr.metrics.Spawns.Load())
			}
		})
	}
}

func TestWarmPool_HelloTimeoutFallsBack(t *testing.T) {
	fs := &fakeSet{serve: func(p *fakeWorkerProcess) { _, _ = io.Copy(io.Discard, p.stdinR) }}
	mgr, exec := newTestPool(t, fs.launcher())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	// ctx expires during the handshake: the request is cancelled, not run, and the worker is killed.
	if _, err := mgr.Execute(ctx, exec, OperationProcessConfig{}, opReq("op-silent"), &workerTestSink{}); err == nil {
		t.Fatal("expected an error")
	}
	if !fs.procs[0].killed.Load() {
		t.Fatal("silent worker was not killed")
	}
}

func TestWarmPool_StreamIDsIncreasePerRequest(t *testing.T) {
	fs := &fakeSet{}
	fs.serve = func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type == frame.Request {
				fs.record(f)
				replyOK(p.stdoutW, f.Stream, f.Payload)
			}
		})
	}
	mgr, exec := newTestPool(t, fs.launcher())
	for i := 0; i < 3; i++ {
		if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-s"), &workerTestSink{}); err != nil {
			t.Fatal(err)
		}
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.procs) != 1 || len(fs.frames) != 3 {
		t.Fatalf("procs=%d frames=%d", len(fs.procs), len(fs.frames))
	}
	for i, f := range fs.frames {
		if f.Stream != uint32(i+1) {
			t.Fatalf("request %d used stream %d", i, f.Stream)
		}
	}
}

func TestWarmPool_WrongStreamKillsWorker(t *testing.T) {
	fs := &fakeSet{}
	fs.serve = func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type == frame.Request {
				replyOK(p.stdoutW, f.Stream+7, f.Payload)
			}
		})
	}
	mgr, exec := newTestPool(t, fs.launcher())
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-w"), &workerTestSink{}); err == nil {
		t.Fatal("expected a protocol failure")
	}
	waitFor(t, func() bool { return fs.procs[0].killed.Load() })
	if mgr.metrics.EvictionsErr.Load() != 1 {
		t.Fatalf("evictions=%d", mgr.metrics.EvictionsErr.Load())
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

// cancelServe holds the first request until Cancel arrives, then ends it with
// a terminal Error (or never, when silent).
func cancelServe(silent bool) func(p *fakeWorkerProcess) {
	return func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			switch f.Type {
			case frame.Cancel:
				if !silent {
					_ = frame.WriteError(p.stdoutW, f.Stream, "cancelled")
				}
			case frame.Request:
				var req adapter.ProcessRequest
				_ = json.Unmarshal(f.Payload, &req)
				if req.OperationID != "op-hold" {
					replyOK(p.stdoutW, f.Stream, f.Payload)
				}
			}
		})
	}
}

func TestWarmPool_CancelWithTerminalKeepsWorker(t *testing.T) {
	fs := &fakeSet{serve: cancelServe(false)}
	mgr, exec := newTestPool(t, fs.launcher())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := mgr.Execute(ctx, exec, OperationProcessConfig{}, opReq("op-hold"), &workerTestSink{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller must get its ctx error, got %v", err)
	}
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-next"), &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	if len(fs.procs) != 1 || fs.procs[0].killed.Load() || mgr.metrics.Hits.Load() != 1 {
		t.Fatalf("worker not reused: procs=%d hits=%d", len(fs.procs), mgr.metrics.Hits.Load())
	}
}

func TestWarmPool_CancelWithoutTerminalKillsWorker(t *testing.T) {
	fs := &fakeSet{serve: cancelServe(true)}
	mgr, exec := newTestPool(t, fs.launcher())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := mgr.Execute(ctx, exec, OperationProcessConfig{}, opReq("op-hold"), &workerTestSink{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller must get its ctx error, got %v", err)
	}
	if d := time.Since(start); d < cancelGrace || d > cancelGrace+time.Second {
		t.Fatalf("worker killed after %v, want ~%v", d, cancelGrace)
	}
	waitFor(t, func() bool { return fs.procs[0].killed.Load() })
	if mgr.metrics.EvictionsErr.Load() != 1 {
		t.Fatalf("evictions=%d", mgr.metrics.EvictionsErr.Load())
	}
}

func TestWarmPool_StreamErrorKeepsWorker(t *testing.T) {
	fs := &fakeSet{}
	fs.serve = func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type != frame.Request {
				return
			}
			var req adapter.ProcessRequest
			_ = json.Unmarshal(f.Payload, &req)
			if req.OperationID == "op-err" {
				_ = frame.WriteError(p.stdoutW, f.Stream, "boom")
				return
			}
			replyOK(p.stdoutW, f.Stream, f.Payload)
		})
	}
	mgr, exec := newTestPool(t, fs.launcher())
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-err"), &workerTestSink{}); err == nil {
		t.Fatal("per-stream Error must fail the request")
	}
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-ok"), &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	if len(fs.procs) != 1 || fs.procs[0].killed.Load() || mgr.metrics.EvictionsErr.Load() != 0 {
		t.Fatalf("worker was not kept: procs=%d evictions=%d", len(fs.procs), mgr.metrics.EvictionsErr.Load())
	}
}

// Suppress unused imports
var _ = bytes.NewBuffer

type failingLauncher struct{ calls int }

func (l *failingLauncher) Launch(context.Context, *Executor, OperationProcessConfig, query.Limits) (WorkerProcess, func() error, error) {
	l.calls++
	return nil, nil, errors.New("sandbox refused the worker")
}

// A warm worker that cannot start must hand the operation back to the
// one-shot path instead of rejecting it, and must not leak a pool entry.
func TestWarmPool_LaunchFailureFallsBackToOneShot(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	launcher := &failingLauncher{}
	mgr, err := NewWarmPoolManager(cfg, launcher)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()
	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	req := testProcessRequest("op-1", "tenant-a", "team-a", "conn-1", strings.Repeat("1", 64))
	for i := 0; i < 2; i++ {
		if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, req, nil); !errors.Is(err, errWarmUnavailable) {
			t.Fatalf("launch failure was not reported as unavailable: %v", err)
		}
	}
	mgr.mu.Lock()
	leaked := len(mgr.workers)
	mgr.mu.Unlock()
	if launcher.calls != 2 || leaked != 0 || mgr.metrics.Fallbacks.Load() != 2 {
		t.Fatalf("calls=%d leaked=%d fallbacks=%d", launcher.calls, leaked, mgr.metrics.Fallbacks.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := mgr.Execute(ctx, exec, OperationProcessConfig{}, req, nil); errors.Is(err, errWarmUnavailable) || err == nil {
		t.Fatalf("a cancelled request must not fall back: %v", err)
	}
}

func TestWarmPool_FailedReceiptReturnedAndWorkerKept(t *testing.T) {
	fs := &fakeSet{}
	fs.serve = func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type != frame.Request {
				return
			}
			var req adapter.ProcessRequest
			_ = json.Unmarshal(f.Payload, &req)
			if req.OperationID == "op-failed" {
				raw, _ := json.Marshal(operations.Receipt{Version: operations.Version, OperationID: req.OperationID,
					RequestSHA256: req.RequestSHA256, Outcome: operations.Failed, Effect: operations.EffectNone, ErrorCode: "SOURCE_FAILED"})
				_ = frame.WriteReceipt(p.stdoutW, f.Stream, raw)
				return
			}
			replyOK(p.stdoutW, f.Stream, f.Payload)
		})
	}
	mgr, exec := newTestPool(t, fs.launcher())
	rec, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-failed"), &workerTestSink{})
	if rec.Outcome != operations.Failed || rec.ErrorCode != "SOURCE_FAILED" {
		t.Fatalf("receipt changed: %+v err=%v", rec, err)
	}
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-ok"), &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	if len(fs.procs) != 1 || fs.procs[0].killed.Load() || mgr.metrics.Hits.Load() != 1 {
		t.Fatalf("worker not reused: procs=%d hits=%d", len(fs.procs), mgr.metrics.Hits.Load())
	}
}
