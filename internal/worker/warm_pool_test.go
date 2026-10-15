// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

func (p *fakeWorkerProcess) Stdin() io.Writer  { return p.stdinW }
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
	cfg.MaxInFlight = 1
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

func TestWarmPool_ConcurrentSameKeyCappedByMaxInFlight(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	cfg.MaxWorkers = 4
	cfg.MaxPerKey = 1   // Only 1 worker per connection key...
	cfg.MaxInFlight = 2 // ...admitting two requests at once
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

	if maxObservedConcurrent.Load() != 2 || launcher.spawns != 1 {
		t.Fatalf("expected one worker admitting exactly max_in_flight=2 at once, observed %d on %d workers", maxObservedConcurrent.Load(), launcher.spawns)
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
	old := helloTimeout
	helloTimeout = 200 * time.Millisecond
	defer func() { helloTimeout = old }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	// The caller stops waiting; the pool's launch times out its handshake and
	// kills the silent worker.
	if _, err := mgr.Execute(ctx, exec, OperationProcessConfig{}, opReq("op-silent"), &workerTestSink{}); err == nil {
		t.Fatal("expected an error")
	}
	waitFor(t, func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return len(fs.procs) == 1 && fs.procs[0].killed.Load()
	})
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

// cancelServe holds the "op-hold" request until Cancel arrives, then ends it
// per mode: "receipt" (non-Completed Receipt), "error" (terminal Error) or
// "silent" (nothing).
func cancelServe(mode string) func(p *fakeWorkerProcess) {
	return func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			switch f.Type {
			case frame.Cancel:
				switch mode {
				case "error":
					_ = frame.WriteError(p.stdoutW, f.Stream, "cancelled")
				case "receipt":
					raw, _ := json.Marshal(operations.Receipt{Version: operations.Version, OperationID: "op-hold",
						RequestSHA256: strings.Repeat("a", 64), Outcome: operations.Failed, Effect: operations.EffectNone, ErrorCode: "CANCELLED"})
					_ = frame.WriteReceipt(p.stdoutW, f.Stream, raw)
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
	fs := &fakeSet{serve: cancelServe("receipt")}
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
	fs := &fakeSet{serve: cancelServe("silent")}
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

func TestWarmPool_StreamErrorKillsWorker(t *testing.T) {
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
	waitFor(t, func() bool { return fs.procs[0].killed.Load() })
	if len(fs.procs) != 2 || mgr.metrics.EvictionsErr.Load() != 1 {
		t.Fatalf("errored worker was reused: procs=%d evictions=%d", len(fs.procs), mgr.metrics.EvictionsErr.Load())
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
	// The second read is inside the failure's backoff window: no relaunch.
	if launcher.calls != 1 || leaked != 0 || mgr.metrics.Fallbacks.Load() != 2 {
		t.Fatalf("calls=%d leaked=%d fallbacks=%d", launcher.calls, leaked, mgr.metrics.Fallbacks.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := mgr.Execute(ctx, exec, OperationProcessConfig{}, req, nil); errors.Is(err, errWarmUnavailable) || err == nil {
		t.Fatalf("a cancelled request must not fall back: %v", err)
	}
}

// A failed launch opens a per-key window (base, doubling to the max, cleared
// by a success) in which reads fall back without launching; other keys still
// launch. One launch per window.
func TestWarmPool_LaunchFailureBacksOffPerKey(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	fs := &fakeSet{serve: func(p *fakeWorkerProcess) { serveFake(p, nil) }}
	ok := fs.launcher()
	l := &mockLauncher{onLaunch: func(ctx context.Context) (WorkerProcess, func() error, error) {
		if fail.Load() {
			return nil, nil, errors.New("sandbox refused the worker")
		}
		return ok.onLaunch(ctx)
	}}
	mgr, exec := newTestPool(t, l)
	req := opReq("op-backoff")
	read := func() error {
		_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, req, &workerTestSink{})
		return err
	}
	spawns := func() int { l.mu.Lock(); defer l.mu.Unlock(); return l.spawns }
	// The failing key is the only one backing off: end its window now.
	expire := func() time.Duration {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		if len(mgr.backoff) != 1 {
			t.Fatalf("backing off %d keys, want 1", len(mgr.backoff))
		}
		for k, b := range mgr.backoff {
			b.until = time.Now().Add(-time.Millisecond)
			mgr.backoff[k] = b
			return b.window
		}
		return 0
	}

	for i := 0; i < 5; i++ {
		if err := read(); !errors.Is(err, errWarmUnavailable) {
			t.Fatalf("read %d: want fallback, got %v", i, err)
		}
	}
	if spawns() != 1 || mgr.metrics.LaunchFailures.Load() != 1 || mgr.metrics.Fallbacks.Load() != 5 {
		t.Fatalf("first window: spawns=%d failures=%d fallbacks=%d", spawns(), mgr.metrics.LaunchFailures.Load(), mgr.metrics.Fallbacks.Load())
	}
	// Each window that ends allows one launch; another failure doubles it.
	want := launchBackoffBase
	for w := 2; w <= 6; w++ {
		if got := expire(); got != want {
			t.Fatalf("window %d: backoff %v, want %v", w, got, want)
		}
		for i := 0; i < 3; i++ {
			_ = read()
		}
		if spawns() != w {
			t.Fatalf("window %d: spawns=%d", w, spawns())
		}
		want = min(2*want, launchBackoffMax)
	}
	if got := expire(); got != launchBackoffMax {
		t.Fatalf("backoff not capped: %v", got)
	}

	// Another connection is not held back by this one's failures.
	fail.Store(false)
	other := testProcessRequest("op-other", "tenant-a", "team-a", "conn-other", strings.Repeat("1", 64))
	other.CredentialsValidUntil = req.CredentialsValidUntil
	before := spawns()
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, other, &workerTestSink{}); err != nil || spawns() != before+1 {
		t.Fatalf("other key did not launch: err=%v spawns=%d", err, spawns())
	}

	// A success clears the window.
	if err := read(); err != nil {
		t.Fatalf("read after window: %v", err)
	}
	mgr.mu.Lock()
	still := len(mgr.backoff)
	mgr.mu.Unlock()
	if still != 0 {
		t.Fatal("success did not clear the backoff")
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

// A terminal Error during Cancel means the adapter is dying: kill, do not reuse.
func TestWarmPool_CancelWithErrorTerminalKillsWorker(t *testing.T) {
	fs := &fakeSet{serve: cancelServe("error")}
	mgr, exec := newTestPool(t, fs.launcher())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := mgr.Execute(ctx, exec, OperationProcessConfig{}, opReq("op-hold"), &workerTestSink{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	waitFor(t, func() bool { return fs.procs[0].killed.Load() })
	if mgr.metrics.EvictionsErr.Load() != 1 {
		t.Fatalf("evictions=%d", mgr.metrics.EvictionsErr.Load())
	}
}

// A Receipt landing right at the grace deadline races the AfterFunc kill. The
// outcome is inherently timing-dependent, so this asserts the invariant for
// either winner (never "killed yet returned to the pool") over many rounds
// instead of forcing one interleaving.
func TestWarmPool_GraceFiredNeverKeeps(t *testing.T) {
	old := cancelGrace
	cancelGrace = 20 * time.Millisecond
	grace := cancelGrace // fakes read this copy, not the var the test restores
	defer func() { cancelGrace = old }()
	for i := 0; i < 20; i++ {
		fs := &fakeSet{}
		fs.serve = func(p *fakeWorkerProcess) {
			serveFake(p, func(f frame.Frame) {
				if f.Type != frame.Cancel {
					return
				}
				time.Sleep(grace) // answer right at the deadline
				raw, _ := json.Marshal(operations.Receipt{Version: operations.Version, OperationID: "op-hold",
					RequestSHA256: strings.Repeat("a", 64), Outcome: operations.Failed, Effect: operations.EffectNone, ErrorCode: "CANCELLED"})
				_ = frame.WriteReceipt(p.stdoutW, f.Stream, raw)
			})
		}
		mgr, exec := newTestPool(t, fs.launcher())
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, _ = mgr.Execute(ctx, exec, OperationProcessConfig{}, opReq("op-hold"), &workerTestSink{})
		cancel()
		mgr.mu.Lock()
		kept := 0
		for _, w := range mgr.workers {
			if w.inFlight == 0 {
				kept++
			}
		}
		mgr.mu.Unlock()
		// Either the Receipt won the race cleanly (kept, never killed) or the
		// timer fired (evicted). A kept worker that was killed is the bug.
		if kept == 1 && fs.procs[0].killed.Load() {
			t.Fatal("worker killed by grace timer but returned to the pool")
		}
	}
}

// interleavedServe answers every request from its own goroutine after a short
// delay, with the Result and Receipt frames written separately, so streams'
// frames interleave on stdout. It records the peak number of open streams.
func interleavedServe(peak *atomic.Int32) func(p *fakeWorkerProcess) {
	return func(p *fakeWorkerProcess) {
		var mu sync.Mutex
		var open atomic.Int32
		serveFake(p, func(f frame.Frame) {
			if f.Type != frame.Request {
				return
			}
			n := open.Add(1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			go func() {
				var wire bytes.Buffer
				replyOK(&wire, f.Stream, f.Payload)
				result, _ := frame.Read(&wire)
				receipt, _ := frame.Read(&wire)
				time.Sleep(time.Duration(f.Stream%7) * 3 * time.Millisecond)
				mu.Lock()
				_ = frame.Write(p.stdoutW, result.Type, result.Stream, result.Payload)
				mu.Unlock()
				time.Sleep(time.Millisecond)
				open.Add(-1)
				mu.Lock()
				_ = frame.Write(p.stdoutW, receipt.Type, receipt.Stream, receipt.Payload)
				mu.Unlock()
			}()
		})
	}
}

// Fifty parallel reads for one connection share one worker, never exceed
// max_in_flight streams on it, and all succeed with cross-checked receipts.
func TestWarmPool_ConcurrentReadsMultiplexOneWorker(t *testing.T) {
	var peak atomic.Int32
	fs := &fakeSet{serve: interleavedServe(&peak)}
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	mgr, err := NewWarmPoolManager(cfg, fs.launcher())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()
	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	var wg sync.WaitGroup
	var failures atomic.Int32
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows := 0
			sink := &workerTestSink{write: func(b arrow.RecordBatch) error { rows += int(b.NumRows()); return nil }}
			rec, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-par"), sink)
			if err != nil || rec.Outcome != operations.Completed || rows != 1 {
				failures.Add(1)
				t.Error(rec.Outcome, err, rows)
			}
		}()
	}
	wg.Wait()
	if failures.Load() != 0 || len(fs.procs) != 1 || peak.Load() > int32(cfg.MaxInFlight) || peak.Load() < 2 {
		t.Fatalf("failures=%d procs=%d peak=%d", failures.Load(), len(fs.procs), peak.Load())
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	for _, w := range mgr.workers {
		if w.inFlight != 0 {
			t.Fatal("slots leaked", w.inFlight)
		}
	}
}

// A full worker makes the next read wait queue_timeout, then refuses it with
// a retryable RESOURCE_EXHAUSTED instead of starting a one-shot child.
func TestWarmPool_FullWorkerWaitsThenRefuses(t *testing.T) {
	fs := &fakeSet{serve: cancelServe("receipt")}
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	cfg.MaxInFlight = 1
	cfg.QueueTimeout = 100 * time.Millisecond
	mgr, err := NewWarmPoolManager(cfg, fs.launcher())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()
	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	holdCtx, release := context.WithCancel(context.Background())
	held := make(chan struct{})
	go func() {
		defer close(held)
		_, _ = mgr.Execute(holdCtx, exec, OperationProcessConfig{}, opReq("op-hold"), &workerTestSink{})
	}()
	waitFor(t, func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return len(mgr.workers) == 1 && mgr.metrics.Spawns.Load() == 1
	})
	start := time.Now()
	rec, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-next"), &workerTestSink{})
	var public *query.Error
	if !errors.As(err, &public) || public.Code != "RESOURCE_EXHAUSTED" || !strings.Contains(public.Message, "retry") || rec.ErrorCode != "RESOURCE_EXHAUSTED" {
		t.Fatalf("got %+v %v", rec, err)
	}
	if d := time.Since(start); d < cfg.QueueTimeout {
		t.Fatalf("refused after %v without waiting", d)
	}
	if mgr.metrics.Fallbacks.Load() != 0 {
		t.Fatal("a busy worker must not fall back to one-shot")
	}
	// The held read ends with a Receipt; its slot frees and the next read runs.
	release()
	<-held
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-after"), &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
}

// A frame for an unknown stream kills the worker and fails every read in
// flight on it, not only the one it seemed to answer.
func TestWarmPool_UnknownStreamFailsAllInFlight(t *testing.T) {
	fs := &fakeSet{}
	var requests atomic.Int32
	fs.serve = func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type == frame.Request && requests.Add(1) == 3 {
				_ = frame.WriteError(p.stdoutW, 999, "stray")
			}
		})
	}
	mgr, exec := newTestPool(t, fs.launcher())
	var wg sync.WaitGroup
	var failed atomic.Int32
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-stray"), &workerTestSink{}); err != nil && !errors.Is(err, errWarmUnavailable) {
				failed.Add(1)
			}
		}()
	}
	wg.Wait()
	if failed.Load() != 3 || !fs.procs[0].killed.Load() || mgr.metrics.EvictionsErr.Load() != 1 {
		t.Fatalf("failed=%d killed=%v evictions=%d", failed.Load(), fs.procs[0].killed.Load(), mgr.metrics.EvictionsErr.Load())
	}
}

// replyRows answers like replyOK with rows rows, in frames of chunk bytes.
func replyRows(w io.Writer, mu *sync.Mutex, stream uint32, payload []byte, rows, chunk int) {
	replyBatches(w, mu, stream, payload, []int{rows}, chunk)
}

// replyBatches answers with one Arrow batch per entry of batches.
func replyBatches(w io.Writer, mu *sync.Mutex, stream uint32, payload []byte, batches []int, chunk int) {
	var req adapter.ProcessRequest
	_ = json.Unmarshal(payload, &req)
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	var wire bytes.Buffer
	iw := ipc.NewWriter(&wire, ipc.WithSchema(schema))
	rows := 0
	for _, n := range batches {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		for i := 0; i < n; i++ {
			b.Append(int64(i))
		}
		col := b.NewArray()
		b.Release()
		rec := array.NewRecordBatch(schema, []arrow.Array{col}, int64(n))
		col.Release()
		_ = iw.Write(rec)
		rec.Release()
		rows += n
	}
	_ = iw.Close()
	sum := sha256.Sum256(wire.Bytes())
	raw, _ := json.Marshal(operations.Receipt{Version: operations.Version, OperationID: req.OperationID, RequestSHA256: req.RequestSHA256,
		Outcome: operations.Completed, Effect: operations.EffectNone,
		Result: &operations.ResultRef{ID: req.OperationID, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(wire.Len()), Rows: int64(rows), Format: "arrow_ipc"}})
	data := wire.Bytes()
	for len(data) > 0 {
		n := min(chunk, len(data))
		mu.Lock()
		_ = frame.WriteResult(w, stream, data[:n])
		mu.Unlock()
		data = data[n:]
	}
	mu.Lock()
	_ = frame.WriteReceipt(w, stream, raw)
	mu.Unlock()
}

// A failed sink and a node-side row limit abandon only their own streams:
// the other fourteen of sixteen reads in flight finish on the same process.
func TestWarmPool_LocalFailuresKeepSharedWorker(t *testing.T) {
	fs := &fakeSet{}
	fs.serve = func(p *fakeWorkerProcess) {
		var mu sync.Mutex
		var held []frame.Frame
		serveFake(p, func(f frame.Frame) {
			switch f.Type {
			case frame.Request:
				held = append(held, f)
				switch {
				case len(held) == 16:
					for _, r := range held {
						go replyRows(p.stdoutW, &mu, r.Stream, r.Payload, 3, 64)
					}
				case len(held) > 16:
					go replyRows(p.stdoutW, &mu, f.Stream, f.Payload, 3, 64)
				}
			case frame.Cancel:
				// Already answering; the Completed receipt is the valid end.
			}
		})
	}
	mgr, exec := newTestPool(t, fs.launcher())
	var wg sync.WaitGroup
	var ok, sinkFailed, limited atomic.Int32
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := opReq(fmt.Sprintf("op-%d", i))
			sink := &workerTestSink{}
			switch i {
			case 0:
				sink.write = func(arrow.RecordBatch) error { return errors.New("client gone") }
			case 1:
				req.Limits.MaxRows = 2
			}
			_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, req, sink)
			switch {
			case err == nil:
				ok.Add(1)
			case i == 0:
				sinkFailed.Add(1)
			case i == 1:
				limited.Add(1)
			default:
				t.Error(i, err)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 14 || sinkFailed.Load() != 1 || limited.Load() != 1 {
		t.Fatalf("ok=%d sink=%d limit=%d", ok.Load(), sinkFailed.Load(), limited.Load())
	}
	if len(fs.procs) != 1 || fs.procs[0].killed.Load() || mgr.metrics.EvictionsErr.Load() != 0 {
		t.Fatalf("a local failure killed the shared worker: procs=%d killed=%v evictions=%d", len(fs.procs), fs.procs[0].killed.Load(), mgr.metrics.EvictionsErr.Load())
	}
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-after"), &workerTestSink{}); err != nil || len(fs.procs) != 1 {
		t.Fatal("worker not reusable after local failures", err)
	}
}

// A consumer that stops reading is failed once more than stream_buffer_mb is
// buffered for it, without blocking the demux: another stream on the same
// worker completes meanwhile, and the worker survives.
func TestWarmPool_SlowConsumerFailsOnlyItsStream(t *testing.T) {
	fs := &fakeSet{}
	fs.serve = func(p *fakeWorkerProcess) {
		var mu sync.Mutex
		serveFake(p, func(f frame.Frame) {
			switch f.Type {
			case frame.Request:
				var r adapter.ProcessRequest
				_ = json.Unmarshal(f.Payload, &r)
				batches := []int{1}
				if r.OperationID == "op-slow" {
					// A tiny first batch the sink blocks on, then ~9.6 MB.
					batches = []int{1, 400_000, 400_000, 400_000}
				}
				go replyBatches(p.stdoutW, &mu, f.Stream, f.Payload, batches, 1<<20)
			case frame.Cancel:
			}
		})
	}
	mgr, exec := newTestPool(t, fs.launcher())
	release := make(chan struct{})
	slow := make(chan error, 1)
	go func() {
		req := opReq("op-slow")
		req.Limits.MaxBytes = 64 << 20
		req.Limits.MaxRows = 1_000_000
		blocked := false
		_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, req, &workerTestSink{write: func(arrow.RecordBatch) error {
			if !blocked {
				blocked = true
				<-release
			}
			return nil
		}})
		slow <- err
	}()
	// Wait until the slow stream has fallen more than stream_buffer_mb behind.
	for i := 0; mgr.metrics.SlowConsumers.Load() == 0; i++ {
		if i > 1000 {
			t.Fatal("slow consumer never detected")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-fast"), &workerTestSink{}); err != nil {
		t.Fatal("a slow consumer blocked another stream", err)
	}
	close(release)
	err := <-slow
	if !errors.Is(err, errSlowConsumer) && !(err != nil && strings.Contains(err.Error(), "slow consumer")) {
		t.Fatal("slow consumer not refused as such:", err)
	}
	if fs.procs[0].killed.Load() || len(fs.procs) != 1 {
		t.Fatal("slow consumer killed the shared worker")
	}
}

// The node buffers at most max_in_flight × stream_buffer_mb per worker; that
// must fit the per-job overhead its reservation carries.
func TestWarmPool_StreamBuffersFitOverhead(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	if cfg.StreamBuffers() != 16*8<<20 || cfg.FitsOverhead(128<<20) != nil || cfg.FitsOverhead(128<<20-1) == nil {
		t.Fatal("default 16 streams × 8 MiB must need exactly 128 MiB of overhead")
	}
	cfg.StreamBufferMB = 2
	if cfg.StreamBuffers() != 32<<20 || cfg.FitsOverhead(32<<20) != nil || cfg.FitsOverhead(32<<20-1) == nil {
		t.Fatal("stream_buffer_mb not accounted")
	}
}

// stream_buffer_mb is the slow-consumer cap: a smaller setting fails a
// stream that falls behind by more than it.
func TestWarmPool_StreamBufferSettingIsTheCap(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.StreamBufferMB = 1
	q := &streamQueue{notify: make(chan struct{}, 1), done: make(chan struct{}), capBytes: cfg.streamBuffer()}
	q.push(frame.Frame{Type: frame.Result, Payload: make([]byte, 600<<10)})
	if q.slow {
		t.Fatal("under the cap marked slow")
	}
	q.push(frame.Frame{Type: frame.Result, Payload: make([]byte, 600<<10)})
	if !q.slow {
		t.Fatal("over stream_buffer_mb not marked slow")
	}
}

// The launch belongs to the pool: when the caller that started it cancels,
// another caller sharing the launching worker still gets it, and the launch
// itself never sees the first caller's cancellation.
func TestWarmPool_LaunchSurvivesFirstCallerCancel(t *testing.T) {
	unblock := make(chan struct{})
	launchCtxErr := make(chan error, 1)
	launcher := &mockLauncher{onLaunch: func(ctx context.Context) (WorkerProcess, func() error, error) {
		<-unblock
		launchCtxErr <- ctx.Err()
		proc := newFakeWorkerProcess()
		go serveFake(proc, nil)
		return proc, proc.Kill, nil
	}}
	mgr, exec := newTestPool(t, launcher)
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := mgr.Execute(firstCtx, exec, OperationProcessConfig{}, opReq("op-first"), &workerTestSink{})
		first <- err
	}()
	waitFor(t, func() bool { mgr.mu.Lock(); defer mgr.mu.Unlock(); return len(mgr.workers) == 1 })
	second := make(chan error, 1)
	go func() {
		_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-second"), &workerTestSink{})
		second <- err
	}()
	waitFor(t, func() bool { return mgr.metrics.Hits.Load() == 1 })
	cancelFirst()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal("first caller did not stop waiting", err)
	}
	close(unblock)
	if err := <-launchCtxErr; err != nil {
		t.Fatal("the launch ran under the first caller's context", err)
	}
	if err := <-second; err != nil || mgr.metrics.Spawns.Load() != 1 {
		t.Fatal("a sharer lost the worker when the first caller left", err)
	}
}

// With more connections than workers, a new connection evicts the least
// recently used idle worker instead of falling back to one-shot; when every
// worker is busy it waits and is refused with a retry hint, never one-shot.
func TestWarmPool_IdleLRUEvictionForNewKeys(t *testing.T) {
	fs := &fakeSet{}
	fs.serve = func(p *fakeWorkerProcess) { serveFake(p, nil) }
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	cfg.MaxWorkers = 2
	cfg.QueueTimeout = 150 * time.Millisecond
	mgr, err := NewWarmPoolManager(cfg, fs.launcher())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()
	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	keyed := func(conn string) adapter.ProcessRequest {
		r := testProcessRequest("op-"+conn, "tenant-a", "team-a", conn, strings.Repeat("1", 64))
		r.CredentialsValidUntil = time.Now().Unix() + 4
		return r
	}
	for _, conn := range []string{"c1", "c2", "c3", "c1", "c4"} {
		if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, keyed(conn), &workerTestSink{}); err != nil {
			t.Fatal(conn, err)
		}
	}
	if mgr.metrics.Fallbacks.Load() != 0 || mgr.metrics.EvictionsLRU.Load() != 3 || mgr.metrics.Spawns.Load() != 5 {
		t.Fatalf("fallbacks=%d lru=%d spawns=%d", mgr.metrics.Fallbacks.Load(), mgr.metrics.EvictionsLRU.Load(), mgr.metrics.Spawns.Load())
	}
	// Each eviction took the least recently used: c1 was used again after c3,
	// so c4 replaced c3 and the survivors are c1 and c4.
	mgr.mu.Lock()
	keys := map[string]bool{}
	for _, w := range mgr.workers {
		keys[w.connectionID] = true
	}
	mgr.mu.Unlock()
	if !keys["c1"] || !keys["c4"] || len(keys) != 2 {
		t.Fatal("not the least recently used evicted", keys)
	}
}

func TestWarmPool_AllBusyWaitsThenRefuses(t *testing.T) {
	release := make(chan struct{})
	fs := &fakeSet{serve: cancelServe("receipt")}
	fs.serve = func(p *fakeWorkerProcess) {
		var mu sync.Mutex
		serveFake(p, func(f frame.Frame) {
			if f.Type == frame.Request {
				go func() { <-release; mu.Lock(); replyOK(p.stdoutW, f.Stream, f.Payload); mu.Unlock() }()
			}
		})
	}
	defer close(release)
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	cfg.MaxWorkers = 1
	cfg.QueueTimeout = 150 * time.Millisecond
	mgr, err := NewWarmPoolManager(cfg, fs.launcher())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()
	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	go func() {
		_, _ = mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-busy"), &workerTestSink{})
	}()
	waitFor(t, func() bool { return mgr.metrics.Spawns.Load() == 1 })
	other := testProcessRequest("op-other", "tenant-a", "team-a", "conn-2", strings.Repeat("1", 64))
	other.CredentialsValidUntil = time.Now().Unix() + 4
	start := time.Now()
	_, err = mgr.Execute(context.Background(), exec, OperationProcessConfig{}, other, &workerTestSink{})
	var public *query.Error
	if !errors.As(err, &public) || public.Code != "RESOURCE_EXHAUSTED" || !strings.Contains(public.Message, "retry after") || time.Since(start) < cfg.QueueTimeout {
		t.Fatal("all-busy pool did not wait and refuse:", err)
	}
	if mgr.metrics.Fallbacks.Load() != 0 {
		t.Fatal("all-busy pool fell back to one-shot")
	}
}

// nodeSlots models the node's worker reservations for mock launchers, as
// reserveWorker does: short of capacity a launch is refused at once, or, when
// the pool lets it wait, waits for a release until the pool's deadline.
type nodeSlots struct {
	mu      sync.Mutex
	free    int
	changed chan struct{}
}

func newNodeSlots(free int) *nodeSlots { return &nodeSlots{free: free, changed: make(chan struct{})} }

func (n *nodeSlots) reserve(ctx context.Context) error {
	deadline, wait := capacityWait(ctx)
	for {
		n.mu.Lock()
		if n.free > 0 {
			n.free--
			n.mu.Unlock()
			return nil
		}
		changed := n.changed
		n.mu.Unlock()
		if !wait {
			return errWarmUnavailable
		}
		select {
		case <-changed:
		case <-time.After(time.Until(deadline)):
			return errWarmUnavailable
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (n *nodeSlots) release() {
	n.mu.Lock()
	n.free++
	close(n.changed)
	n.changed = make(chan struct{})
	n.mu.Unlock()
}

// A launch refused for want of a reservation evicts an idle worker of
// another connection and retries, instead of falling back.
func TestWarmPool_LaunchRefusalEvictsIdleWorker(t *testing.T) {
	node := newNodeSlots(1) // the node has room for one worker
	launcher := &mockLauncher{onLaunch: func(ctx context.Context) (WorkerProcess, func() error, error) {
		if err := node.reserve(ctx); err != nil {
			return nil, nil, err
		}
		proc := newFakeWorkerProcess()
		go serveFake(proc, nil)
		return proc, func() error { node.release(); return proc.Kill() }, nil
	}}
	mgr, exec := newTestPool(t, launcher)
	first := opReq("op-first")
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, first, &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	second := testProcessRequest("op-second", "tenant-a", "team-a", "conn-2", strings.Repeat("1", 64))
	second.CredentialsValidUntil = time.Now().Unix() + 4
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, second, &workerTestSink{}); err != nil {
		t.Fatal("refused launch did not evict the idle worker:", err)
	}
	if mgr.metrics.EvictionsLRU.Load() != 1 || mgr.metrics.Fallbacks.Load() != 0 {
		t.Fatal("lru", mgr.metrics.EvictionsLRU.Load(), "fallbacks", mgr.metrics.Fallbacks.Load())
	}
}

// A launch the node refuses for want of a reservation, with every worker
// busy, is capacity, not a failure: the read waits queue_timeout and is
// refused with a retry hint; it never falls back to one-shot. Once a worker
// is idle again, the next read evicts it and launches.
func TestWarmPool_CapacityRefusalWaitsNeverFallsBack(t *testing.T) {
	release := make(chan struct{})
	node := newNodeSlots(1) // the node has room for one worker
	launcher := &mockLauncher{onLaunch: func(ctx context.Context) (WorkerProcess, func() error, error) {
		if err := node.reserve(ctx); err != nil {
			return nil, nil, err
		}
		proc := newFakeWorkerProcess()
		go serveFake(proc, func(f frame.Frame) {
			if f.Type != frame.Request {
				return
			}
			var r adapter.ProcessRequest
			_ = json.Unmarshal(f.Payload, &r)
			if r.OperationID == "op-busy" {
				go func() { <-release; replyOK(proc.stdoutW, f.Stream, f.Payload) }()
				return
			}
			replyOK(proc.stdoutW, f.Stream, f.Payload)
		})
		return proc, func() error { node.release(); return proc.Kill() }, nil
	}}
	mgr, exec := newTestPool(t, launcher)
	mgr.cfg.QueueTimeout = 150 * time.Millisecond
	busy := make(chan error, 1)
	go func() {
		_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-busy"), &workerTestSink{})
		busy <- err
	}()
	waitFor(t, func() bool { return mgr.metrics.Spawns.Load() == 1 })
	other := testProcessRequest("op-other", "tenant-a", "team-a", "conn-2", strings.Repeat("1", 64))
	other.CredentialsValidUntil = time.Now().Unix() + 4
	start := time.Now()
	rec, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, other, &workerTestSink{})
	var public *query.Error
	if !errors.As(err, &public) || public.Code != "RESOURCE_EXHAUSTED" || !strings.Contains(public.Message, "retry after") || rec.ErrorCode != "RESOURCE_EXHAUSTED" || time.Since(start) < mgr.cfg.QueueTimeout {
		t.Fatal("capacity refusal did not wait and refuse:", rec, err)
	}
	if mgr.metrics.Fallbacks.Load() != 0 {
		t.Fatal("capacity refusal fell back to one-shot")
	}
	close(release)
	if err := <-busy; err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, other, &workerTestSink{}); err != nil || mgr.metrics.EvictionsLRU.Load() != 1 || mgr.metrics.Spawns.Load() != 2 {
		t.Fatal("idle worker not replaced", err, mgr.metrics.EvictionsLRU.Load(), mgr.metrics.Spawns.Load())
	}
}
