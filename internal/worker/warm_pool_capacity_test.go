// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapter/frame"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func connReq(id, conn string) adapter.ProcessRequest {
	r := testProcessRequest(id, "tenant-a", "team-a", conn, strings.Repeat("1", 64))
	r.CredentialsValidUntil = time.Now().Unix() + 4
	return r
}

func isBusy(err error) bool {
	var public *query.Error
	return errors.As(err, &public) && public.Code == "RESOURCE_EXHAUSTED"
}

// launchGate blocks the first launch until released and reports how it ends;
// later launches start a fake worker at once.
type launchGate struct {
	calls   atomic.Int32
	started chan struct{}
	release chan error // nil error: the first launch succeeds
	first   atomic.Pointer[fakeWorkerProcess]
}

func newLaunchGate() *launchGate {
	return &launchGate{started: make(chan struct{}), release: make(chan error, 1)}
}

func (g *launchGate) launcher() *mockLauncher {
	return &mockLauncher{onLaunch: func(context.Context) (WorkerProcess, func() error, error) {
		if g.calls.Add(1) == 1 {
			close(g.started)
			if err := <-g.release; err != nil {
				return nil, nil, err
			}
			proc := newFakeWorkerProcess()
			g.first.Store(proc)
			go serveFake(proc, nil)
			return proc, proc.Kill, nil
		}
		proc := newFakeWorkerProcess()
		go serveFake(proc, nil)
		return proc, proc.Kill, nil
	}}
}

// startAbandonedLaunch begins a launch for conn-1 whose only caller leaves,
// leaving an idle launching worker for another key to evict.
func startAbandonedLaunch(t *testing.T, mgr *WarmPoolManager, exec *Executor, g *launchGate) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = mgr.Execute(ctx, exec, OperationProcessConfig{}, connReq("op-a", "conn-1"), &workerTestSink{})
		close(done)
	}()
	<-g.started
	cancel()
	<-done
}

// D1: a worker evicted while it launches ends on every path, and the read that
// evicted it waits for that within its queue_timeout, never longer.
func TestWarmPool_EvictedWhileLaunching(t *testing.T) {
	t.Run("launch succeeds: its process is killed, then the new key launches", func(t *testing.T) {
		g := newLaunchGate()
		mgr, exec := newTestPool(t, g.launcher())
		mgr.cfg.MaxWorkers, mgr.cfg.QueueTimeout = 1, 5*time.Second
		startAbandonedLaunch(t, mgr, exec, g)
		result := make(chan error, 1)
		go func() {
			_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, connReq("op-b", "conn-2"), &workerTestSink{})
			result <- err
		}()
		waitFor(t, func() bool { return mgr.metrics.EvictionsLRU.Load() == 1 })
		if g.calls.Load() != 1 {
			t.Fatal("the new key launched before the evicted worker ended")
		}
		g.release <- nil
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { p := g.first.Load(); return p != nil && p.killed.Load() })
	})
	t.Run("launch fails: the evicting read goes on without waiting for its context", func(t *testing.T) {
		g := newLaunchGate()
		mgr, exec := newTestPool(t, g.launcher())
		mgr.cfg.MaxWorkers, mgr.cfg.QueueTimeout = 1, 5*time.Second
		startAbandonedLaunch(t, mgr, exec, g)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, err := mgr.Execute(ctx, exec, OperationProcessConfig{}, connReq("op-b", "conn-2"), &workerTestSink{})
			result <- err
		}()
		waitFor(t, func() bool { return mgr.metrics.EvictionsLRU.Load() == 1 })
		g.release <- errors.New("sandbox refused the worker")
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("the evicting read waited past the evicted launch's failure")
		}
	})
	t.Run("launch hangs: the evicting read is refused at its queue_timeout", func(t *testing.T) {
		g := newLaunchGate()
		mgr, exec := newTestPool(t, g.launcher())
		mgr.cfg.MaxWorkers, mgr.cfg.QueueTimeout = 1, 200*time.Millisecond
		defer func() { g.release <- errors.New("test over") }()
		startAbandonedLaunch(t, mgr, exec, g)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		start := time.Now()
		_, err := mgr.Execute(ctx, exec, OperationProcessConfig{}, connReq("op-b", "conn-2"), &workerTestSink{})
		if !isBusy(err) || time.Since(start) > 2*time.Second || mgr.metrics.Fallbacks.Load() != 0 {
			t.Fatal("evicting read was not refused at its queue_timeout:", err, time.Since(start), mgr.metrics.Fallbacks.Load())
		}
	})
}

// D2: a node out of capacity does not set off relaunches. Each read launches at
// most twice (a refusal, then one launch that waits for capacity), every read
// is refused at its queue_timeout, and none falls back.
func TestWarmPool_CapacityRefusalDoesNotStorm(t *testing.T) {
	node := newNodeSlots(0)
	launcher := &mockLauncher{onLaunch: func(ctx context.Context) (WorkerProcess, func() error, error) {
		return nil, nil, node.reserve(ctx)
	}}
	mgr, exec := newTestPool(t, launcher)
	mgr.cfg.QueueTimeout = 300 * time.Millisecond
	reads := []adapter.ProcessRequest{connReq("op-1", "conn-1"), connReq("op-2", "conn-1"), connReq("op-3", "conn-1"), connReq("op-4", "conn-2"), connReq("op-5", "conn-3")}
	var wg sync.WaitGroup
	errs := make([]error, len(reads))
	for i, r := range reads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = mgr.Execute(context.Background(), exec, OperationProcessConfig{}, r, &workerTestSink{})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if !isBusy(err) {
			t.Fatalf("read %d: want RESOURCE_EXHAUSTED, got %v", i, err)
		}
	}
	launcher.mu.Lock()
	launches := launcher.spawns
	launcher.mu.Unlock()
	if launches > 4*len(reads) || mgr.metrics.Fallbacks.Load() != 0 || mgr.metrics.LaunchFailures.Load() != 0 {
		t.Fatal("capacity refusals relaunched:", launches, "fallbacks", mgr.metrics.Fallbacks.Load(), "failures", mgr.metrics.LaunchFailures.Load())
	}
}

// D3: a read that joined a launching worker shares its launcher's outcome. A
// capacity refusal keeps it waiting within its own queue_timeout and ends in
// RESOURCE_EXHAUSTED; only a launch that failed sends it to one-shot.
func TestWarmPool_JoinerSharesLaunchOutcome(t *testing.T) {
	t.Run("capacity", func(t *testing.T) {
		node := newNodeSlots(0)
		launcher := &mockLauncher{onLaunch: func(ctx context.Context) (WorkerProcess, func() error, error) {
			return nil, nil, node.reserve(ctx)
		}}
		mgr, exec := newTestPool(t, launcher)
		mgr.cfg.QueueTimeout = 300 * time.Millisecond
		first := make(chan error, 1)
		go func() {
			_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-first"), &workerTestSink{})
			first <- err
		}()
		// The first read's second launch waits for capacity; join it.
		waitFor(t, func() bool { launcher.mu.Lock(); defer launcher.mu.Unlock(); return launcher.spawns == 2 })
		_, joined := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-joiner"), &workerTestSink{})
		if !isBusy(<-first) || !isBusy(joined) || mgr.metrics.Fallbacks.Load() != 0 {
			t.Fatal("a joiner on a capacity refusal fell back:", joined, mgr.metrics.Fallbacks.Load())
		}
	})
	t.Run("failure", func(t *testing.T) {
		g := newLaunchGate()
		mgr, exec := newTestPool(t, g.launcher())
		first := make(chan error, 1)
		go func() {
			_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-first"), &workerTestSink{})
			first <- err
		}()
		<-g.started
		joined := make(chan error, 1)
		go func() {
			_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-joiner"), &workerTestSink{})
			joined <- err
		}()
		waitFor(t, func() bool { return mgr.metrics.Hits.Load() == 1 })
		g.release <- errors.New("sandbox refused the worker")
		if err := <-first; !errors.Is(err, errWarmUnavailable) {
			t.Fatal("launcher:", err)
		}
		if err := <-joined; !errors.Is(err, errWarmUnavailable) {
			t.Fatal("a joiner on a failed launch did not fall back:", err)
		}
	})
}

// Reads waiting for a full worker get its slots in the order they queued, one
// freed slot per waiter.
func TestWarmPool_WaitersServedInOrder(t *testing.T) {
	var mu sync.Mutex
	var order []string
	holds := map[string]chan struct{}{}
	hold := func(id string) chan struct{} {
		mu.Lock()
		defer mu.Unlock()
		if holds[id] == nil {
			holds[id] = make(chan struct{})
		}
		return holds[id]
	}
	fs := &fakeSet{serve: func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type != frame.Request {
				return
			}
			var r adapter.ProcessRequest
			_ = json.Unmarshal(f.Payload, &r)
			mu.Lock()
			order = append(order, r.OperationID)
			mu.Unlock()
			go func() { <-hold(r.OperationID); replyOK(p.stdoutW, f.Stream, f.Payload) }()
		})
	}}
	mgr, exec := newTestPool(t, fs.launcher())
	mgr.cfg.MaxInFlight, mgr.cfg.MaxWorkers, mgr.cfg.QueueTimeout = 1, 1, 5*time.Second
	ids := []string{"op-busy", "op-w1", "op-w2", "op-w3"}
	results := make(chan error, len(ids))
	for i, id := range ids {
		go func() {
			_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq(id), &workerTestSink{})
			results <- err
		}()
		if i == 0 {
			waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 1 })
		} else {
			waitFor(t, func() bool { mgr.mu.Lock(); defer mgr.mu.Unlock(); return len(mgr.waiters) == i })
		}
	}
	for i, id := range ids {
		close(hold(id))
		if err := <-results; err != nil {
			t.Fatal(err)
		}
		if i+1 < len(ids) {
			waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == i+2 })
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(order, ",") != strings.Join(ids, ",") {
		t.Fatal("waiters served out of order:", order)
	}
}
