// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
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

// A read whose credentials expire while it waits for a slot is not sent with
// stale ones and does not cost the worker: the caller authorizes again.
func TestWarmPool_CredentialsExpiredWhileWaiting(t *testing.T) {
	fs := &fakeSet{}
	fs.serve = func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			fs.record(f)
			if f.Type == frame.Request {
				replyOK(p.stdoutW, f.Stream, f.Payload)
			}
		})
	}
	mgr, exec := newTestPool(t, fs.launcher())
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-warm"), &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	expired := withCredentialBound(context.Background(), time.Now().Unix()-1)
	if _, err := mgr.Execute(expired, exec, OperationProcessConfig{}, opReq("op-stale"), &workerTestSink{}); !errors.Is(err, errCredentialsExpired) {
		t.Fatal("a read with expired credentials was not sent back to authorize:", err)
	}
	fs.mu.Lock()
	requests := 0
	for _, f := range fs.frames {
		if f.Type == frame.Request {
			requests++
		}
	}
	fs.mu.Unlock()
	if requests != 1 || mgr.metrics.EvictionsErr.Load() != 0 || mgr.metrics.Fallbacks.Load() != 0 {
		t.Fatal("stale read reached or cost the worker:", requests, mgr.metrics.EvictionsErr.Load(), mgr.metrics.Fallbacks.Load())
	}
	// The worker still serves.
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-after"), &workerTestSink{}); err != nil || mgr.metrics.Spawns.Load() != 1 {
		t.Fatal(err, mgr.metrics.Spawns.Load())
	}
}

// A key at max_in_flight whose scale-out launch the node refuses waits for a
// slot on its own worker, which frees in milliseconds; it does not wait out
// its queue_timeout on node capacity.
func TestWarmPool_RefusedScaleOutWaitsForOwnWorker(t *testing.T) {
	node := newNodeSlots(1)
	release := make(chan struct{})
	fs := &fakeSet{serve: func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type != frame.Request {
				return
			}
			var r adapter.ProcessRequest
			_ = json.Unmarshal(f.Payload, &r)
			if r.OperationID == "op-hold" {
				go func() { <-release; replyOK(p.stdoutW, f.Stream, f.Payload) }()
				return
			}
			replyOK(p.stdoutW, f.Stream, f.Payload)
		})
	}}
	start := fs.launcher()
	launcher := &mockLauncher{onLaunch: func(ctx context.Context) (WorkerProcess, func() error, error) {
		if err := node.reserve(ctx); err != nil {
			return nil, nil, err
		}
		proc, _, err := start.onLaunch(ctx)
		return proc, func() error { node.release(); return proc.Kill() }, err
	}}
	mgr, exec := newTestPool(t, launcher)
	mgr.cfg.MaxInFlight, mgr.cfg.MaxPerKey, mgr.cfg.QueueTimeout = 1, 2, 3*time.Second
	held := make(chan error, 1)
	go func() {
		_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-hold"), &workerTestSink{})
		held <- err
	}()
	waitFor(t, func() bool { return mgr.metrics.Spawns.Load() == 1 })
	go func() { time.Sleep(200 * time.Millisecond); close(release) }()
	began := time.Now()
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq("op-next"), &workerTestSink{}); err != nil || time.Since(began) > 2*time.Second {
		t.Fatal("a refused scale-out did not wait for the key's own worker:", err, time.Since(began))
	}
	if err := <-held; err != nil {
		t.Fatal(err)
	}
}

// holdServe holds a request whose operation ID has a release channel until it
// is closed, and answers every other request at once.
func holdServe(holds map[string]chan struct{}) func(p *fakeWorkerProcess) {
	return func(p *fakeWorkerProcess) {
		var write sync.Mutex
		serveFake(p, func(f frame.Frame) {
			if f.Type != frame.Request {
				return
			}
			var r adapter.ProcessRequest
			_ = json.Unmarshal(f.Payload, &r)
			go func() {
				if hold := holds[r.OperationID]; hold != nil {
					<-hold
				}
				write.Lock()
				defer write.Unlock()
				replyOK(p.stdoutW, f.Stream, f.Payload)
			}()
		})
	}
}

// A freed slot wakes the earliest waiter that can use it, and that waiter uses
// it: the pool is full (conn-1 and conn-3 each have a busy worker), X waits
// for conn-1, then Y for conn-4, and conn-3's worker goes idle. With
// max_per_key 1, X cannot add a worker, so Y is woken and takes the idle
// worker's place. With max_per_key 2, X can: it is woken, evicts the idle
// worker and launches. Either way the idle worker is not left while waiters
// time out.
func TestWarmPool_FreedSlotWakesAWaiterThatCanUseIt(t *testing.T) {
	for _, tc := range []struct {
		maxPerKey int
		served    string
	}{{1, "op-y"}, {2, "op-x"}} {
		t.Run(fmt.Sprintf("max_per_key %d", tc.maxPerKey), func(t *testing.T) {
			holds := map[string]chan struct{}{"op-k1": make(chan struct{}), "op-k3": make(chan struct{})}
			fs := &fakeSet{serve: holdServe(holds)}
			mgr, exec := newTestPool(t, fs.launcher())
			mgr.cfg.MaxWorkers, mgr.cfg.MaxPerKey, mgr.cfg.MaxInFlight, mgr.cfg.QueueTimeout = 2, tc.maxPerKey, 1, 5*time.Second
			results := map[string]chan error{}
			run := func(id, conn string) {
				results[id] = make(chan error, 1)
				go func() {
					_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, connReq(id, conn), &workerTestSink{})
					results[id] <- err
				}()
			}
			run("op-k1", "conn-1")
			run("op-k3", "conn-3")
			waitFor(t, func() bool { return mgr.metrics.Spawns.Load() == 2 })
			run("op-x", "conn-1")
			waitFor(t, func() bool { mgr.mu.Lock(); defer mgr.mu.Unlock(); return len(mgr.waiters) == 1 })
			run("op-y", "conn-4")
			waitFor(t, func() bool { mgr.mu.Lock(); defer mgr.mu.Unlock(); return len(mgr.waiters) == 2 })
			close(holds["op-k3"]) // conn-3's worker goes idle
			if err := <-results["op-k3"]; err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-results[tc.served]:
				// Served only by evicting the idle worker (the pool is full); a
				// later waiter may evict again once this read's worker idles.
				if err != nil || mgr.metrics.EvictionsLRU.Load() < 1 {
					t.Fatal(err, mgr.metrics.EvictionsLRU.Load())
				}
			case <-time.After(2 * time.Second):
				t.Fatal("the idle worker's place went unused while", tc.served, "waited")
			}
			close(holds["op-k1"])
			for id, done := range results {
				if id == tc.served || id == "op-k3" {
					continue
				}
				if err := <-done; err != nil {
					t.Fatal(id, err)
				}
			}
		})
	}
}

// A read already past its queue deadline evicts no warm worker on its way to
// RESOURCE_EXHAUSTED.
func TestWarmPool_OutOfTimeReadEvictsNothing(t *testing.T) {
	fs := &fakeSet{serve: func(p *fakeWorkerProcess) { serveFake(p, nil) }}
	mgr, exec := newTestPool(t, fs.launcher())
	mgr.cfg.MaxWorkers = 1
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, connReq("op-idle", "conn-1"), &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	mgr.cfg.QueueTimeout = 0 // the next read's deadline has passed when it arrives
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, connReq("op-late", "conn-2"), &workerTestSink{}); !isBusy(err) || mgr.metrics.EvictionsLRU.Load() != 0 {
		t.Fatal("a read out of time evicted a warm worker:", err, mgr.metrics.EvictionsLRU.Load())
	}
}

// The credential window is stamped when a request is written, from the read's
// bound: a read stamped long ago (it queued) is sent with a current window.
func TestWarmPool_CredentialWindowStampedAtDispatch(t *testing.T) {
	var sent atomic.Int64
	fs := &fakeSet{}
	fs.serve = func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type != frame.Request {
				return
			}
			var r adapter.ProcessRequest
			_ = json.Unmarshal(f.Payload, &r)
			sent.Store(r.CredentialsValidUntil)
			replyOK(p.stdoutW, f.Stream, f.Payload)
		})
	}
	mgr, exec := newTestPool(t, fs.launcher())
	stale := opReq("op-queued")
	stale.CredentialsValidUntil = time.Now().Unix() - 1 // stamped before a long wait
	ctx := withCredentialBound(context.Background(), time.Now().Unix()+60)
	before := time.Now().Unix()
	if _, err := mgr.Execute(ctx, exec, OperationProcessConfig{}, stale, &workerTestSink{}); err != nil {
		t.Fatal("a read stamped before its wait was not re-stamped at dispatch:", err)
	}
	if got := sent.Load(); got <= before || got > time.Now().Add(credentialWindowLimit()).Unix() {
		t.Fatal("dispatched window", got, "not within (now, now+window]")
	}
}

// A failed launch is logged once per backoff window: launches already in
// flight for the key that fail inside it are counted but not logged again,
// and do not lengthen the window.
func TestWarmPool_LaunchFailureLoggedOncePerWindow(t *testing.T) {
	var logged bytes.Buffer
	log.SetOutput(&logged)
	defer log.SetOutput(os.Stderr)
	var started sync.WaitGroup
	started.Add(2)
	launcher := &mockLauncher{onLaunch: func(context.Context) (WorkerProcess, func() error, error) {
		started.Done()
		started.Wait() // both launches are in flight before either fails
		return nil, nil, errors.New("sandbox refused the worker")
	}}
	mgr, exec := newTestPool(t, launcher)
	mgr.cfg.MaxInFlight, mgr.cfg.MaxPerKey = 1, 2
	var wg sync.WaitGroup
	for _, id := range []string{"op-1", "op-2"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = mgr.Execute(context.Background(), exec, OperationProcessConfig{}, opReq(id), &workerTestSink{})
		}()
	}
	wg.Wait()
	mgr.mu.Lock()
	var window time.Duration
	for _, b := range mgr.backoff {
		window = b.window
	}
	mgr.mu.Unlock()
	if lines := strings.Count(logged.String(), "launch failed"); lines != 1 || mgr.metrics.LaunchFailures.Load() != 2 || window != launchBackoffBase {
		t.Fatal("log lines", lines, "failures", mgr.metrics.LaunchFailures.Load(), "window", window)
	}
}

// A capacity refusal frees nothing a waiter could use, so it wakes nobody;
// a failed launch frees its place and wakes one waiter.
func TestWarmPool_CapacityRefusalWakesNobody(t *testing.T) {
	for _, tc := range []struct {
		err  error
		wake bool
	}{{errWarmUnavailable, false}, {errors.New("sandbox refused the worker"), true}} {
		launcher := &mockLauncher{onLaunch: func(context.Context) (WorkerProcess, func() error, error) { return nil, nil, tc.err }}
		mgr, exec := newTestPool(t, launcher)
		waiter := &slotWaiter{key: "other", ready: make(chan struct{}, 1)}
		entry := &warmWorkerEntry{id: "launching", key: "k", inFlight: 1, ready: make(chan struct{}), dead: make(chan struct{})}
		mgr.mu.Lock()
		mgr.waiters = append(mgr.waiters, waiter)
		mgr.workers[entry.id] = entry
		mgr.mu.Unlock()
		_ = mgr.launch(exec, OperationProcessConfig{}, entry, time.Time{})
		if woken := len(waiter.ready) == 1; woken != tc.wake {
			t.Fatalf("launch error %v: waiter woken = %v, want %v", tc.err, woken, tc.wake)
		}
	}
}
