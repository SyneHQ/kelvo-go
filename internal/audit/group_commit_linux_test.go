//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package audit

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// trackedIO wraps the disk operations to prove what a sync covers: it numbers
// every write, remembers the last write number per file offset, and after each
// sync records the highest write number that sync was started after.
type trackedIO struct {
	diskIO
	mu       sync.Mutex
	seq      int64
	byOffset map[int64]int64
	durable  int64
	syncs    atomic.Int64
	syncNS   atomic.Int64 // time spent inside sync
	syncErr  atomic.Bool
	delay    time.Duration
	// skipFsync makes sync a no-op for tests that assert what the writer decides,
	// not durability: a real fsync on a loaded host can stall for seconds.
	skipFsync bool
	// gate, when set, holds the first sync until it is closed; entered is
	// closed when that sync starts.
	gate     chan struct{}
	entered  chan struct{}
	gateOnce sync.Once
}

func newTrackedIO(delay time.Duration) *trackedIO {
	t := &trackedIO{diskIO: defaultIO(), byOffset: map[int64]int64{}, delay: delay}
	t.diskIO.write = func(f *os.File, p []byte, off int64) error {
		t.mu.Lock()
		t.seq++
		t.byOffset[off] = t.seq
		t.mu.Unlock()
		return writeExact(f, p, off)
	}
	t.diskIO.sync = func(f *os.File) error {
		t.mu.Lock()
		covered := t.seq
		t.mu.Unlock()
		t.syncs.Add(1)
		if t.gate != nil {
			t.gateOnce.Do(func() {
				close(t.entered)
				<-t.gate
			})
		}
		began := time.Now()
		defer func() { t.syncNS.Add(int64(time.Since(began))) }()
		if t.delay > 0 {
			time.Sleep(t.delay)
		}
		if t.syncErr.Load() {
			return errors.New("injected sync failure")
		}
		var err error
		if !t.skipFsync {
			err = f.Sync()
		}
		if err == nil {
			t.mu.Lock()
			t.durable = max(t.durable, covered)
			t.mu.Unlock()
		}
		return err
	}
	return t
}

// durableAt reports whether everything written at offset is covered by a sync
// that has returned.
func (t *trackedIO) durableAt(offset int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.byOffset[offset] <= t.durable
}

func groupConfig(t testing.TB, entries, pending int) Config {
	cfg := testConfig(t.TempDir())
	cfg.MaxEntries, cfg.MaxPending = entries, pending
	cfg.WriteTimeout = 30 * time.Second // the longest allowed: a slow host must not read as a lost write
	return cfg
}

func openTracked(t *testing.T, cfg Config, io *trackedIO) *Journal {
	t.Helper()
	j, err := openWithIO(cfg, testScope(), io.diskIO)
	if err != nil {
		t.Fatal(err)
	}
	io.syncs.Store(0) // opening a fresh journal syncs its header
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = j.Close(ctx)
	})
	return j
}

// Operations that arrive together share syncs, and none is acknowledged before
// the sync that covers its writes has returned. This is the one test that uses
// real fsyncs, to prove the records are durable; it asserts correctness only,
// with the longest write timeout, so a slow disk cannot fail it. The other
// grouping tests inject the sync and never touch the disk's speed.
func TestAuditGroupCommitSharesSyncsAndAcknowledgesOnlyDurableOperations(t *testing.T) {
	const workers = 24
	tracked := newTrackedIO(3 * time.Millisecond)
	j := openTracked(t, groupConfig(t, 256, workers), tracked)
	var wg sync.WaitGroup
	var unsafe atomic.Int32
	var ops atomic.Int64
	for range workers {
		wg.Go(func() {
			for range 8 {
				r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
				if err != nil {
					t.Error(err)
					return
				}
				ops.Add(1)
				if !tracked.durableAt(slotOffset(r.slot)) {
					unsafe.Add(1) // acknowledged before its start was durable
				}
				if err := r.Finish(context.Background(), Succeeded, None); err != nil {
					t.Error(err)
					return
				}
				ops.Add(1)
				if !tracked.durableAt(slotOffset(r.slot) + frameSize) {
					unsafe.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if unsafe.Load() != 0 {
		t.Fatalf("%d operations were acknowledged before they were durable", unsafe.Load())
	}
	if syncs := tracked.syncs.Load(); syncs*2 > ops.Load() {
		t.Fatalf("%d syncs for %d operations: not grouped", syncs, ops.Load())
	}
	t.Logf("%d operations, %d syncs (%.2f per operation)", ops.Load(), tracked.syncs.Load(), float64(tracked.syncs.Load())/float64(ops.Load()))
	if err := j.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(allEvents(t, j.cfg.Directory)); got != workers*8 {
		t.Fatalf("%d events recorded, want %d", got, workers*8)
	}
}

func TestAuditGroupCommitSyncFailureFailsTheWholeGroupWithoutReceipts(t *testing.T) {
	tracked := newTrackedIO(5 * time.Millisecond)
	tracked.skipFsync = true // the failure is injected; no real fsync is needed
	j := openTracked(t, groupConfig(t, 64, 16), tracked)
	tracked.syncErr.Store(true)
	var wg sync.WaitGroup
	var receipts, uncertain atomic.Int32
	for range 8 {
		wg.Go(func() {
			r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
			if r != nil {
				receipts.Add(1)
			}
			if errors.Is(err, ErrUncertain) {
				uncertain.Add(1)
			}
		})
	}
	wg.Wait()
	if receipts.Load() != 0 || uncertain.Load() != 8 {
		t.Fatalf("receipts=%d uncertain=%d: a failed sync must fail every operation that wrote", receipts.Load(), uncertain.Load())
	}
	if j.Ready() {
		t.Fatal("journal stayed ready after an uncertain sync")
	}
	if _, err := j.Begin(context.Background(), testBinding(), QuerySubmit); !errors.Is(err, ErrUncertain) {
		t.Fatalf("new work admitted after uncertainty: %v", err)
	}
}

// What the writer decides, not how long it took: a lone operation never waits
// for company, so it never starts a wait on the group window, however loaded the
// machine is. The window wait is counted by the journal for exactly this.
func TestAuditLoneOperationNeverWaitsOnTheGroupWindow(t *testing.T) {
	tracked := newTrackedIO(0)
	tracked.skipFsync = true
	j := openTracked(t, groupConfig(t, 64, 8), tracked)
	for range 10 {
		r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Finish(context.Background(), Succeeded, None); err != nil {
			t.Fatal(err)
		}
	}
	if got := j.windowWaits.Load(); got != 0 {
		t.Fatalf("the writer waited on the group window %d times for lone operations", got)
	}
	if got := tracked.syncs.Load(); got != 20 {
		t.Fatalf("%d syncs for 20 lone operations, want 20", got)
	}
}

// ... and that holds right after a real group. The group is forced, not hoped
// for: the first sync is held while three more operations queue behind it, so
// the writer must see three together next.
func TestAuditLoneOperationAfterAGroupNeverWaitsOnTheGroupWindow(t *testing.T) {
	tracked := newTrackedIO(0)
	tracked.skipFsync = true
	j := openTracked(t, groupConfig(t, 64, 8), tracked)
	// Set after opening: opening a fresh journal syncs its header, and that sync
	// must not be the held one. The writer reads these only after the first
	// operation, which reaches it through a channel from this goroutine.
	tracked.gate, tracked.entered = make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	begin := func() {
		wg.Go(func() {
			r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
			if err != nil {
				t.Error(err)
				return
			}
			if err := r.Finish(context.Background(), Succeeded, None); err != nil {
				t.Error(err)
			}
		})
	}
	begin() // its sync is held
	select {
	case <-tracked.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the first sync never started")
	}
	for range 3 {
		begin()
	}
	for len(j.jobs) < 3 { // all three are queued behind the held sync
		time.Sleep(time.Millisecond)
	}
	close(tracked.gate)
	wg.Wait()
	if got := j.windowWaits.Load(); got < 1 {
		t.Fatalf("no group formed (%d window waits), so this test would prove nothing", got)
	}
	groupWaits := j.windowWaits.Load()
	for range 10 {
		r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Finish(context.Background(), Succeeded, None); err != nil {
			t.Fatal(err)
		}
	}
	if got := j.windowWaits.Load(); got != groupWaits {
		t.Fatalf("lone operations after a group waited on the group window %d times", got-groupWaits)
	}
}

// Close finishes still-active receipts with one sync and reports a failed one.
func TestAuditCloseSyncsInterruptedReceiptsOnce(t *testing.T) {
	tracked := newTrackedIO(0)
	tracked.skipFsync = true
	j, err := openWithIO(groupConfig(t, 64, 8), testScope(), tracked.diskIO)
	if err != nil {
		t.Fatal(err)
	}
	tracked.syncs.Store(0)
	for range 4 {
		if _, err := j.Begin(context.Background(), testBinding(), QueryExecution); err != nil {
			t.Fatal(err)
		}
	}
	before := tracked.syncs.Load()
	if err := j.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := tracked.syncs.Load() - before; got != 1 {
		t.Fatalf("close used %d syncs for 4 interrupted receipts", got)
	}
	events := allEvents(t, j.cfg.Directory)
	if len(events) != 4 {
		t.Fatalf("%d events after close", len(events))
	}
}

// Real fsyncs per operation by arrival pattern. It prints the numbers the
// design asks for; run with -v.
func TestAuditFsyncsPerOperationByLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement")
	}
	run := func(name string, workers int, gap time.Duration, total int, atMost float64) {
		tracked := newTrackedIO(0)
		j := openTracked(t, groupConfig(t, 4096, workers+4), tracked)
		var wg sync.WaitGroup
		var ops atomic.Int64
		per := total / workers
		started := time.Now()
		for range workers {
			wg.Go(func() {
				for range per {
					if gap > 0 {
						time.Sleep(gap)
					}
					r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
					if err != nil {
						t.Error(err)
						return
					}
					if err := r.Finish(context.Background(), Succeeded, None); err != nil {
						t.Error(err)
						return
					}
					ops.Add(2)
				}
			})
		}
		wg.Wait()
		elapsed := time.Since(started)
		t.Logf("%-34s %5d operations in %6.0f ms (%6.0f ops/s): %5d fsyncs = %.2f per operation, %.2f per read of 6 operations",
			name, ops.Load(), float64(elapsed.Milliseconds()), float64(ops.Load())/elapsed.Seconds(), tracked.syncs.Load(),
			float64(tracked.syncs.Load())/float64(ops.Load()), 6*float64(tracked.syncs.Load())/float64(ops.Load()))
		if perOp := float64(tracked.syncs.Load()) / float64(ops.Load()); perOp > atMost {
			t.Errorf("%s: %.2f fsyncs per operation, want at most %.2f", name, perOp, atMost)
		}
	}
	run("1 caller, back to back", 1, 0, 200, 1.0)
	run("8 callers, back to back", 8, 0, 800, 0.5)
	run("50 callers, back to back", 50, 0, 2000, 0.15)
	run("50 callers, one operation pair per 20 ms", 50, 20*time.Millisecond, 500, 0.3)
	run("4 callers, one operation pair per 80 ms", 4, 80*time.Millisecond, 40, 1.0) // about 100 operations a second: little to share
}
