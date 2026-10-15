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
		began := time.Now()
		defer func() { t.syncNS.Add(int64(time.Since(began))) }()
		if t.delay > 0 {
			time.Sleep(t.delay)
		}
		if t.syncErr.Load() {
			return errors.New("injected sync failure")
		}
		err := f.Sync()
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
// the sync that covers its writes has returned.
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

// A burst that forms a group must not make the operation after it wait: a lone
// operation is written and synced at once, whatever came before.
func TestAuditLoneOperationAfterAGroupIsNotDelayed(t *testing.T) {
	// A long window makes waiting for company obvious: a lone operation that
	// waited would take about this long, however loaded the runner is.
	const longWindow = 400 * time.Millisecond
	defer func(previous time.Duration) { groupWindow = previous }(groupWindow)
	groupWindow = longWindow
	tracked := newTrackedIO(2 * time.Millisecond)
	j := openTracked(t, groupConfig(t, 256, 32), tracked)
	var wg sync.WaitGroup
	for range 16 { // a burst: several operations reach the writer together
		wg.Go(func() {
			for range 4 {
				r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
				if err != nil {
					t.Error(err)
					return
				}
				_ = r.Finish(context.Background(), Succeeded, None)
			}
		})
	}
	wg.Wait()
	for range 5 {
		started, syncBefore, groupsBefore := time.Now(), tracked.syncNS.Load(), tracked.syncs.Load()
		r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
		if err != nil {
			t.Fatal(err)
		}
		overhead := time.Since(started) - time.Duration(tracked.syncNS.Load()-syncBefore)
		// Waiting for company would cost the whole window; a loaded runner adds
		// scheduling noise, nowhere near that.
		if tracked.syncs.Load()-groupsBefore != 1 || overhead > longWindow/4 {
			t.Fatalf("a lone begin after a group added %s beyond its sync (%d syncs)", overhead, tracked.syncs.Load()-groupsBefore)
		}
		if err := r.Finish(context.Background(), Succeeded, None); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond) // lone again
	}
}

// Grouping costs a lone operation nothing: it is written and synced at once.
func TestAuditLoneOperationIsNotDelayedByGrouping(t *testing.T) {
	tracked := newTrackedIO(0)
	j := openTracked(t, groupConfig(t, 64, 8), tracked)
	for range 10 {
		started, syncBefore := time.Now(), tracked.syncNS.Load()
		r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Finish(context.Background(), Succeeded, None); err != nil {
			t.Fatal(err)
		}
		// Everything but the filesystem's own sync time is the writer's overhead.
		overhead := time.Since(started) - time.Duration(tracked.syncNS.Load()-syncBefore)
		if overhead > groupCommitWindow*2 {
			t.Fatalf("a lone begin and finish added %s beyond its syncs", overhead)
		}
	}
	if got := tracked.syncs.Load(); got != 20 {
		t.Fatalf("%d syncs for 20 lone operations, want 20", got)
	}
}

// Close finishes still-active receipts with one sync and reports a failed one.
func TestAuditCloseSyncsInterruptedReceiptsOnce(t *testing.T) {
	tracked := newTrackedIO(0)
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
