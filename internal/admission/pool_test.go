// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package admission

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func newPool(t *testing.T, l Limits) *Pool {
	t.Helper()
	p, err := New(l)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func awaitWaiting(t *testing.T, p *Pool, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Snapshot().Waiting == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waiting count: got %d want %d", p.Snapshot().Waiting, n)
}

func TestLimitsAndOversize(t *testing.T) {
	for _, l := range []Limits{{}, {MaxConcurrent: 1, MemoryBytes: -1}, {MaxConcurrent: 1, MemoryBytes: 1, ScratchBytes: -1}} {
		if _, err := New(l); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid limits: %v", err)
		}
	}
	p := newPool(t, Limits{MaxConcurrent: 1, MemoryBytes: 100, ScratchBytes: 50})
	for _, r := range []Request{{MemoryBytes: 101}, {MemoryBytes: 1, ScratchBytes: 51}} {
		if _, err := p.Acquire(context.Background(), r); !errors.Is(err, ErrOversize) {
			t.Fatalf("oversize: %v", err)
		}
	}
	for _, r := range []Request{{}, {MemoryBytes: -1}, {MemoryBytes: 1, ScratchBytes: -1}} {
		if _, err := p.TryAcquire(r); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid: %v", err)
		}
	}
}

func TestQueryAndRefreshShareMemoryAndScratch(t *testing.T) {
	p := newPool(t, Limits{MaxConcurrent: 3, MemoryBytes: 100, ScratchBytes: 80})
	query, err := p.TryAcquire(Request{MemoryBytes: 60, ScratchBytes: 20})
	if err != nil {
		t.Fatal(err)
	}
	defer query.Release()
	if _, err = p.TryAcquire(Request{MemoryBytes: 41}); !errors.Is(err, ErrBusy) {
		t.Fatalf("memory: %v", err)
	}
	if _, err = p.TryAcquire(Request{MemoryBytes: 1, ScratchBytes: 61}); !errors.Is(err, ErrBusy) {
		t.Fatalf("scratch: %v", err)
	}
	refresh, err := p.TryAcquire(Request{MemoryBytes: 40, ScratchBytes: 60})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() { defer wg.Done(); refresh.Release() }()
	}
	wg.Wait()
	if s := p.Snapshot(); s.Active != 1 || s.Used != (Request{MemoryBytes: 60, ScratchBytes: 20}) {
		t.Fatalf("release accounting: %+v", s)
	}
	query.Release()
	if s := p.Snapshot(); s.Active != 0 || s.Used != (Request{}) {
		t.Fatalf("leaked: %+v", s)
	}
}

func TestConcurrentSlots(t *testing.T) {
	p := newPool(t, Limits{MaxConcurrent: 1, MemoryBytes: 100})
	r, err := p.TryAcquire(Request{MemoryBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.TryAcquire(Request{MemoryBytes: 1}); !errors.Is(err, ErrBusy) {
		t.Fatalf("slot: %v", err)
	}
	r.Release()
}

func TestCanceledWaitDoesNotLeak(t *testing.T) {
	p := newPool(t, Limits{MaxConcurrent: 1, MemoryBytes: 10})
	r, _ := p.TryAcquire(Request{MemoryBytes: 10})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := p.Acquire(ctx, Request{MemoryBytes: 1}); result <- err }()
	awaitWaiting(t, p, 1)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s := p.Snapshot(); s.Waiting != 0 || s.Active != 1 {
		t.Fatalf("cancellation accounting: %+v", s)
	}
	r.Release()
	if _, err := p.Acquire(ctx, Request{MemoryBytes: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("already canceled: %v", err)
	}
	if p.Snapshot().Active != 0 {
		t.Fatal("canceled context acquired resources")
	}
}

func TestReleaseWakesWaiters(t *testing.T) {
	p := newPool(t, Limits{MaxConcurrent: 1, MemoryBytes: 10})
	r, _ := p.TryAcquire(Request{MemoryBytes: 10})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		next, err := p.Acquire(ctx, Request{MemoryBytes: 5})
		if next != nil {
			next.Release()
		}
		result <- err
	}()
	awaitWaiting(t, p, 1)
	r.Release()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if s := p.Snapshot(); s.Active != 0 || s.Waiting != 0 {
		t.Fatalf("leaked: %+v", s)
	}
}

func TestDrainRejectsQueuedWorkAndWaitsForCleanup(t *testing.T) {
	p := newPool(t, Limits{MaxConcurrent: 1, MemoryBytes: 10})
	r, _ := p.TryAcquire(Request{MemoryBytes: 10})
	result := make(chan error, 1)
	go func() { _, err := p.Acquire(context.Background(), Request{MemoryBytes: 1}); result <- err }()
	awaitWaiting(t, p, 1)
	p.Drain()
	p.Drain()
	if err := <-result; !errors.Is(err, ErrDraining) {
		t.Fatal(err)
	}
	if _, err := p.TryAcquire(Request{MemoryBytes: 1}); !errors.Is(err, ErrDraining) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p.Snapshot().Active != 1 {
		t.Fatal("drain freed active execution early")
	}
	r.Release()
	if err := p.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !p.Snapshot().Draining {
		t.Fatal("drain reversed")
	}
}

func TestCapacityArithmeticDoesNotOverflow(t *testing.T) {
	p := newPool(t, Limits{MaxConcurrent: 2, MemoryBytes: math.MaxInt64, ScratchBytes: math.MaxInt64})
	r, err := p.TryAcquire(Request{MemoryBytes: math.MaxInt64 - 1, ScratchBytes: math.MaxInt64 - 1})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	if _, err = p.TryAcquire(Request{MemoryBytes: 2, ScratchBytes: 2}); !errors.Is(err, ErrBusy) {
		t.Fatalf("overflow permitted reservation: %v", err)
	}
}

func TestConcurrentMixedWorkNeverExceedsCapacity(t *testing.T) {
	p := newPool(t, Limits{MaxConcurrent: 4, MemoryBytes: 100, ScratchBytes: 100})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := p.Acquire(ctx, Request{MemoryBytes: int64(20 + i%3*10), ScratchBytes: 25})
			if err != nil {
				t.Error(err)
				return
			}
			defer r.Release()
			s := p.Snapshot()
			if s.Active > 4 || s.Used.MemoryBytes > 100 || s.Used.ScratchBytes > 100 {
				t.Errorf("exceeded capacity: %+v", s)
			}
		}()
	}
	wg.Wait()
	if s := p.Snapshot(); s.Active != 0 || s.Waiting != 0 || s.Used != (Request{}) {
		t.Fatalf("leaked: %+v", s)
	}
}
