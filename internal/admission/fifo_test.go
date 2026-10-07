// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package admission

import (
	"context"
	"errors"
	"testing"
	"time"
)

type acquireResult struct {
	reservation *Reservation
	err         error
}

func queuedAcquire(p *Pool, ctx context.Context, r Request) <-chan acquireResult {
	result := make(chan acquireResult, 1)
	go func() {
		reservation, err := p.Acquire(ctx, r)
		result <- acquireResult{reservation, err}
	}()
	return result
}

func receiveAcquire(t *testing.T, result <-chan acquireResult) acquireResult {
	t.Helper()
	select {
	case r := <-result:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("queued admission did not return")
		return acquireResult{}
	}
}

func TestFIFOStopsSmallRequestsOvertakingLargeWaiter(t *testing.T) {
	p := newPool(t, Limits{MaxConcurrent: 3, MemoryBytes: 100})
	held, _ := p.TryAcquire(Request{MemoryBytes: 60})
	defer held.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	large := queuedAcquire(p, ctx, Request{MemoryBytes: 100})
	awaitWaiting(t, p, 1)
	// Forty bytes are idle, but using them would postpone the older request.
	if r, err := p.TryAcquire(Request{MemoryBytes: 1}); !errors.Is(err, ErrBusy) {
		r.Release()
		t.Fatalf("TryAcquire bypassed the class queue: %v", err)
	}
	small := queuedAcquire(p, ctx, Request{MemoryBytes: 1})
	awaitWaiting(t, p, 2)
	held.Release()
	first := receiveAcquire(t, large)
	if first.err != nil {
		t.Fatal(first.err)
	}
	defer first.reservation.Release()
	if s := p.Snapshot(); s.Active != 1 || s.Waiting != 1 || s.Used.MemoryBytes != 100 {
		t.Fatalf("large request did not acquire first: %+v", s)
	}
	first.reservation.Release()
	last := receiveAcquire(t, small)
	if last.err != nil {
		t.Fatal(last.err)
	}
	last.reservation.Release()
	if s := p.Snapshot(); s.Active != 0 || s.Waiting != 0 {
		t.Fatalf("admission leaked: %+v", s)
	}
}

func TestFIFOCancellationRemovesHeadAndMiddle(t *testing.T) {
	p := newPool(t, Limits{MaxConcurrent: 1, MemoryBytes: 100})
	held, _ := p.TryAcquire(Request{MemoryBytes: 100})
	defer held.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	headCtx, cancelHead := context.WithCancel(ctx)
	defer cancelHead()
	middleCtx, cancelMiddle := context.WithCancel(ctx)
	defer cancelMiddle()
	head := queuedAcquire(p, headCtx, Request{MemoryBytes: 100})
	awaitWaiting(t, p, 1)
	middle := queuedAcquire(p, middleCtx, Request{MemoryBytes: 100})
	awaitWaiting(t, p, 2)
	tail := queuedAcquire(p, ctx, Request{MemoryBytes: 100})
	awaitWaiting(t, p, 3)
	cancelMiddle()
	if result := receiveAcquire(t, middle); !errors.Is(result.err, context.Canceled) {
		t.Fatalf("middle cancellation: %v", result.err)
	}
	cancelHead()
	if result := receiveAcquire(t, head); !errors.Is(result.err, context.Canceled) {
		t.Fatalf("head cancellation: %v", result.err)
	}
	awaitWaiting(t, p, 1)
	held.Release()
	last := receiveAcquire(t, tail)
	if last.err != nil {
		t.Fatal(last.err)
	}
	last.reservation.Release()
	if s := p.Snapshot(); s.Active != 0 || s.Waiting != 0 {
		t.Fatalf("cancellation leaked: %+v", s)
	}
}

func TestFIFODrainWakesAllQueuedClasses(t *testing.T) {
	p := newPool(t, Limits{MaxConcurrent: 2, MemoryBytes: 100, ReservedSlots: 1})
	held, _ := p.TryAcquire(Request{MemoryBytes: 100})
	defer held.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var results []<-chan acquireResult
	for i := range 12 {
		results = append(results, queuedAcquire(p, ctx, Request{MemoryBytes: 10, Background: i%2 == 0}))
	}
	awaitWaiting(t, p, len(results))
	p.Drain()
	for _, pending := range results {
		if result := receiveAcquire(t, pending); !errors.Is(result.err, ErrDraining) {
			t.Fatalf("drain result: %v", result.err)
		}
	}
	if s := p.Snapshot(); s.Waiting != 0 || s.Active != 1 {
		t.Fatalf("drain released active work or retained waiters: %+v", s)
	}
}

func BenchmarkAdmissionContended(b *testing.B) {
	p, err := New(Limits{MaxConcurrent: 1, MemoryBytes: 1024})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetParallelism(16)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r, err := p.Acquire(context.Background(), Request{MemoryBytes: 1024})
			if err != nil {
				b.Error(err)
				return
			}
			r.Release()
		}
	})
	if s := p.Snapshot(); s.Waiting != 0 || s.Active != 0 {
		b.Fatalf("benchmark leaked resources: %+v", s)
	}
}
