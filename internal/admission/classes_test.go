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

func TestBackgroundLeavesInteractiveCapacity(t *testing.T) {
	for _, tc := range []struct {
		name        string
		limits      Limits
		request     Request
		interactive Request
	}{
		{"slots", Limits{MaxConcurrent: 3, MemoryBytes: 100, ScratchBytes: 100, ReservedSlots: 1}, Request{MemoryBytes: 1, Background: true}, Request{MemoryBytes: 1}},
		{"memory", Limits{MaxConcurrent: 4, MemoryBytes: 100, ScratchBytes: 100, ReservedMemoryBytes: 40}, Request{MemoryBytes: 30, Background: true}, Request{MemoryBytes: 40}},
		{"scratch", Limits{MaxConcurrent: 4, MemoryBytes: 100, ScratchBytes: 100, ReservedScratchBytes: 40}, Request{MemoryBytes: 1, ScratchBytes: 30, Background: true}, Request{MemoryBytes: 1, ScratchBytes: 40}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(tc.limits)
			if err != nil {
				t.Fatal(err)
			}
			first, err := p.TryAcquire(tc.request)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Release()
			second, err := p.TryAcquire(tc.request)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Release()
			if _, err := p.TryAcquire(tc.request); !errors.Is(err, ErrBusy) {
				t.Fatalf("background consumed protected capacity: %v", err)
			}
			interactive, err := p.TryAcquire(tc.interactive)
			if err != nil {
				t.Fatalf("interactive reserve unavailable: %v", err)
			}
			defer interactive.Release()
			s := p.Snapshot()
			if s.Active != 3 || s.BackgroundActive != 2 || s.BackgroundUsed.MemoryBytes != 2*tc.request.MemoryBytes || s.BackgroundUsed.ScratchBytes != 2*tc.request.ScratchBytes {
				t.Fatalf("incorrect class accounting: %+v", s)
			}
		})
	}
}

func TestInteractiveCanUseAllIdleCapacity(t *testing.T) {
	p, err := New(Limits{MaxConcurrent: 2, MemoryBytes: 100, ScratchBytes: 100, ReservedSlots: 1, ReservedMemoryBytes: 40, ReservedScratchBytes: 40})
	if err != nil {
		t.Fatal(err)
	}
	held, err := p.TryAcquire(Request{MemoryBytes: 100, ScratchBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.TryAcquire(Request{MemoryBytes: 1, Background: true}); !errors.Is(err, ErrBusy) {
		t.Fatal("background bypassed aggregate memory capacity")
	}
	held.Release()
	background, err := p.TryAcquire(Request{MemoryBytes: 60, ScratchBytes: 60, Background: true})
	if err != nil {
		t.Fatal(err)
	}
	defer background.Release()
	if _, err := p.TryAcquire(Request{MemoryBytes: 41}); !errors.Is(err, ErrBusy) {
		t.Fatal("interactive bypassed aggregate memory capacity")
	}
}

func TestProtectedCapacityValidationAndOversize(t *testing.T) {
	for _, limits := range []Limits{
		{MaxConcurrent: 1, MemoryBytes: 10, ReservedSlots: -1},
		{MaxConcurrent: 1, MemoryBytes: 10, ReservedSlots: 1},
		{MaxConcurrent: 1, MemoryBytes: 10, ReservedMemoryBytes: -1},
		{MaxConcurrent: 1, MemoryBytes: 10, ReservedMemoryBytes: 11},
		{MaxConcurrent: 1, MemoryBytes: 10, ReservedScratchBytes: 1},
		{MaxConcurrent: 1, MemoryBytes: 10, ReservedScratchBytes: -1},
	} {
		if _, err := New(limits); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid limits accepted: %+v", limits)
		}
	}
	p, err := New(Limits{MaxConcurrent: 2, MemoryBytes: 100, ScratchBytes: 100, ReservedMemoryBytes: 40, ReservedScratchBytes: 40})
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []Request{{Background: true, MemoryBytes: 61}, {Background: true, MemoryBytes: 1, ScratchBytes: 61}} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := p.Acquire(ctx, request)
		cancel()
		if !errors.Is(err, ErrOversize) || p.Snapshot().Waiting != 0 {
			t.Fatalf("oversize background request queued: %v", err)
		}
	}
	full, err := New(Limits{MaxConcurrent: 1, MemoryBytes: 100, ReservedMemoryBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := full.TryAcquire(Request{Background: true, MemoryBytes: 1}); !errors.Is(err, ErrOversize) {
		t.Fatal("fully reserved memory accepted background job")
	}
}

func TestClassReleaseConcurrentAndIdempotent(t *testing.T) {
	p, _ := New(Limits{MaxConcurrent: 3, MemoryBytes: 100, ScratchBytes: 100, ReservedSlots: 1, ReservedMemoryBytes: 20})
	background, _ := p.TryAcquire(Request{MemoryBytes: 40, ScratchBytes: 10, Background: true})
	interactive, _ := p.TryAcquire(Request{MemoryBytes: 50, ScratchBytes: 20})
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(background.Release)
		wg.Go(interactive.Release)
	}
	wg.Wait()
	s := p.Snapshot()
	if s.Active != 0 || s.BackgroundActive != 0 || s.Used != (Request{}) || s.BackgroundUsed != (Request{}) {
		t.Fatalf("release accounting leaked or underflowed: %+v", s)
	}
}

func TestBackgroundBoundsAvoidOverflow(t *testing.T) {
	p, err := New(Limits{MaxConcurrent: 2, MemoryBytes: math.MaxInt64, ScratchBytes: math.MaxInt64, ReservedMemoryBytes: 1, ReservedScratchBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := p.TryAcquire(Request{Background: true, MemoryBytes: math.MaxInt64 - 1, ScratchBytes: math.MaxInt64 - 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()
	if _, err := p.TryAcquire(Request{Background: true, MemoryBytes: 1}); !errors.Is(err, ErrBusy) {
		t.Fatal("overflow bypassed background budget")
	}
	interactive, err := p.TryAcquire(Request{MemoryBytes: 1, ScratchBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	interactive.Release()
}

func TestBackgroundWaiterDoesNotBlockInteractive(t *testing.T) {
	p, _ := New(Limits{MaxConcurrent: 2, MemoryBytes: 100, ReservedSlots: 1})
	held, err := p.TryAcquire(Request{Background: true, MemoryBytes: 20})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan *Reservation, 1)
	go func() { r, _ := p.Acquire(ctx, Request{Background: true, MemoryBytes: 20}); done <- r }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for p.Snapshot().Waiting != 1 {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("background did not wait")
		}
	}
	interactive, err := p.TryAcquire(Request{MemoryBytes: 20})
	if err != nil {
		t.Fatalf("background waiter blocked interactive reserve: %v", err)
	}
	defer interactive.Release()
	held.Release()
	select {
	case r := <-done:
		if r == nil {
			t.Fatal("background waiter failed after release")
		}
		r.Release()
	case <-ctx.Done():
		t.Fatal("background waiter did not wake")
	}
}
