// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package admission

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCapsBoundTenantsAndConnections(t *testing.T) {
	caps := &Caps{MaxPerTenant: 3, MaxPerConnection: 2}
	short := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		t.Cleanup(cancel)
		return ctx
	}
	a1, _ := caps.Acquire(context.Background(), "a", "c1")
	a2, _ := caps.Acquire(context.Background(), "a", "c1")
	if _, err := caps.Acquire(short(), "a", "c1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("connection cap not enforced", err)
	}
	a3, err := caps.Acquire(context.Background(), "a", "c2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := caps.Acquire(short(), "a", "c3"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("tenant cap not enforced", err)
	}
	// Another tenant's connection with the same ID has its own count.
	b1, err := caps.Acquire(short(), "b", "c1")
	if err != nil {
		t.Fatal("tenants shared a connection count", err)
	}
	done := make(chan error, 1)
	go func() {
		release, err := caps.Acquire(context.Background(), "a", "c1")
		if err == nil {
			release()
		}
		done <- err
	}()
	a1()
	a1() // idempotent
	if err := <-done; err != nil {
		t.Fatal("waiter not woken by release", err)
	}
	a2()
	a3()
	b1()
	if len(caps.tenants) != 0 || len(caps.connections) != 0 {
		t.Fatal("counts leaked", caps.tenants, caps.connections)
	}
	var unlimited *Caps
	if release, err := unlimited.Acquire(context.Background(), "a", "c"); err != nil || release == nil {
		t.Fatal("nil caps must admit")
	}
}

// With separate classes, interactive work is held to the reserved share and
// background work to the rest: neither can take the other's slots.
func TestSeparateClassesKeepSlotsApart(t *testing.T) {
	if _, err := New(Limits{MaxConcurrent: 3, MemoryBytes: 300, SeparateClasses: true}); !errors.Is(err, ErrInvalid) {
		t.Fatal("separate classes without a reserved share accepted")
	}
	p, err := New(Limits{MaxConcurrent: 3, MemoryBytes: 300, ReservedSlots: 1, ReservedMemoryBytes: 100, SeparateClasses: true})
	if err != nil {
		t.Fatal(err)
	}
	interactive, err := p.TryAcquire(Request{Class: ClassInteractive, MemoryBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.TryAcquire(Request{Class: ClassInteractive, MemoryBytes: 10}); !errors.Is(err, ErrBusy) {
		t.Fatal("interactive took a background slot", err)
	}
	if _, err := p.TryAcquire(Request{Class: ClassInteractive, MemoryBytes: 150}); !errors.Is(err, ErrOversize) {
		t.Fatal("interactive request larger than its share", err)
	}
	b1, err1 := p.TryAcquire(Request{Class: ClassOperation, MemoryBytes: 100})
	b2, err2 := p.TryAcquire(Request{Class: ClassRefresh, MemoryBytes: 100})
	if err1 != nil || err2 != nil {
		t.Fatal("background share unavailable", err1, err2)
	}
	interactive.Release()
	if _, err := p.TryAcquire(Request{Class: ClassOperation, MemoryBytes: 10}); !errors.Is(err, ErrBusy) {
		t.Fatal("background took the interactive slot", err)
	}
	if again, err := p.TryAcquire(Request{Class: ClassInteractive, MemoryBytes: 100}); err != nil {
		t.Fatal("interactive slot not available under full background", err)
	} else {
		again.Release()
	}
	b1.Release()
	b2.Release()
	if s := p.Snapshot(); s.Active != 0 || s.Classes[ClassOperation].Active != 0 {
		t.Fatal("accounting leaked", s)
	}
}

// A tenant's waiters are served in arrival order: a later caller does not
// overtake an earlier one, while another tenant is not held behind them.
func TestCapsServeEachTenantFIFO(t *testing.T) {
	caps := &Caps{MaxPerTenant: 1}
	held, _ := caps.Acquire(context.Background(), "a", "c1")
	order := make(chan int, 2)
	for i := 1; i <= 2; i++ {
		go func() {
			release, err := caps.Acquire(context.Background(), "a", "c1")
			if err == nil {
				order <- i
				time.Sleep(10 * time.Millisecond)
				release()
			}
		}()
		// Let caller i queue before caller i+1.
		for {
			caps.mu.Lock()
			n := caps.waiting
			caps.mu.Unlock()
			if n == i {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
	other, err := caps.Acquire(context.Background(), "b", "c1")
	if err != nil {
		t.Fatal("another tenant waited behind tenant a", err)
	}
	other()
	held()
	if first, second := <-order, <-order; first != 1 || second != 2 {
		t.Fatal("tenant a's waiters were not served in order", first, second)
	}
}

// Federated reads (no single connection) are bounded by the tenant cap only:
// they do not share one per-connection bucket.
func TestCapsFederatedReadsSkipConnectionCap(t *testing.T) {
	caps := &Caps{MaxPerTenant: 3, MaxPerConnection: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	for i := 0; i < 3; i++ {
		if _, err := caps.Acquire(ctx, "a", ""); err != nil {
			t.Fatal("federated read held to a per-connection cap", i, err)
		}
	}
	if _, err := caps.Acquire(ctx, "a", ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("tenant cap not applied to federated reads", err)
	}
}

// The wait queue is bounded: past MaxWaiting a caller is refused at once.
func TestCapsWaitQueueIsBounded(t *testing.T) {
	caps := &Caps{MaxPerTenant: 1, MaxWaiting: 1}
	held, _ := caps.Acquire(context.Background(), "a", "c")
	defer held()
	go func() { _, _ = caps.Acquire(context.Background(), "a", "c") }()
	for {
		caps.mu.Lock()
		n := caps.waiting
		caps.mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	if _, err := caps.Acquire(context.Background(), "a", "c"); !errors.Is(err, ErrQueueFull) || time.Since(start) > 50*time.Millisecond {
		t.Fatal("full queue did not refuse at once", err)
	}
}

// A release wakes exactly one waiter, its tenant's head, not every waiter.
func TestCapsReleaseWakesOnlyTheHead(t *testing.T) {
	caps := &Caps{MaxPerTenant: 1}
	held, _ := caps.Acquire(context.Background(), "a", "c")
	const waiters = 100
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < waiters; i++ {
		go func() { _, _ = caps.Acquire(ctx, "a", "c") }()
	}
	for {
		caps.mu.Lock()
		n := caps.waiting
		caps.mu.Unlock()
		if n == waiters {
			break
		}
		time.Sleep(time.Millisecond)
	}
	caps.mu.Lock()
	caps.wakeups = 0
	caps.mu.Unlock()
	held()
	for {
		caps.mu.Lock()
		n := caps.waiting
		caps.mu.Unlock()
		if n == waiters-1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	caps.mu.Lock()
	defer caps.mu.Unlock()
	if caps.wakeups != 1 {
		t.Fatal("release woke more than the head", caps.wakeups)
	}
}
