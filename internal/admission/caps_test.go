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
