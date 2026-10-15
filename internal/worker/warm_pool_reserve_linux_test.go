//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
)

// A worker's node reservation: short of capacity it is refused at once, or,
// when the pool lets the launch wait, after waiting no longer than the pool's
// deadline. Running out of capacity is errWarmUnavailable (capacity), never a
// raw context error the pool would take for a failed launch.
func TestReserveWorkerClassifiesCapacity(t *testing.T) {
	newPool := func(t *testing.T) (*admission.Pool, *admission.Reservation) {
		t.Helper()
		pool, err := admission.New(admission.Limits{MaxConcurrent: 3, MemoryBytes: 100, ScratchBytes: 100})
		if err != nil {
			t.Fatal(err)
		}
		// Two slots taken: a worker may not take the node's last one.
		held, err := pool.Acquire(context.Background(), admission.Request{MemoryBytes: 10})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Acquire(context.Background(), admission.Request{MemoryBytes: 10}); err != nil {
			t.Fatal(err)
		}
		return pool, held
	}
	worker := admission.Request{MemoryBytes: 10}
	launchCtx, cancel := context.WithTimeout(context.Background(), time.Minute) // the pool's launchTimeout
	defer cancel()

	t.Run("refused at once without a wait", func(t *testing.T) {
		pool, _ := newPool(t)
		start := time.Now()
		if _, err := reserveWorker(launchCtx, pool, worker); !errors.Is(err, errWarmUnavailable) || time.Since(start) > time.Second {
			t.Fatal(err, time.Since(start))
		}
	})
	t.Run("a wait ends at the pool's deadline as capacity", func(t *testing.T) {
		pool, _ := newPool(t)
		start := time.Now()
		_, err := reserveWorker(withCapacityWait(launchCtx, time.Now().Add(150*time.Millisecond)), pool, worker)
		if !errors.Is(err, errWarmUnavailable) || time.Since(start) < 150*time.Millisecond || time.Since(start) > 5*time.Second {
			t.Fatal(err, time.Since(start))
		}
	})
	t.Run("a wait for memory ends at the pool's deadline as capacity", func(t *testing.T) {
		pool, err := admission.New(admission.Limits{MaxConcurrent: 10, MemoryBytes: 100, ScratchBytes: 100})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Acquire(context.Background(), admission.Request{MemoryBytes: 95}); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		_, err = reserveWorker(withCapacityWait(launchCtx, time.Now().Add(150*time.Millisecond)), pool, worker)
		if !errors.Is(err, errWarmUnavailable) || time.Since(start) < 150*time.Millisecond || time.Since(start) > 5*time.Second {
			t.Fatal(err, time.Since(start))
		}
	})
	t.Run("the launch's own deadline ending a wait is capacity", func(t *testing.T) {
		short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		pool, _ := newPool(t) // short of a slot
		if _, err := reserveWorker(withCapacityWait(short, time.Now().Add(5*time.Second)), pool, worker); !errors.Is(err, errWarmUnavailable) {
			t.Fatal("slot wait:", err)
		}
		memory, err := admission.New(admission.Limits{MaxConcurrent: 10, MemoryBytes: 100, ScratchBytes: 100})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := memory.Acquire(context.Background(), admission.Request{MemoryBytes: 95}); err != nil {
			t.Fatal(err)
		}
		short2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel2()
		if _, err := reserveWorker(withCapacityWait(short2, time.Now().Add(5*time.Second)), memory, worker); !errors.Is(err, errWarmUnavailable) {
			t.Fatal("memory wait:", err)
		}
	})
	t.Run("a wait takes capacity released in time", func(t *testing.T) {
		pool, held := newPool(t)
		go func() { time.Sleep(50 * time.Millisecond); held.Release() }()
		reservation, err := reserveWorker(withCapacityWait(launchCtx, time.Now().Add(5*time.Second)), pool, worker)
		if err != nil {
			t.Fatal(err)
		}
		reservation.Release()
	})
	t.Run("a worker larger than the node is a failure", func(t *testing.T) {
		pool, held := newPool(t)
		held.Release()
		if _, err := reserveWorker(launchCtx, pool, admission.Request{MemoryBytes: 1000}); err == nil || errors.Is(err, errWarmUnavailable) {
			t.Fatal(err)
		}
	})
}
