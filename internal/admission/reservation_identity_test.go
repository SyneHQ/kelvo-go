// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package admission

import (
	"context"
	"sync"
	"testing"
)

func TestReservationIdentityRejectsCopiesForeignAndReleasedHandles(t *testing.T) {
	pool, err := New(Limits{MaxConcurrent: 1, MemoryBytes: 128, ScratchBytes: 0})
	if err != nil {
		t.Fatal(err)
	}
	foreign, _ := New(Limits{MaxConcurrent: 1, MemoryBytes: 128})
	r, err := pool.Acquire(context.Background(), Request{MemoryBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	copy := *r
	if !r.Owns(pool, 128) || r.Owns(pool, 127) || r.Owns(foreign, 128) || copy.Owns(pool, 128) || (*Reservation)(nil).Owns(pool, 128) {
		t.Fatal("reservation proof accepted an incorrect cost or identity")
	}
	var callers sync.WaitGroup
	for range 16 {
		callers.Add(1)
		go func() { defer callers.Done(); copy.Release(); r.Release() }()
	}
	callers.Wait()
	if r.Owns(pool, 128) || pool.Snapshot().Active != 0 || pool.Snapshot().Used.MemoryBytes != 0 {
		t.Fatal("copied handles duplicated release or retained false ownership")
	}
	replacement, err := pool.Acquire(context.Background(), Request{MemoryBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Release()
	if r.Owns(pool, 128) || !replacement.Owns(pool, 128) {
		t.Fatal("a later reservation revived the previous identity")
	}
}
