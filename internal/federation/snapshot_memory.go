// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

type snapshotMemoryExceeded struct{}

// This bounds logical decoder-owned Arrow buffers across snapshot scans in one
// query. Guard masks/results, allocator padding and other Go/native memory are
// outside this counter; process containment remains required.
type snapshotAllocator struct {
	mu          sync.Mutex
	used, limit int64
}

func (a *snapshotAllocator) reserve(size int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if size < 0 || int64(size) > a.limit-a.used {
		panic(snapshotMemoryExceeded{})
	}
	a.used += int64(size)
}
func (a *snapshotAllocator) release(size int) {
	a.mu.Lock()
	a.used -= int64(size)
	a.mu.Unlock()
}
func (a *snapshotAllocator) Allocate(size int) []byte {
	a.reserve(size)
	return memory.DefaultAllocator.Allocate(size)
}
func (a *snapshotAllocator) Reallocate(size int, old []byte) []byte {
	// Charge both buffers during a grow/copy instead of hiding the transient.
	next := a.Allocate(size)
	copy(next, old)
	a.Free(old)
	return next
}
func (a *snapshotAllocator) Free(buffer []byte) {
	a.release(len(buffer))
	memory.DefaultAllocator.Free(buffer)
}
