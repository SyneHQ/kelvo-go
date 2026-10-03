// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"sync"
)

type scanBudgetKey struct{}
type scanBudget struct {
	active         chan struct{}
	mu             sync.Mutex
	snapshotMemory *snapshotAllocator
}

func (b *scanBudget) snapshotAllocator(limit int64) *snapshotAllocator {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.snapshotMemory == nil {
		b.snapshotMemory = &snapshotAllocator{limit: limit}
	}
	return b.snapshotMemory
}

// WithScanBudget shares a concurrent source-scan limit across Tables made from
// this context. A full budget fails immediately: waiting while a join holds a
// different scan could deadlock. Nonpositive limits permit one active scan.
func WithScanBudget(ctx context.Context, maximum int) context.Context {
	if maximum < 1 {
		maximum = 1
	}
	return context.WithValue(ctx, scanBudgetKey{}, &scanBudget{active: make(chan struct{}, maximum)})
}

func budgetFromContext(ctx context.Context) *scanBudget {
	if shared, ok := ctx.Value(scanBudgetKey{}).(*scanBudget); ok {
		return shared
	}
	return &scanBudget{active: make(chan struct{}, 4)}
}

func (b *scanBudget) acquire() bool {
	select {
	case b.active <- struct{}{}:
		return true
	default:
		return false
	}
}
func (b *scanBudget) release() { <-b.active }
