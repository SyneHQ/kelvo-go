// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCompletionRetainsReservationThroughPublicationAndChildCleanup(t *testing.T) {
	var completion Completion
	ctx := WithCompletion(context.Background(), &completion)
	var released atomic.Int32
	custody, _ := NewCustody(func() { released.Add(1) })
	child, _ := custody.Hold()
	if err := RetainUntilCompletion(ctx, custody); err != nil {
		t.Fatal(err)
	}
	custody.Complete()
	if released.Load() != 0 {
		t.Fatal("worker return released publication capacity")
	}
	completion.Complete()
	if released.Load() != 0 {
		t.Fatal("publication released unresolved child custody")
	}
	child()
	completion.Complete()
	if released.Load() != 1 {
		t.Fatal("reservation did not release exactly once")
	}
}

func TestCompletionCannotAcceptWorkAfterPublication(t *testing.T) {
	var completion Completion
	completion.Complete()
	var released atomic.Int32
	custody, _ := NewCustody(func() { released.Add(1) })
	if err := RetainUntilCompletion(WithCompletion(context.Background(), &completion), custody); !errors.Is(err, ErrCompleted) {
		t.Fatal("completed owner accepted a reservation", err)
	}
	custody.Complete()
	if released.Load() != 1 {
		t.Fatal("rejected reservation leaked")
	}
}

func TestCompletionConcurrentRegistrationAndPublication(t *testing.T) {
	var completion Completion
	ctx := WithCompletion(context.Background(), &completion)
	var released atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			custody, _ := NewCustody(func() { released.Add(1) })
			err := RetainUntilCompletion(ctx, custody)
			if err != nil && !errors.Is(err, ErrCompleted) {
				t.Error(err)
			}
			custody.Complete()
		}()
	}
	completion.Complete()
	wg.Wait()
	completion.Complete()
	if released.Load() != 32 {
		t.Fatal("completion raced a reservation release", released.Load())
	}
}
