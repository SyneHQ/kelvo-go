// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCustodyRetainsReservationUntilOperationAndAllGroupsFinish(t *testing.T) {
	var released atomic.Int32
	custody, err := NewCustody(func() { released.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	first, _ := custody.Hold()
	second, _ := custody.Hold()
	custody.Complete()
	first()
	first()
	if released.Load() != 0 || custody.State().Held != 1 {
		t.Fatal("released while another group retained custody")
	}
	if _, err := custody.Hold(); !errors.Is(err, ErrCompleted) {
		t.Fatal("accepted new work after completion")
	}
	second()
	custody.Complete()
	if released.Load() != 1 {
		t.Fatal("reservation did not release exactly once")
	}
}

func TestCustodySourceCompletionDoesNotReleaseRefreshPublicationReservation(t *testing.T) {
	var released atomic.Int32
	custody, _ := NewCustody(func() { released.Add(1) })
	child, _ := custody.Hold()
	child()
	if released.Load() != 0 {
		t.Fatal("source completion released ongoing publication")
	}
	custody.Complete()
	if released.Load() != 1 {
		t.Fatal("completed publication retained empty custody")
	}
}

func TestCustodyConcurrentCompletionAndRelease(t *testing.T) {
	var released atomic.Int32
	custody, _ := NewCustody(func() { released.Add(1) })
	var callbacks []func()
	for range 32 {
		release, _ := custody.Hold()
		callbacks = append(callbacks, release)
	}
	var wg sync.WaitGroup
	for _, callback := range callbacks {
		wg.Add(1)
		go func() { defer wg.Done(); callback(); callback(); custody.Complete() }()
	}
	wg.Wait()
	if state := custody.State(); state.Held != 0 || !state.Completed || !state.Released || released.Load() != 1 {
		t.Fatal("concurrent custody released incorrectly", state)
	}
}

func TestCustodyIsPrivateToEachRequestContext(t *testing.T) {
	first, _ := NewCustody(func() {})
	second, _ := NewCustody(func() {})
	parent := context.Background()
	a, b := WithCustody(parent, first), WithCustody(parent, second)
	if FromContext(parent) != nil || FromContext(a) != first || FromContext(b) != second {
		t.Fatal("request custody crossed contexts")
	}
}
