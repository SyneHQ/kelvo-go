// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

func TestResolutionKeepsPinsThroughRangeProcessAndScratchCleanup(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	guard, pin := ownerTestAcquire(t, owner, context.Background())
	prepared, err := guard.HoldConsumer()
	if err != nil {
		t.Fatal(err)
	}
	prepared()
	rangeToken, err := guard.HoldConsumer()
	if err != nil {
		t.Fatal(err)
	}
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseRange := func() { once.Do(func() { close(unblock) }) }
	t.Cleanup(releaseRange)
	var rangeCloses atomic.Int64
	resolution := &Resolution{guard: guard, ctx: guard.Context(), done: make(chan struct{}), rangeToken: rangeToken,
		release: func() { rangeCloses.Add(1); close(entered); <-unblock }}
	process, err := resolution.HoldConsumer()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(process)
	scratch, err := resolution.HoldConsumer()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scratch)
	for range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		err := resolution.Close(ctx)
		cancel()
		if !errors.Is(err, errReaderCleanupUnknown) {
			t.Fatal("unjoined consumers reported cleanup success", err)
		}
	}
	ownerTestWait(t, entered, "resolution range cleanup")
	if rangeCloses.Load() != 1 || pin.closes.Load() != 0 || pin.renew() != nil {
		t.Fatal("repeated close lost custody or started more cleanup")
	}
	if budget.snapshot() != (readerCounts{owners: 1, guards: 1, pins: 1}) {
		t.Fatal("unjoined resolution returned node capacity")
	}
	releaseRange()
	process()
	ownerTestPending(t, resolution.done, "scratch still owns snapshot data")
	if pin.closes.Load() != 0 {
		t.Fatal("missing scratch proof released pin")
	}
	scratch()
	ownerTestWait(t, resolution.done, "resolution cleanup")
	if pin.closes.Load() != 1 || budget.snapshot() != (readerCounts{owners: 1}) {
		t.Fatal("joined consumers did not release exact custody")
	}
	if err := ownerTestClose(t, resolution.Close); !errors.Is(err, errReaderCleanupUnknown) {
		t.Fatal("late cleanup erased the earlier uncertain result", err)
	}
}

func TestResolutionRetainsPinLossAfterLastResult(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	guard, pin := ownerTestAcquire(t, owner, context.Background())
	resolution := &Resolution{guard: guard, ctx: guard.Context(), done: make(chan struct{})}
	if err := resolution.Check(); err != nil {
		t.Fatal(err)
	}
	// Models loss after a complete Arrow batch but before terminal publication.
	pin.cancel(readerlease.ErrLost)
	if err := ownerTestClose(t, resolution.Close); !errors.Is(err, readerlease.ErrLost) {
		t.Fatal("complete result bytes concealed terminal lease loss", err)
	}
}

func TestResolutionRequiresMatchingProtectedRuntimeBeforeSourceIO(t *testing.T) {
	c := catalog.Config{Acceleration: &catalog.AccelerationConfig{ObjectStorage: &catalog.ObjectStorage{
		ReaderRegistry: &catalog.ObjectReaderRegistry{},
	}}}
	if !ProtectedObjects(c) {
		t.Fatal("protected configuration not recognized")
	}
	if resolution, err := ResolveWithRuntime(context.Background(), c, query.Request{}, nil); err == nil || resolution != nil {
		t.Fatal("protected resolution fell back to an unpinned backend")
	}
	if sources, _, _, err := Resolve(context.Background(), c, query.Request{}); err == nil || sources != nil {
		t.Fatal("legacy resolver accepted protected storage")
	}
}

func TestResolutionPreservesLegacySourcesAndJoinedClose(t *testing.T) {
	c := catalog.Config{Sources: []catalog.Source{{ID: "orders", Type: "csv", Path: "/tmp/orders.csv"}}}
	r, err := ResolveWithRuntime(context.Background(), c, query.Request{Sources: []string{"orders"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Sources) != 1 || r.Sources[0].ID != "orders" || len(r.Versions) != 0 || r.Check() != nil {
		t.Fatal("legacy source selection changed")
	}
	if err := ownerTestClose(t, r.Close); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyResolutionDoesNotDetachUnchargedCleanup(t *testing.T) {
	entered, unblock, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	t.Cleanup(release)
	r := &Resolution{ctx: context.Background(), done: make(chan struct{}), release: func() { close(entered); <-unblock }}
	ctx, cancel := context.WithCancel(context.Background())
	// Close requires a bounded observation even though legacy cleanup keeps the
	// caller's reservation until join, including after that observation expires.
	deadline, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	go func() { _ = r.Close(deadline); close(finished) }()
	ownerTestWait(t, entered, "legacy range cleanup")
	cancel()
	ownerTestPending(t, finished, "legacy worker still owns cleanup")
	release()
	ownerTestWait(t, finished, "legacy cleanup handback")
}
