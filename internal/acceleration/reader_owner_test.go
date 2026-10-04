// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestReaderOwnerAdoptsLatePartialConstructionAfterClose(t *testing.T) {
	budget := newReaderBudget()
	owner, err := budget.newOwner(ownerTestSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	ownerTestAttach(t, budget, ownerTestSpec(t), newOwnerTestResources())
	started, returned, release := make(chan struct{}), make(chan error, 1), make(chan struct{})
	resources := newOwnerTestResources()
	var once sync.Once
	defer once.Do(func() { close(release) })
	failure := errors.New("late construction failed")
	go func() {
		returned <- owner.open(func(ctx context.Context) (readerResources, error) {
			close(started)
			<-release // Deliberately ignore cancellation; the Opening slot must remain owned.
			return resources, failure
		})
	}()
	ownerTestWait(t, started, "constructor entry")
	ownerTestTimeout(t, owner.Close)
	if _, err := budget.newOwner(ownerTestSpec(t)); !errors.Is(err, errReaderCapacity) {
		t.Fatal("unfinished opening released its rotation slot", err)
	}
	ownerTestPending(t, owner.Quiesced(), "late construction")
	if resources.closes.Load() != 0 {
		t.Fatal("unreturned resource closed prematurely")
	}
	once.Do(func() { close(release) })
	if err := <-returned; !errors.Is(err, failure) {
		t.Fatal(err)
	}
	ownerTestWait(t, owner.Quiesced(), "late partial resource cleanup")
	if resources.closes.Load() != 1 || budget.snapshot() != (readerCounts{owners: 1}) {
		t.Fatal("late resource was discarded or capacity returned before cleanup")
	}
}

func TestReaderBudgetSharedAcrossOwnerRotations(t *testing.T) {
	budget, spec := newReaderBudget(), ownerTestSpec(t)
	firstResources, secondResources := newOwnerTestResources(), newOwnerTestResources()
	first := ownerTestAttach(t, budget, spec, firstResources)
	second := ownerTestAttach(t, budget, spec, secondResources)
	a := ownerTestBegin(t, first, context.Background(), 64)
	b := ownerTestBegin(t, second, context.Background(), 64)
	if got := budget.snapshot(); got != (readerCounts{owners: 2, guards: 2, pins: 128}) {
		t.Fatal("node did not share complete pin reservations", got)
	}
	if _, err := first.begin(context.Background(), ownerTestBindings(1)); !errors.Is(err, errReaderCapacity) {
		t.Fatal("new guard bypassed node pin limit", err)
	}
	if _, err := budget.newOwner(spec); !errors.Is(err, errReaderCapacity) {
		t.Fatal("rotation bypassed live owner limit", err)
	}
	if firstResources.acquires.Load()+secondResources.acquires.Load() != 0 {
		t.Fatal("reservation-only work performed provider calls")
	}
	if err := ownerTestClose(t, a.Close); err != nil {
		t.Fatal(err)
	}
	if got := budget.snapshot(); got != (readerCounts{owners: 2, guards: 1, pins: 64}) {
		t.Fatal("joined guard returned the wrong reservation", got)
	}
	if err := ownerTestClose(t, first.Close); err != nil {
		t.Fatal(err)
	}
	replacement := ownerTestAttach(t, budget, spec, newOwnerTestResources())
	c := ownerTestBegin(t, replacement, context.Background(), 64)
	if got := budget.snapshot(); got != (readerCounts{owners: 2, guards: 2, pins: 128}) {
		t.Fatal("replacement reset the old owner's charged pin capacity", got)
	}
	if err := ownerTestClose(t, b.Close); err != nil {
		t.Fatal(err)
	}
	if err := ownerTestClose(t, c.Close); err != nil {
		t.Fatal(err)
	}
}

func TestReaderBudgetBoundsGuardRecordsAcrossOwners(t *testing.T) {
	budget, spec := newReaderBudget(), ownerTestSpec(t)
	owners := []*ReaderOwner{ownerTestAttach(t, budget, spec, newOwnerTestResources()), ownerTestAttach(t, budget, spec, newOwnerTestResources())}
	guards := make([]*ReadGuard, readerGuardLimit)
	for i := range guards {
		guards[i] = ownerTestBegin(t, owners[i%2], context.Background(), 1)
	}
	if _, err := owners[0].begin(context.Background(), ownerTestBindings(1)); !errors.Is(err, errReaderCapacity) {
		t.Fatal("guard record limit was bypassed", err)
	}
	if got := budget.snapshot(); got != (readerCounts{owners: 2, guards: 128, pins: 128}) {
		t.Fatal(got)
	}
	for _, guard := range guards {
		if err := ownerTestClose(t, guard.Close); err != nil {
			t.Fatal(err)
		}
	}
	if got := budget.snapshot(); got != (readerCounts{owners: 2}) {
		t.Fatal("record cleanup leaked capacity", got)
	}
}

func TestReaderOwnerResourceCloseRetainsRotationCapacity(t *testing.T) {
	for _, mode := range []string{"method", "quiescence"} {
		t.Run(mode, func(t *testing.T) {
			budget, spec, resources := newReaderBudget(), ownerTestSpec(t), newOwnerTestResources()
			gate := make(chan struct{})
			if mode == "method" {
				resources.closeGate = gate
			} else {
				resources.manualQuiet = true
			}
			owner := ownerTestAttach(t, budget, spec, resources)
			var once sync.Once
			release := func() {
				once.Do(func() {
					if mode == "method" {
						close(gate)
					} else {
						close(resources.quiet)
					}
				})
			}
			t.Cleanup(release)
			ownerTestTimeout(t, owner.Close)
			ownerTestWait(t, resources.closeStarted, "resource close entry")
			ownerTestPending(t, owner.Quiesced(), "owner")
			ownerTestAttach(t, budget, spec, newOwnerTestResources())
			if _, err := budget.newOwner(spec); !errors.Is(err, errReaderCapacity) {
				t.Fatal("unresolved client close was bypassed by rotation", err)
			}
			for range 16 {
				ownerTestTimeout(t, owner.Close)
			}
			if resources.closes.Load() != 1 || budget.snapshot() != (readerCounts{owners: 2}) {
				t.Fatal("repeated Close launched work or returned live capacity")
			}
			release()
			ownerTestWait(t, owner.Quiesced(), "late owner cleanup")
			if err := ownerTestClose(t, owner.Close); !errors.Is(err, errReaderCleanupUnknown) {
				t.Fatal("later cleanup erased failed Close", err)
			}
			if budget.snapshot() != (readerCounts{owners: 1}) {
				t.Fatal("verified resource cleanup did not return one owner slot")
			}
		})
	}
}

func TestReaderOwnerResourceFailureSeparateFromQuiescence(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	failure := errors.New("fixture resource close failure")
	resources.closeError = failure
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	if err := ownerTestClose(t, owner.Close); !errors.Is(err, failure) {
		t.Fatal("resource close failure was discarded", err)
	}
	ownerTestWait(t, owner.Quiesced(), "failed but quiescent owner")
	if budget.snapshot() != (readerCounts{}) || resources.closes.Load() != 1 {
		t.Fatal("local quiescence did not return capacity once")
	}
	if err := ownerTestClose(t, owner.Close); !errors.Is(err, failure) || resources.closes.Load() != 1 {
		t.Fatal("repeat Close discarded failure or retried resource close", err)
	}
}

func TestReaderOwnerOpeningAndInvalidCloseCannotAdmitConsumers(t *testing.T) {
	budget := newReaderBudget()
	owner, err := budget.newOwner(ownerTestSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.begin(context.Background(), ownerTestBindings(1)); !errors.Is(err, errReaderClosed) {
		t.Fatal("Opening owner admitted a guard", err)
	}
	var typedNil *ownerTestResources
	if err := owner.attach(typedNil); !errors.Is(err, errReaderInvalid) {
		t.Fatal("nil attachment was accepted", err)
	}
	for _, ctx := range []context.Context{nil, context.Background()} {
		if err := owner.Close(ctx); !errors.Is(err, errReaderInvalid) {
			t.Fatal("unbounded cleanup was accepted", err)
		}
	}
	if err := ownerTestClose(t, owner.Close); err != nil {
		t.Fatal(err)
	}
	resources := newOwnerTestResources()
	if err := owner.attach(resources); !errors.Is(err, errReaderClosed) || resources.closes.Load() != 0 {
		t.Fatal("rejected attachment changed resource custody", err)
	}
	if budget.snapshot() != (readerCounts{}) {
		t.Fatal("never-attached owner leaked its reservation")
	}
}
