// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

func TestReadGuardAcquisitionOnceAndLatePinRetained(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	entered, release := make(chan struct{}), make(chan struct{})
	pins := make(chan *ownerTestPin, 1)
	resources.acquire = func(_ context.Context, custody context.Context, _ readerlease.Binding) (readerPin, error) {
		close(entered)
		<-release // Simulate provider work that ignores cancellation.
		pin := newOwnerTestPin(custody)
		pins <- pin
		return pin, nil
	}
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	guard := ownerTestBegin(t, owner, context.Background(), 1)
	if guard.Check() == nil {
		t.Fatal("Acquiring guard passed Check")
	}
	if complete, err := guard.HoldConsumer(); complete != nil || err == nil {
		t.Fatal("Acquiring guard registered a consumer")
	}
	result := make(chan error, 1)
	go func() { result <- guard.acquire() }()
	ownerTestWait(t, entered, "acquisition entry")
	if err := guard.acquire(); !errors.Is(err, errReaderNotActive) || resources.acquires.Load() != 1 {
		t.Fatal("concurrent acquire started another provider call", err)
	}
	ownerTestTimeout(t, guard.Close)
	ownerTestTimeout(t, owner.Close)
	ownerTestPending(t, guard.Quiesced(), "guard with pending acquisition")
	if budget.snapshot() != (readerCounts{owners: 1, guards: 1, pins: 1}) || resources.closes.Load() != 0 {
		t.Fatal("pending acquisition lost custody")
	}
	unblock()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("late pin granted a cancelled consumer")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("released provider call did not return")
	}
	ownerTestWait(t, owner.Quiesced(), "late acquisition owner cleanup")
	pin := <-pins
	if pin.closes.Load() != 1 || resources.closes.Load() != 1 || budget.snapshot() != (readerCounts{}) {
		t.Fatal("late returned pin was not closed exactly once")
	}
	if err := ownerTestClose(t, guard.Close); !errors.Is(err, errReaderCleanupUnknown) {
		t.Fatal("late cleanup erased failed guard Close", err)
	}
}

func TestReadGuardCancellationRetainsRenewalUntilConsumerProof(t *testing.T) {
	type requestValue struct{}
	request, cancel := context.WithCancel(context.WithValue(context.Background(), requestValue{}, "request-only"))
	defer cancel()
	budget, resources := newReaderBudget(), newOwnerTestResources()
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	guard, pin := ownerTestAcquire(t, owner, request)
	complete, err := guard.HoldConsumer()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(complete)
	if pin.Context().Value(requestValue{}) != nil || guard.Context().Value(requestValue{}) != "request-only" {
		t.Fatal("request values escaped into custody or execution lost its request")
	}
	cancel()
	ownerTestWait(t, guard.Context().Done(), "request cancellation")
	if err := pin.renew(); err != nil || pin.renews.Load() != 1 || pin.closes.Load() != 0 || resources.closes.Load() != 0 {
		t.Fatal("request cancellation stopped retained pin renewal", err)
	}
	if guard.Check() == nil || budget.snapshot() != (readerCounts{owners: 1, guards: 1, pins: 1}) {
		t.Fatal("cancelled consumer remained usable or returned capacity")
	}
	complete()
	if err := ownerTestClose(t, guard.Close); !errors.Is(err, context.Canceled) {
		t.Fatal("request cancellation outcome was lost", err)
	}
	if pin.closes.Load() != 1 || budget.snapshot() != (readerCounts{owners: 1}) {
		t.Fatal("consumer proof did not release pin custody once")
	}
	if err := ownerTestClose(t, owner.Close); err != nil {
		t.Fatal("request failure became an owner cleanup failure", err)
	}
}

func TestReadGuardFourLifetimeTokensAndIdempotentProof(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	guard, pin := ownerTestAcquire(t, owner, context.Background())
	var completions [readerHoldLimit]func()
	for i := range completions {
		var err error
		completions[i], err = guard.HoldConsumer()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(completions[i])
	}
	completions[0]()
	if complete, err := guard.HoldConsumer(); complete != nil || !errors.Is(err, errReaderCapacity) {
		t.Fatal("completed token was reused for a fifth registration", err)
	}
	ownerTestTimeout(t, guard.Close)
	if complete, err := guard.HoldConsumer(); complete != nil || err == nil {
		t.Fatal("closing guard admitted a new consumer")
	}
	if pin.closes.Load() != 0 || pin.renew() != nil || budget.snapshot() != (readerCounts{owners: 1, guards: 1, pins: 1}) {
		t.Fatal("a missing cleanup token was released by timeout")
	}
	var wg sync.WaitGroup
	for range 16 {
		for _, complete := range completions {
			wg.Add(1)
			go func() { defer wg.Done(); complete() }()
		}
	}
	wg.Wait()
	ownerTestWait(t, guard.Quiesced(), "completed consumer proofs")
	if pin.closes.Load() != 1 || resources.closes.Load() != 0 || budget.snapshot() != (readerCounts{owners: 1}) {
		t.Fatal("duplicate consumer callbacks changed custody more than once")
	}
	if err := ownerTestClose(t, guard.Close); !errors.Is(err, errReaderCleanupUnknown) {
		t.Fatal("late proofs erased bounded cleanup failure", err)
	}
}

func TestReadGuardPinLossCancelsExecutionButRetainsConsumer(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	guard, pin := ownerTestAcquire(t, owner, context.Background())
	complete, err := guard.HoldConsumer()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(complete)
	pin.cancel(readerlease.ErrLost)
	if err := guard.Check(); !errors.Is(err, readerlease.ErrLost) {
		t.Fatal("synchronous Check missed pin loss", err)
	}
	ownerTestWait(t, guard.Context().Done(), "pin loss cancellation")
	ownerTestPending(t, guard.Quiesced(), "consumer after pin loss")
	if pin.closes.Load() != 0 || resources.closes.Load() != 0 {
		t.Fatal("pin loss supplied false consumer cleanup proof")
	}
	complete()
	if err := ownerTestClose(t, guard.Close); !errors.Is(err, readerlease.ErrLost) {
		t.Fatal("pin loss was discarded after cleanup", err)
	}
}

func TestReadGuardPinCloseAndQuiescenceRemainCharged(t *testing.T) {
	for _, mode := range []string{"method", "quiescence"} {
		t.Run(mode, func(t *testing.T) {
			budget, resources := newReaderBudget(), newOwnerTestResources()
			gate := make(chan struct{})
			var pin *ownerTestPin
			resources.acquire = func(_ context.Context, custody context.Context, _ readerlease.Binding) (readerPin, error) {
				pin = newOwnerTestPin(custody)
				if mode == "method" {
					pin.closeGate = gate
				} else {
					pin.manualQuiet = true
				}
				pin.closeError = readerlease.ErrReleaseUnknown
				return pin, nil
			}
			owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
			guard, _ := ownerTestAcquire(t, owner, context.Background())
			var once sync.Once
			release := func() {
				once.Do(func() {
					if mode == "method" {
						close(gate)
					} else {
						close(pin.quiet)
					}
				})
			}
			t.Cleanup(release)
			ownerTestTimeout(t, guard.Close)
			ownerTestWait(t, pin.closeStarted, "pin close entry")
			for range 16 {
				ownerTestTimeout(t, guard.Close)
			}
			ownerTestPending(t, guard.Quiesced(), "blocked pin cleanup")
			if pin.closes.Load() != 1 || resources.closes.Load() != 0 || budget.snapshot() != (readerCounts{owners: 1, guards: 1, pins: 1}) {
				t.Fatal("repeated Close released custody or started another finalizer")
			}
			release()
			ownerTestWait(t, guard.Quiesced(), "released pin cleanup")
			if err := ownerTestClose(t, guard.Close); !errors.Is(err, errReaderCleanupUnknown) || !errors.Is(err, readerlease.ErrReleaseUnknown) {
				t.Fatal("cleanup or remote release uncertainty was lost", err)
			}
			if budget.snapshot() != (readerCounts{owners: 1}) {
				t.Fatal("joined pin did not return guard capacity")
			}
		})
	}
}

func TestReadGuardPartialAcquisitionKeepsEntireReservation(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	entered, release := make(chan struct{}), make(chan struct{})
	var first *ownerTestPin
	resources.acquire = func(_ context.Context, custody context.Context, _ readerlease.Binding) (readerPin, error) {
		if resources.acquires.Load() == 1 {
			first = newOwnerTestPin(custody)
			return first, nil
		}
		close(entered)
		<-release
		return nil, readerlease.ErrUnavailable
	}
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	guard := ownerTestBegin(t, owner, context.Background(), 3)
	result := make(chan error, 1)
	go func() { result <- guard.acquire() }()
	ownerTestWait(t, entered, "second acquisition")
	ownerTestTimeout(t, guard.Close)
	if budget.snapshot() != (readerCounts{owners: 1, guards: 1, pins: 3}) || first.closes.Load() != 0 || first.renew() != nil {
		t.Fatal("partial acquisition released pins or unused reserved capacity")
	}
	unblock()
	select {
	case err := <-result:
		if !errors.Is(err, readerlease.ErrUnavailable) {
			t.Fatal("acquisition failure lost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("failed acquisition did not join")
	}
	ownerTestWait(t, guard.Quiesced(), "partial acquisition cleanup")
	if resources.acquires.Load() != 2 || first.closes.Load() != 1 || budget.snapshot() != (readerCounts{owners: 1}) {
		t.Fatal("partial failure reached another pin or leaked capacity")
	}
}

func TestReadGuardAdoptsPinReturnedWithError(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	var pin *ownerTestPin
	resources.acquire = func(_ context.Context, custody context.Context, _ readerlease.Binding) (readerPin, error) {
		pin = newOwnerTestPin(custody)
		return pin, readerlease.ErrUnavailable
	}
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	guard := ownerTestBegin(t, owner, context.Background(), 1)
	if err := guard.acquire(); !errors.Is(err, readerlease.ErrUnavailable) {
		t.Fatal(err)
	}
	if err := ownerTestClose(t, guard.Close); !errors.Is(err, readerlease.ErrUnavailable) {
		t.Fatal(err)
	}
	if pin.closes.Load() != 1 || budget.snapshot() != (readerCounts{owners: 1}) {
		t.Fatal("error-returned pin escaped custody")
	}
}

func TestReadGuardNormalCloseAndLastBoundaryLoss(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "loss_before_close"}[lost], func(t *testing.T) {
			budget, resources := newReaderBudget(), newOwnerTestResources()
			owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
			guard, pin := ownerTestAcquire(t, owner, context.Background())
			if lost {
				pin.closeHook = func() { pin.cancel(readerlease.ErrLost) }
			}
			for _, ctx := range []context.Context{nil, context.Background()} {
				if err := guard.Close(ctx); !errors.Is(err, errReaderInvalid) {
					t.Fatal("unbounded cleanup was accepted", err)
				}
			}
			err := ownerTestClose(t, guard.Close)
			if (!lost && err != nil) || (lost && !errors.Is(err, readerlease.ErrLost)) {
				t.Fatal("wrong final pin outcome", err)
			}
			if pin.closes.Load() != 1 || budget.snapshot() != (readerCounts{owners: 1}) {
				t.Fatal("normal cleanup did not release once")
			}
		})
	}
}

func TestReadGuardConcurrentCloseRegistrationAndProof(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	for range 16 {
		guard, pin := ownerTestAcquire(t, owner, context.Background())
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				complete, err := guard.HoldConsumer()
				if err == nil {
					complete()
					complete()
				}
				_ = guard.Check()
			}()
		}
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _ = ownerTestClose(t, guard.Close) }()
		close(start)
		wg.Wait()
		if err := ownerTestClose(t, guard.Close); err != nil {
			t.Fatal(err)
		}
		if pin.closes.Load() != 1 || budget.snapshot() != (readerCounts{owners: 1}) {
			t.Fatal("concurrent proof changed custody more than once")
		}
	}
}

func TestReadGuardOwnerBookkeepingPrecedesCapacityAndQuiescence(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	guard, pin := ownerTestAcquire(t, owner, context.Background())
	owner.mu.Lock()
	var once sync.Once
	unlock := func() { once.Do(owner.mu.Unlock) }
	defer unlock()
	guard.startClose(nil)
	ownerTestWait(t, pin.Quiesced(), "pin cleanup before owner bookkeeping")
	ownerTestWait(t, guard.custody.Done(), "joined guard work before owner bookkeeping")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := guard.Close(ctx); !errors.Is(err, errReaderCleanupUnknown) {
		t.Fatal("guard completed before its owner could retire the record", err)
	}
	ownerTestPending(t, guard.Quiesced(), "guard bookkeeping")
	if budget.snapshot() != (readerCounts{owners: 1, guards: 1, pins: 1}) || len(owner.guards) != 1 {
		t.Fatal("capacity returned while the owner still held its unfinished record")
	}
	unlock()
	ownerTestWait(t, guard.Quiesced(), "released owner bookkeeping")
	owner.mu.Lock()
	left := len(owner.guards)
	owner.mu.Unlock()
	if budget.snapshot() != (readerCounts{owners: 1}) || left != 0 {
		t.Fatal("owner record and returned capacity did not retire together")
	}
}

func TestReadGuardCheckOnlyFailureIsStickyAndSealsConsumers(t *testing.T) {
	for _, observation := range []string{"check", "hold"} {
		t.Run(observation, func(t *testing.T) {
			budget, resources := newReaderBudget(), newOwnerTestResources()
			owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
			guard, pin := ownerTestAcquire(t, owner, context.Background())
			complete, err := guard.HoldConsumer()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(complete)
			pin.checkFailure.Store(true)
			if observation == "check" {
				err = guard.Check()
			} else {
				var extra func()
				extra, err = guard.HoldConsumer()
				if extra != nil {
					t.Fatal("failed Check registered a consumer")
				}
			}
			if !errors.Is(err, readerlease.ErrExpired) {
				t.Fatal("check-only failure was hidden", err)
			}
			pin.checkFailure.Store(false)
			if context.Cause(pin.Context()) != nil || pin.Check() != nil {
				t.Fatal("fixture unexpectedly cancelled custody")
			}
			ownerTestWait(t, guard.Context().Done(), "check-only failure cancellation")
			if !errors.Is(guard.Check(), readerlease.ErrExpired) {
				t.Fatal("observed failure was cleared by provider recovery")
			}
			if extra, err := guard.HoldConsumer(); extra != nil || !errors.Is(err, readerlease.ErrExpired) {
				t.Fatal("failed guard admitted a new consumer", err)
			}
			if err := pin.renew(); err != nil || pin.closes.Load() != 0 {
				t.Fatal("failure released an outstanding consumer", err)
			}
			complete()
			if err := ownerTestClose(t, guard.Close); !errors.Is(err, readerlease.ErrExpired) {
				t.Fatal("Close erased check-only failure", err)
			}
			if err := ownerTestClose(t, owner.Close); err != nil {
				t.Fatal("query failure became owner cleanup failure", err)
			}
		})
	}
}

func TestReadGuardLateSuccessSeparatesExplicitCloseFromOwnerDrain(t *testing.T) {
	for _, stop := range []string{"guard_close", "owner_drain"} {
		t.Run(stop, func(t *testing.T) {
			budget, resources := newReaderBudget(), newOwnerTestResources()
			entered, release := make(chan struct{}), make(chan struct{})
			resources.acquire = func(_ context.Context, custody context.Context, _ readerlease.Binding) (readerPin, error) {
				close(entered)
				<-release
				return newOwnerTestPin(custody), nil
			}
			owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			guard := ownerTestBegin(t, owner, context.Background(), 1)
			acquired := make(chan error, 1)
			go func() { acquired <- guard.acquire() }()
			ownerTestWait(t, entered, "late acquisition entry")
			if stop == "guard_close" {
				guard.startClose(nil)
			} else {
				owner.startClose()
			}
			ownerTestWait(t, guard.Context().Done(), "intentional acquisition stop")
			unblock()
			select {
			case err := <-acquired:
				if err == nil {
					t.Fatal("late acquisition was usable after seal")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("late acquisition failed to return")
			}
			err := ownerTestClose(t, guard.Close)
			if stop == "guard_close" && err != nil {
				t.Fatal("explicit Close invented a query failure", err)
			}
			if stop == "owner_drain" && !errors.Is(err, errReaderOwnerClosed) {
				t.Fatal("owner drain lost its query failure", err)
			}
			if guard.Check() == nil {
				t.Fatal("closed late acquisition still usable")
			}
			if err := ownerTestClose(t, owner.Close); err != nil {
				t.Fatal("joined owner cleanup should succeed", err)
			}
			if budget.snapshot() != (readerCounts{}) {
				t.Fatal("joined late acquisition retained capacity")
			}
		})
	}
}

func TestReadGuardOwnerDrainAfterFinalCheckCannotBecomeSuccess(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	guard, _ := ownerTestAcquire(t, owner, context.Background())
	if err := guard.Check(); err != nil {
		t.Fatal(err)
	}
	owner.startClose()
	if err := ownerTestClose(t, guard.Close); !errors.Is(err, errReaderOwnerClosed) {
		t.Fatal("owner drain after Check became a successful query", err)
	}
	if err := ownerTestClose(t, owner.Close); err != nil {
		t.Fatal("joined owner drain should have nil cleanup outcome", err)
	}
	if resources.closes.Load() != 1 || budget.snapshot() != (readerCounts{}) {
		t.Fatal("owner drain leaked or retried resource close")
	}
}

func TestReadGuardCopiesBindingsBeforeAcquire(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	want := ownerTestBindings(1)[0]
	observed := make(chan readerlease.Binding, 1)
	resources.acquire = func(_ context.Context, custody context.Context, binding readerlease.Binding) (readerPin, error) {
		observed <- binding
		return newOwnerTestPin(custody), nil
	}
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	input := []readerlease.Binding{want}
	guard, err := owner.begin(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = readerlease.Binding{}
	if err := guard.acquire(); err != nil {
		t.Fatal(err)
	}
	if got := <-observed; got != want {
		t.Fatal("caller mutation changed acquired immutable identity")
	}
	if err := ownerTestClose(t, guard.Close); err != nil {
		t.Fatal(err)
	}
}

func TestReadGuardOwnerDrainTerminalHandoffCannotLoseFailure(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	first, _ := ownerTestAcquire(t, owner, context.Background())
	second, _ := ownerTestAcquire(t, owner, context.Background())
	entered, resume := make(chan *ReadGuard, 2), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	for _, guard := range []*ReadGuard{first, second} {
		cancel := guard.cancelExecution
		guard.cancelExecution = func(cause error) {
			cancel(cause)
			if cause == errReaderOwnerClosed {
				entered <- guard
				<-resume
			}
		}
	}
	drainReturned := make(chan struct{})
	go func() { owner.startClose(); close(drainReturned) }()
	var paused *ReadGuard
	select {
	case paused = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("owner drain did not reach notification handoff")
	}
	finishing := first
	if paused == first {
		finishing = second
	}
	// Actual owner drain is paused before notifying this guard. It can finish
	// its own successful work now, but terminal publication must see the seal.
	if err := ownerTestClose(t, finishing.Close); !errors.Is(err, errReaderOwnerClosed) {
		t.Fatal("terminal handoff lost an already-sealed owner drain", err)
	}
	unblock()
	ownerTestWait(t, drainReturned, "owner drain notifications")
	if err := ownerTestClose(t, owner.Close); err != nil {
		t.Fatal("joined drain became an owner cleanup failure", err)
	}
	if budget.snapshot() != (readerCounts{}) || resources.closes.Load() != 1 {
		t.Fatal("drain handoff leaked resource ownership")
	}
}

func TestReadGuardRequestCancellationAfterCallbackJoinRemainsFailure(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	guard, _ := ownerTestAcquire(t, owner, request)
	owner.mu.Lock()
	var once sync.Once
	unlock := func() { once.Do(owner.mu.Unlock) }
	defer unlock()
	guard.startClose(nil)
	// Custody ends only after cancellation callbacks have joined. Holding the
	// owner lock keeps terminal publication pending without any provider work.
	ownerTestWait(t, guard.custody.Done(), "callback join before publication")
	cancel()
	unlock()
	if err := ownerTestClose(t, guard.Close); !errors.Is(err, context.Canceled) {
		t.Fatal("late request cancellation disappeared with its stopped callback", err)
	}
	if budget.snapshot() != (readerCounts{owners: 1}) {
		t.Fatal("terminal cancellation leaked joined guard capacity")
	}
}
