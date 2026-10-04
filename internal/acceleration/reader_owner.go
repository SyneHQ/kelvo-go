// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"sync"

	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

type readerOwnerState uint8

const (
	readerOpening readerOwnerState = iota
	readerOwnerActive
	readerDraining
	readerOwnerQuiesced
)

// ReaderOwner must not be copied. Its clients and unfinished guards stay owned
// after a bounded Close failure. The immutable object runtime reserves the
// owner before construction; fixture attachment takes completed bundles only.
type ReaderOwner struct {
	budget        *ReaderBudget
	spec          readerOwnerSpec
	mu            sync.Mutex
	state         readerOwnerState
	resources     readerResources
	guards        map[*ReadGuard]struct{}
	outcome       readerOutcome
	wake          chan struct{}
	quiesced      chan struct{}
	opening       bool
	openingDone   chan struct{}
	openingCancel context.CancelCauseFunc
}

func (b *ReaderBudget) newOwner(spec readerOwnerSpec) (*ReaderOwner, error) {
	if b == nil {
		return nil, errReaderInvalid
	}
	immutable, err := spec.immutable()
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.counts.owners >= readerOwnerLimit {
		return nil, errReaderCapacity
	}
	b.counts.owners++
	return &ReaderOwner{budget: b, spec: immutable, guards: make(map[*ReadGuard]struct{}),
		wake: make(chan struct{}, 1), quiesced: make(chan struct{})}, nil
}

// attach only transfers an already-created fixture bundle. No method call or
// provider construction takes place here. A rejected attachment does not take
// custody from its caller. Runtime construction uses open so partial resources
// remain owned by the reserved Opening record.
func (o *ReaderOwner) attach(resources readerResources) error {
	if nilReaderDependency(resources) {
		return errReaderInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state != readerOpening || o.opening {
		return errReaderClosed
	}
	o.resources = resources
	o.state = readerOwnerActive
	return nil
}

// open keeps the Opening reservation charged while a synchronous constructor
// runs. Every late or partial result is adopted before cleanup can finish.
func (o *ReaderOwner) open(factory func(context.Context) (readerResources, error)) error {
	if factory == nil {
		return errReaderInvalid
	}
	o.mu.Lock()
	if o.state != readerOpening || o.opening {
		o.mu.Unlock()
		return errReaderClosed
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	o.opening, o.openingCancel, o.openingDone = true, cancel, make(chan struct{})
	o.mu.Unlock()
	resources, err := factory(ctx)
	if nilReaderDependency(resources) {
		resources = nil
		if err == nil {
			err = errReaderInvalid
		}
	}
	o.mu.Lock()
	o.resources = resources
	if err == nil && o.state == readerOpening {
		o.state = readerOwnerActive
	} else if err == nil {
		err = errReaderClosed
	}
	o.opening = false
	close(o.openingDone)
	o.mu.Unlock()
	cancel(errReaderClosed)
	if err != nil {
		o.startClose()
	}
	return err
}

func (o *ReaderOwner) begin(ctx context.Context, bindings []readerlease.Binding) (*ReadGuard, error) {
	if ctx == nil || !o.spec.validBindings(bindings) {
		return nil, errReaderInvalid
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state != readerOwnerActive {
		return nil, errReaderClosed
	}
	if !o.budget.reserveGuard(len(bindings)) {
		return nil, errReaderCapacity
	}
	// Everything below, including cancellation callbacks, has a complete node
	// reservation. Owner drain cannot interleave before the guard is registered.
	g := newReadGuard(o, ctx, bindings)
	o.guards[g] = struct{}{}
	g.watchRequest()
	return g, nil
}

func (o *ReaderOwner) Quiesced() <-chan struct{} { return o.quiesced }

func (o *ReaderOwner) startClose() {
	o.mu.Lock()
	if o.state >= readerDraining {
		o.mu.Unlock()
		return
	}
	o.state = readerDraining
	openingCancel := o.openingCancel
	var guards [readerGuardLimit]*ReadGuard
	n := 0
	for g := range o.guards {
		guards[n] = g
		n++
	}
	o.mu.Unlock()
	if openingCancel != nil {
		openingCancel(errReaderOwnerClosed)
	}
	for _, g := range guards[:n] {
		g.startClose(errReaderOwnerClosed)
	}
	go o.finish()
}

func (o *ReaderOwner) Close(ctx context.Context) error {
	if !validReaderCleanup(ctx) {
		return errReaderInvalid
	}
	o.startClose()
	select {
	case <-o.quiesced:
	case <-ctx.Done():
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state != readerOwnerQuiesced {
		o.outcome.unknown = true
	}
	return o.outcome.err()
}

func (o *ReaderOwner) guardDone(g *ReadGuard) {
	o.mu.Lock()
	// All external work has joined. Retire the record and publish its capacity
	// and quiescence under the admission lock: a replacement cannot grow the
	// unfinished-guard set beyond the fixed node bound between those steps.
	g.mu.Lock()
	if o.state == readerDraining {
		// Drain seals owner admission before notifying guards outside this
		// lock. A guard can finish in that handoff gap; it must not publish
		// success merely because its cancellation notification has not run.
		g.outcome.fail(errReaderOwnerClosed)
	}
	// The request callback may already have been joined/stopped while this
	// finalizer waited for owner bookkeeping. Preserve cancellation observed
	// at the terminal publication boundary even when no callback will run.
	g.outcome.fail(context.Cause(g.request))
	delete(o.guards, g)
	o.outcome.cleanupFailed(g.outcome.cleanupError())
	o.budget.releaseGuard(len(g.bindings))
	g.state = readerQuiesced
	close(g.quiesced)
	g.mu.Unlock()
	o.mu.Unlock()
	readerWake(o.wake)
}

func (o *ReaderOwner) finish() {
	for {
		o.mu.Lock()
		empty := len(o.guards) == 0
		resources := o.resources
		opening, openingDone := o.opening, o.openingDone
		o.mu.Unlock()
		if opening {
			<-openingDone
			continue
		}
		if empty {
			if resources != nil {
				err := resources.Close()
				o.mu.Lock()
				o.outcome.cleanupFailed(err)
				o.mu.Unlock()
				<-resources.Quiesced()
			}
			break
		}
		<-o.wake
	}
	o.budget.mu.Lock()
	o.budget.counts.owners--
	o.budget.mu.Unlock()
	o.mu.Lock()
	o.state = readerOwnerQuiesced
	close(o.quiesced)
	o.mu.Unlock()
}
