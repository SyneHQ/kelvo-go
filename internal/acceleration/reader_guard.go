// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"slices"
	"sync"

	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

type readerGuardState uint8

const (
	readerAcquiring readerGuardState = iota
	readerActive
	readerClosing
	readerQuiesced
)

// ReadGuard must not be copied. No consumer may start from an Acquiring guard.
// Close seals registrations but retains renewing custody until every registered
// consumer and acquisition actually joins. Quiesced proves only local cleanup;
// it never grants remote deletion authority or reverses a failed query result.
type ReadGuard struct {
	owner                       *ReaderOwner
	bindings                    []readerlease.Binding
	request, execution, custody context.Context
	cancelExecution, endCustody context.CancelCauseFunc
	mu                          sync.Mutex
	state                       readerGuardState
	acquireStarted, pinsClosing bool
	acquireDone                 chan struct{}
	pins                        []readerPin
	requestWatch                readerCallback
	pinWatches                  []readerCallback
	consumers                   [readerHoldLimit]bool
	registered, outstanding     int
	outcome                     readerOutcome
	wake, quiesced              chan struct{}
}

func newReadGuard(owner *ReaderOwner, request context.Context, bindings []readerlease.Binding) *ReadGuard {
	execution, cancelExecution := context.WithCancelCause(request)
	custody, endCustody := context.WithCancelCause(context.Background())
	return &ReadGuard{owner: owner, bindings: slices.Clone(bindings), request: request,
		execution: execution, custody: custody, cancelExecution: cancelExecution, endCustody: endCustody,
		acquireDone: make(chan struct{}), pins: make([]readerPin, 0, len(bindings)),
		pinWatches: make([]readerCallback, 0, len(bindings)), wake: make(chan struct{}, 1), quiesced: make(chan struct{})}
}

func (g *ReadGuard) watchRequest() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.requestWatch = readerAfter(g.request, func() { g.startClose(context.Cause(g.request)) })
}

func (g *ReadGuard) Context() context.Context  { return g.execution }
func (g *ReadGuard) Quiesced() <-chan struct{} { return g.quiesced }

// acquire is once-only and sequential. A synchronous provider that ignores
// cancellation keeps this call and its node reservation alive until it returns.
// A concurrent Close waits for that return before releasing any adopted pin.
func (g *ReadGuard) acquire() (result error) {
	g.mu.Lock()
	if g.state != readerAcquiring || g.acquireStarted {
		g.mu.Unlock()
		return errReaderNotActive
	}
	g.acquireStarted = true
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		if result == nil {
			result = context.Cause(g.execution)
		}
		if result == nil && g.state == readerAcquiring {
			g.state = readerActive
		} else if result == nil {
			result = errReaderClosed
		}
		failure := result
		if failure == errReaderClosed || (failure == context.Canceled && context.Cause(g.execution) == errReaderClosed && context.Cause(g.request) == nil) {
			// Explicit guard.Close stops admission/execution. Its own stop
			// does not turn a joined late pin into a spurious lease failure.
			// Owner drain is different: that query must remain failed.
			failure = nil
		}
		g.outcome.fail(failure)
		close(g.acquireDone)
		g.mu.Unlock()
		if result != nil {
			if failure == nil {
				g.startClose(nil)
			} else {
				g.startClose(failure)
			}
		}
	}()
	for _, binding := range g.bindings {
		g.mu.Lock()
		closing := g.state != readerAcquiring
		g.mu.Unlock()
		if closing {
			return errReaderClosed
		}
		if err := context.Cause(g.execution); err != nil {
			return err
		}
		pin, err := g.owner.resources.AcquireWithLifetime(g.execution, g.custody, binding)
		if !nilReaderDependency(pin) {
			// Adopt even a pin returned with an error or after cancellation.
			// Ownership never escapes through a late provider result.
			g.mu.Lock()
			g.pins = append(g.pins, pin)
			pinCtx := pin.Context()
			if pinCtx == nil || pin.Quiesced() == nil {
				err = errReaderInvalid
			} else {
				g.pinWatches = append(g.pinWatches, readerAfter(pinCtx, func() { g.pinStopped(context.Cause(pinCtx)) }))
			}
			g.mu.Unlock()
		} else if err == nil {
			err = errReaderInvalid
		}
		if err != nil {
			return err
		}
		if err := pin.Check(); err != nil {
			return err
		}
	}
	return nil
}

func (g *ReadGuard) pinStopped(cause error) {
	g.mu.Lock()
	intentional := g.pinsClosing && cause == readerlease.ErrClosed
	g.mu.Unlock()
	if !intentional {
		g.startClose(cause)
	}
}

func (g *ReadGuard) checkLocked() error {
	if g.state != readerActive {
		if err := g.outcome.err(); err != nil {
			return err
		}
		return errReaderNotActive
	}
	if err := context.Cause(g.request); err != nil {
		return err
	}
	for _, pin := range g.pins {
		if err := pin.Check(); err != nil {
			return err
		}
	}
	return nil
}

func (g *ReadGuard) Check() error {
	g.mu.Lock()
	active := g.state == readerActive
	err := g.checkLocked()
	first := false
	if active && err != nil {
		first = g.sealLocked(err)
	}
	g.mu.Unlock()
	if active && err != nil {
		g.finishStop(err, first)
	}
	return err
}

// HoldConsumer registers one of four lifetime tokens. Completion is trusted
// cleanup proof, not a timeout or a public safe=true assertion. The callback
// performs no provider I/O; it only records this exact token once and wakes the
// existing finalizer. Completed tokens cannot be reused for more registrations.
func (g *ReadGuard) HoldConsumer() (func(), error) {
	g.mu.Lock()
	active := g.state == readerActive
	if err := g.checkLocked(); err != nil {
		first := false
		if active {
			first = g.sealLocked(err)
		}
		g.mu.Unlock()
		if active {
			g.finishStop(err, first)
		}
		return nil, err
	}
	if g.registered == len(g.consumers) {
		g.mu.Unlock()
		return nil, errReaderCapacity
	}
	index := g.registered
	g.registered++
	g.outstanding++
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		if !g.consumers[index] {
			g.consumers[index] = true
			g.outstanding--
		}
		g.mu.Unlock()
		readerWake(g.wake)
	}, nil
}

func (g *ReadGuard) startClose(cause error) {
	g.mu.Lock()
	first := g.sealLocked(cause)
	g.mu.Unlock()
	g.finishStop(cause, first)
}

// Seal before releasing the observation lock. A Check-only failure cannot be
// cleared by a provider and followed by a new successful consumer registration.
func (g *ReadGuard) sealLocked(failure error) bool {
	if g.state == readerQuiesced {
		return false
	}
	g.outcome.fail(failure)
	first := g.state != readerClosing
	if first {
		g.state = readerClosing
		if !g.acquireStarted {
			close(g.acquireDone)
		}
	}
	return first
}

func (g *ReadGuard) finishStop(cause error, first bool) {
	if cause == nil {
		cause = errReaderClosed
	}
	g.cancelExecution(cause)
	if first {
		go g.finish()
	}
}

func (g *ReadGuard) Close(ctx context.Context) error {
	if !validReaderCleanup(ctx) {
		return errReaderInvalid
	}
	g.startClose(nil)
	select {
	case <-g.quiesced:
	case <-ctx.Done():
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state != readerQuiesced {
		g.outcome.unknown = true
	}
	return g.outcome.err()
}

func (g *ReadGuard) finish() {
	<-g.acquireDone
	for {
		g.mu.Lock()
		quiet := g.outstanding == 0
		g.mu.Unlock()
		if quiet {
			break
		}
		<-g.wake
	}
	g.mu.Lock()
	g.outcome.fail(context.Cause(g.request))
	for _, pin := range g.pins {
		g.outcome.fail(pin.Check())
	}
	g.pinsClosing = true
	g.mu.Unlock()
	for _, pin := range g.pins {
		err := pin.Close()
		g.mu.Lock()
		g.outcome.cleanupFailed(err)
		g.mu.Unlock()
		<-pin.Quiesced()
	}
	// A cancellation callback may be queued but not yet started when join
	// stops it. Read the pin's winning cause synchronously as well, so loss
	// between the final Check and our Close cannot disappear with that callback.
	g.mu.Lock()
	for _, pin := range g.pins {
		if pinCtx := pin.Context(); pinCtx != nil {
			if cause := context.Cause(pinCtx); cause != readerlease.ErrClosed {
				g.outcome.fail(cause)
			}
		}
	}
	g.mu.Unlock()
	for _, callback := range g.pinWatches {
		callback.join()
	}
	g.requestWatch.join()
	g.endCustody(errReaderClosed)
	g.owner.guardDone(g)
}
