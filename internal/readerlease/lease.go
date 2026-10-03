// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package readerlease

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// Lease must not be copied. Its context is the lifetime for every protected
// request and consumer. Call Check again before exposing success. Close must
// follow consumer/bridge shutdown and must be called even after cancellation.
// It never waits indefinitely for a provider. An uncooperative provider retains
// local capacity until its work actually exits, preventing unbounded growth.
type Lease struct {
	registry             *Registry
	binding              Binding
	ctx                  context.Context
	cancel               context.CancelCauseFunc
	mu                   sync.Mutex
	reader               entry
	clock                observation
	cutoff               time.Time
	wake                 chan struct{}
	renewDone, watchDone chan struct{}
	closeOnce            sync.Once
	closeErr             error
	quiesced             chan struct{}
}

func newLease(parent context.Context, r *Registry, binding Binding, reader entry, clock observation) (*Lease, error) {
	ctx, cancel := context.WithCancelCause(parent)
	l := &Lease{registry: r, binding: binding, ctx: ctx, cancel: cancel, reader: reader, clock: clock,
		cutoff: clock.local.Add(reader.ExpiresAt.Sub(clock.upper)), wake: make(chan struct{}, 1), renewDone: make(chan struct{}), watchDone: make(chan struct{}), quiesced: make(chan struct{})}
	if err := l.Check(); err != nil {
		cancel(err)
		return nil, err
	}
	return l, nil
}

func (l *Lease) Context() context.Context { return l.ctx }

func (l *Lease) Check() error {
	if err := context.Cause(l.ctx); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// Keep the expiry decision and cancellation in the same critical section
	// as renewal publication. A stale cutoff must not cancel a newer lease.
	now := l.registry.now()
	if _, _, err := l.clock.at(now); err != nil {
		l.cancel(err)
		return err
	}
	if !now.Before(l.cutoff) {
		l.cancel(ErrExpired)
		return ErrExpired
	}
	return context.Cause(l.ctx)
}

func (l *Lease) start() {
	go func() {
		defer close(l.renewDone)
		ticker := time.NewTicker(l.registry.config.RenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-l.ctx.Done():
				return
			case <-ticker.C:
				if err := l.renew(); err != nil {
					l.cancel(errors.Join(ErrLost, err))
					return
				}
			}
		}
	}()
	// The deadline watchdog is independent of renewal I/O. A stuck CAS cannot
	// keep the consumer authorized until a provider call happens to return.
	go func() {
		defer close(l.watchDone)
		for {
			if l.Check() != nil {
				return
			}
			l.mu.Lock()
			cutoff := l.cutoff
			l.mu.Unlock()
			timer := time.NewTimer(max(0, cutoff.Sub(l.registry.now())))
			select {
			case <-l.ctx.Done():
				timer.Stop()
				return
			case <-l.wake:
				timer.Stop()
			case <-timer.C:
			}
		}
	}()
}

func (l *Lease) renew() (resultErr error) {
	defer func() {
		if resultErr != nil {
			l.cancel(errors.Join(ErrLost, resultErr))
		}
	}()
	r := l.registry
	for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
		if err := l.Check(); err != nil {
			return err
		}
		l.mu.Lock()
		previous, clock, cutoff := l.reader, l.clock, l.cutoff
		l.mu.Unlock()
		ctx, cancel := context.WithDeadline(l.ctx, cutoff)
		old, err := r.read(ctx, l.binding.Reference)
		cancel()
		if check := l.Check(); check != nil {
			return check
		}
		if err != nil {
			return err
		}
		if err = bindingMatches(old.document, l.binding); err != nil {
			return err
		}
		if err = compatibleClock(clock, old.clock); err != nil {
			return err
		}
		lower, upper, err := old.clock.at(r.now())
		if err != nil {
			return err
		}
		index := -1
		for i, e := range old.document.Readers {
			if e.ID == previous.ID {
				index = i
				break
			}
		}
		if index < 0 || old.document.Readers[index] != previous || !previous.ExpiresAt.After(upper) {
			return ErrLost
		}
		if previous.Sequence == math.MaxUint64 {
			return ErrCorrupt
		}
		next := old.document
		if err = advance(&next); err != nil {
			return err
		}
		reader := previous
		reader.Sequence++
		reader.RenewedAt = lower.UTC()
		reader.ExpiresAt = lower.Add(r.config.LeaseDuration).UTC()
		if !reader.ExpiresAt.After(previous.ExpiresAt) {
			return ErrClock
		}
		next.Readers[index] = reader
		ctx, cancel = context.WithDeadline(l.ctx, cutoff)
		written, err := r.write(ctx, old, next)
		cancel()
		// A slow or ambiguous response cannot revive an expired local reader,
		// even when the provider actually committed a longer durable pin.
		if check := l.Check(); check != nil {
			return check
		}
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return err
		}
		if err = compatibleClock(clock, written.clock); err != nil {
			return err
		}
		newCutoff := written.clock.local.Add(reader.ExpiresAt.Sub(written.clock.upper))
		if !r.now().Before(newCutoff) {
			return ErrExpired
		}
		l.mu.Lock()
		l.reader, l.clock, l.cutoff = reader, written.clock, newCutoff
		l.mu.Unlock()
		select {
		case l.wake <- struct{}{}:
		default:
		}
		return l.Check()
	}
	return ErrConflict
}

func (l *Lease) Close() error {
	l.closeOnce.Do(func() {
		l.cancel(ErrClosed)
		timer := time.NewTimer(l.registry.config.OperationTimeout)
		defer timer.Stop()
		for _, done := range []chan struct{}{l.renewDone, l.watchDone} {
			select {
			case <-done:
			case <-timer.C:
				l.closeErr = ErrReleaseUnknown
				// Retain capacity while renewal or its watchdog is still alive.
				// This waiter is itself bounded by the reserved lease slot.
				go func() {
					<-l.renewDone
					<-l.watchDone
					l.finish()
				}()
				return
			}
		}
		// Renewal has joined before release, so it cannot overwrite our release.
		ctx, cancel := context.WithTimeout(context.Background(), l.registry.config.OperationTimeout)
		defer cancel()
		result := make(chan error, 1)
		go func() {
			err := l.release(ctx)
			l.finish()
			result <- err
		}()
		select {
		case err := <-result:
			if err != nil {
				l.closeErr = ErrReleaseUnknown
			}
		case <-ctx.Done():
			l.closeErr = ErrReleaseUnknown
		}
	})
	return l.closeErr
}

// finish has a single owner selected by Close: either the unjoined-renewal
// waiter or the release goroutine. It runs only after all provider work exits.
func (l *Lease) finish() {
	l.registry.unreserve()
	close(l.quiesced)
}

func (l *Lease) release(ctx context.Context) error {
	r := l.registry
	l.mu.Lock()
	reader := l.reader
	l.mu.Unlock()
	for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
		old, err := r.read(ctx, l.binding.Reference)
		if err != nil {
			return err
		}
		if err = bindingMatches(old.document, l.binding); err != nil {
			return err
		}
		index := -1
		for i, e := range old.document.Readers {
			if e.ID == reader.ID {
				index = i
				break
			}
		}
		if index < 0 {
			return nil
		} // Already expired/reclaimed; never adopt a slot.
		if old.document.Readers[index] != reader {
			return ErrLost
		}
		next := old.document
		if err = advance(&next); err != nil {
			return err
		}
		next.Readers = append(next.Readers[:index:index], next.Readers[index+1:]...)
		_, err = r.write(ctx, old, next)
		if errors.Is(err, ErrConflict) {
			continue
		}
		return err
	}
	return ErrConflict
}
