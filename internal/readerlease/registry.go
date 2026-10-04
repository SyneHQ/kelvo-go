// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package readerlease

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"time"
)

type Registry struct {
	store  Store
	config Config
	owner  string
	now    func() time.Time
	leases chan struct{}
}

func New(store Store, c Config) (*Registry, error) {
	if nilStore(store) || c.validate() != nil {
		return nil, ErrInvalid
	}
	owner, err := randomToken()
	if err != nil {
		return nil, err
	}
	return &Registry{store: store, config: c, owner: owner, now: time.Now, leases: make(chan struct{}, c.MaxLeases)}, nil
}

func nilStore(store Store) bool {
	if store == nil {
		return true
	}
	v := reflect.ValueOf(store)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func (r *Registry) reserve(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	select {
	case r.leases <- struct{}{}:
		return nil
	default:
		return ErrCapacity
	}
}

func (r *Registry) unreserve() { <-r.leases }

func (r *Registry) key(ref Reference) (string, error) {
	if !validReference(ref) || ref.Tenant != r.config.Tenant {
		return "", ErrBinding
	}
	return r.config.Prefix + "/" + ref.Tenant + "/" + ref.Dataset + "/reader-leases/" + ref.Generation + ".yml", nil
}

type observation struct {
	lower, upper, local time.Time
}

func (o observation) at(now time.Time) (time.Time, time.Time, error) {
	elapsed := now.Sub(o.local)
	if elapsed < 0 {
		return time.Time{}, time.Time{}, ErrClock
	}
	return o.lower.Add(elapsed), o.upper.Add(elapsed), nil
}
func (r *Registry) observe(info Metadata, before, after time.Time) (observation, error) {
	roundtrip := after.Sub(before)
	if !validVersion(info.Version) {
		return observation{}, ErrCorrupt
	}
	if info.ServerTime.IsZero() || roundtrip < 0 || roundtrip > r.config.OperationTimeout {
		return observation{}, ErrClock
	}
	return observation{lower: info.ServerTime.Add(-r.config.ClockUncertainty),
		upper: info.ServerTime.Add(roundtrip + r.config.ClockUncertainty), local: after}, nil
}
func compatibleClock(old, next observation) error {
	lower, upper, err := old.at(next.local)
	if err != nil || next.lower.After(upper) || next.upper.Before(lower) {
		return ErrClock
	}
	return nil
}

type state struct {
	document document
	version  string
	clock    observation
}

func (r *Registry) read(parent context.Context, ref Reference) (state, error) {
	key, err := r.key(ref)
	if err != nil {
		return state{}, err
	}
	ctx, cancel := context.WithTimeout(parent, r.config.OperationTimeout)
	defer cancel()
	if err := context.Cause(ctx); err != nil {
		return state{}, err
	}
	before := r.now()
	body, info, err := r.store.Get(ctx, key)
	if body != nil {
		defer body.Close()
	}
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return state{}, cause
		}
		if errors.Is(err, ErrNotFound) {
			return state{}, ErrNotFound
		}
		return state{}, ErrUnavailable
	}
	if body == nil || info.Size <= 0 || info.Size > int64(r.config.MaxBytes) {
		return state{}, ErrCorrupt
	}
	raw, err := io.ReadAll(io.LimitReader(body, int64(r.config.MaxBytes)+1))
	if cause := context.Cause(ctx); cause != nil {
		return state{}, cause
	}
	if err != nil {
		return state{}, ErrUnavailable
	}
	if len(raw) > r.config.MaxBytes || len(raw) != int(info.Size) {
		return state{}, ErrCorrupt
	}
	clock, err := r.observe(info, before, r.now())
	if err != nil {
		return state{}, err
	}
	doc, err := decode(raw, r.config)
	if err != nil {
		return state{}, err
	}
	if doc.Reference != ref {
		return state{}, ErrBinding
	}
	return state{document: doc, version: info.Version, clock: clock}, nil
}

func (r *Registry) write(parent context.Context, old state, next document) (state, error) {
	key, err := r.key(next.Reference)
	if err != nil {
		return state{}, err
	}
	raw, err := encode(next, r.config)
	if err != nil {
		return state{}, err
	}
	ctx, cancel := context.WithTimeout(parent, r.config.OperationTimeout)
	defer cancel()
	if err := context.Cause(ctx); err != nil {
		return state{}, err
	}
	before := r.now()
	info, err := r.store.CompareAndSwap(ctx, key, old.version, raw)
	if cause := context.Cause(ctx); cause != nil {
		return state{}, cause
	}
	if err != nil {
		if errors.Is(err, ErrConflict) {
			return state{}, ErrConflict
		}
		return state{}, ErrUnavailable
	}
	if info.Size != int64(len(raw)) {
		return state{}, ErrCorrupt
	}
	clock, err := r.observe(info, before, r.now())
	if err != nil {
		return state{}, err
	}
	if old.version != "" {
		if info.Version == old.version {
			return state{}, ErrCorrupt
		}
		if err = compatibleClock(old.clock, clock); err != nil {
			return state{}, err
		}
	}
	return state{document: next, version: info.Version, clock: clock}, nil
}

func advance(d *document) error {
	if d.Sequence == math.MaxUint64 {
		return ErrCorrupt
	}
	d.Sequence++
	return nil
}
func writerValid(owner string) bool { return hexToken(owner, 32) || hexToken(owner, 64) }

// Stage creates protection metadata before generation upload. Staged state does
// not expire or become collectible here. Retry only with the same reference and
// writer fence. Existing sealed state is never reverted by an idempotent retry.
func (r *Registry) Stage(ctx context.Context, ref Reference, writerOwner string) error {
	if _, err := r.key(ref); err != nil {
		return err
	}
	if !writerValid(writerOwner) {
		return ErrInvalid
	}
	for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
		old, err := r.read(ctx, ref)
		if err == nil {
			if old.document.WriterOwner != writerOwner {
				return ErrFence
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		next := document{Version: 1, Reference: ref, Sequence: 1, WriterOwner: writerOwner}
		_, err = r.write(ctx, state{}, next)
		if errors.Is(err, ErrConflict) {
			continue
		}
		return err
	}
	return ErrConflict
}

// Seal fixes the complete immutable content binding before manifest publication.
// The existing writer fence must still be held by the caller at this boundary.
func (r *Registry) Seal(ctx context.Context, binding Binding, writerOwner string) error {
	if !hexToken(binding.ContentSHA256, 64) || !writerValid(writerOwner) {
		return ErrInvalid
	}
	for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
		old, err := r.read(ctx, binding.Reference)
		if err != nil {
			return err
		}
		if old.document.WriterOwner != writerOwner {
			return ErrFence
		}
		if old.document.Sealed {
			if old.document.ContentSHA256 != binding.ContentSHA256 {
				return ErrBinding
			}
			return nil
		}
		next := old.document
		if err = advance(&next); err != nil {
			return err
		}
		next.Sealed = true
		next.ContentSHA256 = binding.ContentSHA256
		_, err = r.write(ctx, old, next)
		if errors.Is(err, ErrConflict) {
			continue
		}
		return err
	}
	return ErrConflict
}

func bindingMatches(d document, binding Binding) error {
	if d.Reference != binding.Reference {
		return ErrBinding
	}
	if !d.Sealed {
		return ErrUnsealed
	}
	if d.ContentSHA256 != binding.ContentSHA256 {
		return ErrBinding
	}
	return nil
}

// Acquire returns only after a confirmed conditional write and conservative
// deadline check. Any ambiguous failure may leave a retained pin, but never
// grants a reader permission to start. A missing registry fails closed. Local
// capacity is reserved before provider I/O and held until failed acquisition
// returns or the successful lease is closed and its background work has exited.
func (r *Registry) Acquire(ctx context.Context, binding Binding) (*Lease, error) {
	return r.AcquireWithLifetime(ctx, ctx, binding)
}

// AcquireWithLifetime separates acquiring a pin from the custody of a confirmed
// pin. Either context cancels acquisition. After the final confirmation check,
// only custodyCtx controls renewal; cancelling acquireCtx cannot drop a pin
// while its consumer is still being cleaned up.
//
// This is a trusted-owner API. The caller must separately apply the request
// deadline to consumers, retain bounded custody until those consumers stop, and
// call Close even after request cancellation. It does not prove consumer exit
// or authorize deletion. No acquireCtx values are retained by the returned Lease
// unless the caller also passes that context as custodyCtx.
func (r *Registry) AcquireWithLifetime(acquireCtx, custodyCtx context.Context, binding Binding) (*Lease, error) {
	if acquireCtx == nil || custodyCtx == nil {
		return nil, ErrInvalid
	}
	if !hexToken(binding.ContentSHA256, 64) {
		return nil, ErrInvalid
	}
	if _, err := r.key(binding.Reference); err != nil {
		return nil, err
	}
	if err := context.Cause(acquireCtx); err != nil {
		return nil, err
	}
	if err := context.Cause(custodyCtx); err != nil {
		return nil, err
	}
	// Even acquisition cancellation callbacks belong to bounded custody. A
	// capacity refusal must not create work that can outlive the refused call.
	if err := r.reserve(acquireCtx); err != nil {
		return nil, err
	}
	operation, cancel := context.WithCancelCause(acquireCtx)
	var ctx context.Context = operation
	if deadline, ok := custodyCtx.Deadline(); ok {
		if current, bounded := acquireCtx.Deadline(); !bounded || deadline.Before(current) {
			// Cancellation still comes from custodyCtx, preserving its cause.
			// Expose its earlier deadline to providers without a second timer.
			ctx = acquisitionDeadline{Context: operation, deadline: deadline}
		}
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(custodyCtx, func() {
		defer close(callbackDone)
		cancel(context.Cause(custodyCtx))
	})
	joined := false
	join := func() {
		if joined {
			return
		}
		joined = true
		if !stop() {
			<-callbackDone
		}
	}
	transferred := false
	defer func() {
		// Join even a cancellation callback that started before stop. On a
		// failed acquisition, no callback may outlive its reserved capacity.
		join()
		cancel(context.Canceled)
		if !transferred {
			r.unreserve()
		}
	}()
	id, err := randomToken()
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
		old, err := r.read(ctx, binding.Reference)
		if err != nil {
			return nil, err
		}
		if err = bindingMatches(old.document, binding); err != nil {
			return nil, err
		}
		lower, _, err := old.clock.at(r.now())
		if err != nil {
			return nil, err
		}
		next := old.document
		next.Readers = make([]entry, 0, len(old.document.Readers)+1)
		for _, reader := range old.document.Readers {
			if reader.ExpiresAt.After(lower) {
				next.Readers = append(next.Readers, reader)
			}
			if reader.ID == id {
				return nil, ErrConflict
			}
		}
		if len(next.Readers) >= r.config.MaxReaders {
			return nil, ErrCapacity
		}
		reader := entry{ID: id, Owner: r.owner, Sequence: 1, RenewedAt: lower.UTC(), ExpiresAt: lower.Add(r.config.LeaseDuration).UTC()}
		next.Readers = append(next.Readers, reader)
		if err = advance(&next); err != nil {
			return nil, err
		}
		written, err := r.write(ctx, old, next)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return nil, err
		}
		// Detach acquisition-only cancellation work before transferring custody.
		// A concurrent custody cancellation is also observed by newLease itself.
		join()
		if err := context.Cause(acquireCtx); err != nil {
			return nil, err
		}
		lease, err := newLease(custodyCtx, r, binding, reader, written.clock)
		if err != nil {
			return nil, err
		}
		if err := lease.Check(); err != nil {
			lease.cancel(err)
			return nil, err
		}
		// This check is the transfer boundary: later request cancellation must
		// stop the consumer, while the confirmed pin remains in caller custody.
		if err := context.Cause(acquireCtx); err != nil {
			lease.cancel(err)
			return nil, err
		}
		lease.start()
		transferred = true
		return lease, nil
	}
	return nil, ErrConflict
}

// The operation's Done is cancelled by the custody callback. Reporting the
// earlier custody deadline also lets a provider honor Deadline directly.
type acquisitionDeadline struct {
	context.Context
	deadline time.Time
}

func (c acquisitionDeadline) Deadline() (time.Time, bool) { return c.deadline, true }
