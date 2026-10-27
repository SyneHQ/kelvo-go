// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package readerlease

import (
	"context"
	"errors"
	"strings"
)

// ErrRetired is returned by Acquire and Seal after Retire committed.
var ErrRetired = errors.New("reader registry generation is retired")

// Retire durably and permanently blocks new acquisitions and sealing for one
// generation. Leases that already exist keep renewing until they close or
// expire, so a reader that started before retirement keeps reading. Retire is
// idempotent. It is the collection fence: callers must retire only generations
// that no manifest references, and deletion must still wait for Inspect.
func (r *Registry) Retire(ctx context.Context, ref Reference) error {
	for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
		old, err := r.read(ctx, ref)
		if err != nil {
			return err
		}
		if old.document.Retired {
			return nil
		}
		next := old.document
		if err = advance(&next); err != nil {
			return err
		}
		next.Retired = true
		_, err = r.write(ctx, old, next)
		if errors.Is(err, ErrConflict) {
			continue
		}
		return err
	}
	return ErrConflict
}

// Inspection is one current read of a generation's registry document.
// MaybeLive counts readers whose expiry is not provably past: an entry is
// counted unless ExpiresAt plus ClockUncertainty is before the earliest
// possible provider time. Version is the exact CAS token of that read.
type Inspection struct {
	Retired, Sealed bool
	MaybeLive       int
	Version         string
}

// Quiescent is true only for a retired document with no reader that might
// still be live. Because Retire blocks new readers and an expired entry can
// never renew, a quiescent document stays quiescent.
func (i Inspection) Quiescent() bool { return i.Retired && i.MaybeLive == 0 }

// Inspect fails closed: any read, clock or decode uncertainty is an error and
// callers must not delete on error.
func (r *Registry) Inspect(ctx context.Context, ref Reference) (Inspection, error) {
	current, err := r.read(ctx, ref)
	if err != nil {
		return Inspection{}, err
	}
	lower, _, err := current.clock.at(r.now())
	if err != nil {
		return Inspection{}, err
	}
	result := Inspection{Retired: current.document.Retired, Sealed: current.document.Sealed, Version: current.version}
	for _, reader := range current.document.Readers {
		if !reader.ExpiresAt.Add(r.config.ClockUncertainty).Before(lower) {
			result.MaybeLive++
		}
	}
	return result, nil
}

// Discover reads the reference stored for an unreferenced generation, whose
// incarnation is not in any manifest. ErrNotFound means no document exists.
func (r *Registry) Discover(ctx context.Context, dataset, generation string) (Reference, error) {
	probe := Reference{Tenant: r.config.Tenant, Dataset: dataset, Generation: generation, Incarnation: strings.Repeat("0", 64)}
	current, err := r.readBound(ctx, probe, false)
	if err != nil {
		return Reference{}, err
	}
	return current.document.Reference, nil
}

// DocumentKey returns the registry document key for a reference in this
// Registry's scope, so collection can delete it with an exact version.
func (r *Registry) DocumentKey(ref Reference) (string, error) { return r.key(ref) }
