// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"context"
	"time"

	api "github.com/SYNEHQ/kelvo-go/operations"
)

const cleanupLifetime = 5 * time.Second

// AcceptedCleanup retains one accepted physical DATA open for read cleanup.
// It contains no source credentials, TLS keys or database cancellation secrets.
// The trusted parent verifies Rabbit's signed receipt before registration.
type AcceptedCleanup struct {
	DataTicketSHA256      string    `json:"data_ticket_sha256"`
	AcceptanceID          string    `json:"acceptance_id"`
	AcceptedUntil         time.Time `json:"accepted_until"`
	CancellationStartedAt time.Time `json:"cancellation_started_at,omitempty"`
	CleanupUntil          time.Time `json:"cleanup_until,omitempty"`
}

func acceptedIdentity(digest, acceptance string) bool {
	if !api.ValidDigest(digest) || len(acceptance) != 32 {
		return false
	}
	for _, c := range acceptance {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func validateAcceptedCleanup(r Record) error {
	a := r.AcceptedCleanup
	if a == nil {
		if r.State == Cancelling || r.State == string(api.CleanupUnknown) {
			return ErrInvalid
		}
		return nil
	}
	if r.Kind.Mutating() || !acceptedIdentity(a.DataTicketSHA256, a.AcceptanceID) || !a.AcceptedUntil.After(r.CreatedAt) || a.AcceptedUntil.After(r.ExecuteBefore) || r.Binding.Validate() != nil || r.State == Queued || r.State == Assigned {
		return ErrInvalid
	}
	if a.CancellationStartedAt.IsZero() {
		if !a.CleanupUntil.IsZero() || r.State == Cancelling {
			return ErrInvalid
		}
		return nil
	}
	if a.CancellationStartedAt.Before(r.CreatedAt) || !a.CleanupUntil.After(a.CancellationStartedAt) || a.CleanupUntil.Sub(a.CancellationStartedAt) > cleanupLifetime || a.CleanupUntil.After(a.AcceptedUntil) || a.CleanupUntil.After(r.ExecuteBefore) {
		return ErrInvalid
	}
	return nil
}

// RegisterAcceptedCleanup is a durable barrier before giving a DATA stream to
// an adapter. A lost acknowledgement does not permit child dispatch. Only the
// original trusted parent may call it after signature and physical-pair checks.
// Repeated registration of the identical acceptance is safe; replacement is not.
func (s *Store) RegisterAcceptedCleanup(ctx context.Context, scope Scope, id string, binding Binding, digest, acceptance string, until time.Time) (Snapshot, error) {
	if !acceptedIdentity(digest, acceptance) || until.IsZero() {
		return Snapshot{}, ErrInvalid
	}
	return s.changeBound(ctx, scope, id, binding, func(r *Record, now time.Time) (bool, error) {
		if r.State != Running || r.Kind.Mutating() || !now.Before(until) || until.After(r.ExecuteBefore) {
			return false, ErrConflict
		}
		want := AcceptedCleanup{DataTicketSHA256: digest, AcceptanceID: acceptance, AcceptedUntil: until}
		if r.AcceptedCleanup != nil {
			if *r.AcceptedCleanup == want {
				return false, nil
			}
			return false, ErrConflict
		}
		r.AcceptedCleanup = &want
		return true, nil
	})
}

// CurrentCleanup reads the first cancellation cutoff. It cannot create or
// renew cleanup custody. The caller must also check the current worker lease,
// original certificate, source mapping, route and hard revocations.
func (s *Store) CurrentCleanup(ctx context.Context, scope Scope, id string, binding Binding, digest, acceptance string) (Snapshot, error) {
	if !acceptedIdentity(digest, acceptance) {
		return Snapshot{}, ErrInvalid
	}
	return s.changeBound(ctx, scope, id, binding, func(r *Record, now time.Time) (bool, error) {
		a := r.AcceptedCleanup
		if r.State != Cancelling || a == nil || a.DataTicketSHA256 != digest || a.AcceptanceID != acceptance || !now.Before(a.CleanupUntil) {
			return false, ErrConflict
		}
		return false, nil
	})
}

// CompleteCleanup records the trusted parent's observed source stop only after
// the original DATA operation and physical cleanup have joined. A successfully
// written cancel packet alone is not a confirmed source stop.
func (s *Store) CompleteCleanup(ctx context.Context, scope Scope, id string, binding Binding, digest, acceptance string, confirmed bool) (Snapshot, error) {
	if !acceptedIdentity(digest, acceptance) {
		return Snapshot{}, ErrInvalid
	}
	return s.changeBound(ctx, scope, id, binding, func(r *Record, now time.Time) (bool, error) {
		a := r.AcceptedCleanup
		if r.State != Cancelling || a == nil || a.DataTicketSHA256 != digest || a.AcceptanceID != acceptance {
			return false, ErrConflict
		}
		if confirmed {
			s.terminal(r, now, api.Failed, api.EffectNone, "CANCELLED")
		} else {
			s.cleanupUnknown(r, now)
		}
		return true, nil
	})
}
func (s *Store) cleanupUnknown(r *Record, now time.Time) {
	s.terminal(r, now, api.CleanupUnknown, api.EffectNone, "CLEANUP_UNKNOWN")
}
