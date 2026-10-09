// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	api "github.com/SYNEHQ/kelvo-go/operations"
)

func cleanupRunning(t *testing.T) (*fixture, Snapshot, AcceptedCleanup) {
	t.Helper()
	f := newFixture(t)
	f.input.Request = api.Request{Version: 1, Kind: api.QueryRead, Connection: api.ConnectionRef{ID: "saved-a", Database: "app"}, Spec: api.Spec{Query: &api.QuerySpec{SQL: "SELECT value FROM rows"}}}
	f.input.RequestRef, _, _ = api.SealRequest(f.input.Request, "request-a")
	snap := f.running(t)
	a := AcceptedCleanup{DataTicketSHA256: strings.Repeat("c", 64), AcceptanceID: strings.Repeat("d", 32), AcceptedUntil: f.now.Add(30 * time.Second)}
	return f, snap, a
}
func registerCleanup(t *testing.T, f *fixture, s Snapshot, a AcceptedCleanup) Snapshot {
	t.Helper()
	out, err := f.store.RegisterAcceptedCleanup(context.Background(), f.scope, s.Record.ID, f.binding, a.DataTicketSHA256, a.AcceptanceID, a.AcceptedUntil)
	if err != nil {
		t.Fatal("register accepted source", err)
	}
	return out
}
func TestAcceptedCleanupCancellationDeniesExecutionAndDoesNotExtend(t *testing.T) {
	f, s, a := cleanupRunning(t)
	s = registerCleanup(t, f, s, a)
	ctx := context.Background()
	first, err := f.store.Cancel(ctx, f.scope, s.Record.ID)
	if err != nil || first.Record.State != Cancelling || first.Record.Terminal() {
		t.Fatal("cancel returned terminal before source stop", err)
	}
	cutoff := first.Record.AcceptedCleanup.CleanupUntil
	if !cutoff.Equal(f.now.Add(5 * time.Second)) {
		t.Fatal("unexpected cleanup cutoff")
	}
	f.now = f.now.Add(time.Second)
	for i := 0; i < 3; i++ {
		next, err := f.store.Cancel(ctx, f.scope, s.Record.ID)
		if err != nil || !next.Record.AcceptedCleanup.CleanupUntil.Equal(cutoff) || next.Revision != first.Revision {
			t.Fatal("duplicate cancellation extended or rewrote cutoff", err)
		}
		lease, err := f.store.CurrentCleanup(ctx, f.scope, s.Record.ID, f.binding, a.DataTicketSHA256, a.AcceptanceID)
		if err != nil || !lease.Record.AcceptedCleanup.CleanupUntil.Equal(cutoff) {
			t.Fatal("cleanup lease failed", err)
		}
	}
	if _, err = f.store.Current(ctx, f.scope, s.Record.ID, f.binding); !errors.Is(err, ErrConflict) {
		t.Fatal("ordinary execution authority survived cancellation", err)
	}
	if _, err = f.store.Renew(ctx, f.scope, s.Record.ID, f.binding); !errors.Is(err, ErrConflict) {
		t.Fatal("cancelled execution renewed", err)
	}
	if _, err = f.store.RegisterAcceptedCleanup(ctx, f.scope, s.Record.ID, f.binding, a.DataTicketSHA256, a.AcceptanceID, a.AcceptedUntil); !errors.Is(err, ErrConflict) {
		t.Fatal("cancelled source reopened", err)
	}
	receipt := api.Receipt{Version: 1, OperationID: s.Record.ID, RequestSHA256: s.Record.RequestSHA256, Outcome: api.Failed, Effect: api.EffectNone, ErrorCode: "CANCELLED"}
	if _, err = f.store.Complete(ctx, f.scope, s.Record.ID, f.binding, receipt); !errors.Is(err, ErrConflict) {
		t.Fatal("ordinary completion claimed cleanup", err)
	}
	done, err := f.store.CompleteCleanup(ctx, f.scope, s.Record.ID, f.binding, a.DataTicketSHA256, a.AcceptanceID, true)
	if err != nil || done.Record.Receipt == nil || done.Record.Receipt.ErrorCode != "CANCELLED" {
		t.Fatal("confirmed cleanup not recorded", err)
	}
}
func TestAcceptedCleanupRejectsWrongCustodyAndReplacement(t *testing.T) {
	for _, name := range []string{"worker", "owner", "claim", "digest", "acceptance", "replacement"} {
		t.Run(name, func(t *testing.T) {
			f, s, a := cleanupRunning(t)
			s = registerCleanup(t, f, s, a)
			ctx := context.Background()
			b := f.binding
			digest, acceptance := a.DataTicketSHA256, a.AcceptanceID
			switch name {
			case "worker":
				b.WorkerID = "worker-b"
			case "owner":
				b.Owner = strings.Repeat("e", 32)
			case "claim":
				b.Claim = strings.Repeat("e", 32)
			case "digest":
				digest = strings.Repeat("e", 64)
			case "acceptance", "replacement":
				acceptance = strings.Repeat("e", 32)
			}
			if name == "replacement" {
				if _, err := f.store.RegisterAcceptedCleanup(ctx, f.scope, s.Record.ID, b, digest, acceptance, a.AcceptedUntil); err == nil {
					t.Fatal("accepted source replaced")
				}
				return
			}
			if _, err := f.store.Cancel(ctx, f.scope, s.Record.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.CurrentCleanup(ctx, f.scope, s.Record.ID, b, digest, acceptance); err == nil {
				t.Fatal("wrong cleanup custody accepted")
			}
			if _, err := f.store.CompleteCleanup(ctx, f.scope, s.Record.ID, b, digest, acceptance, true); err == nil {
				t.Fatal("wrong cleanup completion accepted")
			}
		})
	}
}
func TestAcceptedCleanupCrashRecoveryAndExpiryStayUnknown(t *testing.T) {
	for _, name := range []string{"cancelled", "worker-expired", "session-expired", "unconfirmed"} {
		t.Run(name, func(t *testing.T) {
			f, s, a := cleanupRunning(t)
			if name == "session-expired" {
				a.AcceptedUntil = f.now.Add(2 * time.Second)
			}
			s = registerCleanup(t, f, s, a)
			ctx := context.Background()
			if name == "cancelled" || name == "unconfirmed" {
				if _, err := f.store.Cancel(ctx, f.scope, s.Record.ID); err != nil {
					t.Fatal(err)
				}
			}
			if name == "unconfirmed" {
				if _, err := f.store.CompleteCleanup(ctx, f.scope, s.Record.ID, f.binding, a.DataTicketSHA256, a.AcceptanceID, false); err != nil {
					t.Fatal(err)
				}
			} else {
				// Reconstruct the store from retained bytes to model a worker/gateway crash.
				restarted, err := New(f.backend, f.store.policy)
				if err != nil {
					t.Fatal(err)
				}
				f.store = restarted
				f.store.now = func() time.Time { return f.now }
				if name == "cancelled" {
					f.now = f.now.Add(5 * time.Second)
				} else if name == "session-expired" {
					f.now = f.now.Add(2 * time.Second)
				} else {
					f.now = f.now.Add(10 * time.Second)
				}
				if err := f.store.RecoverShard(ctx, 0); err != nil {
					t.Fatal(err)
				}
			}
			got, err := f.store.Get(ctx, f.scope, s.Record.ID)
			if err != nil || got.Record.State != string(api.CleanupUnknown) || got.Record.Receipt.ErrorCode != "CLEANUP_UNKNOWN" || got.Record.Receipt.Effect != api.EffectNone {
				t.Fatal("unconfirmed source stop became confirmed", got.Record.State, err)
			}
			if _, err := f.store.CurrentCleanup(ctx, f.scope, s.Record.ID, f.binding, a.DataTicketSHA256, a.AcceptanceID); err == nil {
				t.Fatal("expired cleanup authority survived")
			}
			if _, err := f.store.CompleteCleanup(ctx, f.scope, s.Record.ID, f.binding, a.DataTicketSHA256, a.AcceptanceID, true); err == nil {
				t.Fatal("late cleanup rewrote terminal uncertainty")
			}
		})
	}
}
func TestAcceptedCleanupRegistrationCancelRace(t *testing.T) {
	for range 32 {
		f, s, a := cleanupRunning(t)
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = f.store.Cancel(ctx, f.scope, s.Record.ID) }()
		go func() {
			defer wg.Done()
			_, _ = f.store.RegisterAcceptedCleanup(ctx, f.scope, s.Record.ID, f.binding, a.DataTicketSHA256, a.AcceptanceID, a.AcceptedUntil)
		}()
		wg.Wait()
		got, err := f.store.Get(ctx, f.scope, s.Record.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Record.State != Cancelling && !(got.Record.Terminal() && got.Record.AcceptedCleanup == nil) {
			t.Fatal("cancel race left executable source", got.Record.State)
		}
		if _, err := f.store.Current(ctx, f.scope, s.Record.ID, f.binding); err == nil {
			t.Fatal("cancel race retained execution authority")
		}
	}
}
func TestAcceptedCleanupCutoffUsesOriginalBounds(t *testing.T) {
	f, s, a := cleanupRunning(t)
	a.AcceptedUntil = f.now.Add(2 * time.Second)
	s = registerCleanup(t, f, s, a)
	got, err := f.store.Cancel(context.Background(), f.scope, s.Record.ID)
	if err != nil || !got.Record.AcceptedCleanup.CleanupUntil.Equal(a.AcceptedUntil) {
		t.Fatal("cleanup extended original source session", err)
	}
}
func TestAcceptedCleanupLostRegistrationAcknowledgementDoesNotAuthorize(t *testing.T) {
	f, s, a := cleanupRunning(t)
	f.backend.loseNextAck = true
	if _, err := f.store.RegisterAcceptedCleanup(context.Background(), f.scope, s.Record.ID, f.binding, a.DataTicketSHA256, a.AcceptanceID, a.AcceptedUntil); !errors.Is(err, ErrUnavailable) {
		t.Fatal("unknown registration acknowledged", err)
	}
}
