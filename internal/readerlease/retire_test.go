// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package readerlease

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRetireBlocksNewReadersButKeepsLiveLeaseRenewing(t *testing.T) {
	r, s, clock, b := testRegistry(t)
	prepare(t, r, b)
	ctx := context.Background()
	live := acquire(t, r, b)
	if err := r.Retire(ctx, b.Reference); err != nil {
		t.Fatal(err)
	}
	if err := r.Retire(ctx, b.Reference); err != nil {
		t.Fatal("retire must be idempotent", err)
	}
	if _, err := r.Acquire(ctx, b); !errors.Is(err, ErrRetired) {
		t.Fatal("retired generation admitted a new reader", err)
	}
	if err := r.Seal(ctx, b, strings.Repeat("c", 32)); !errors.Is(err, ErrRetired) {
		t.Fatal("retired generation accepted a seal", err)
	}
	inspection, err := r.Inspect(ctx, b.Reference)
	if err != nil || inspection.Quiescent() || inspection.MaybeLive != 1 {
		t.Fatalf("live lease not reported: %+v %v", inspection, err)
	}
	// The live reader keeps renewing after retirement.
	clock.Add(15 * time.Second)
	if err := live.renew(); err != nil {
		t.Fatal("live lease failed to renew after retirement", err)
	}
	if err := live.Check(); err != nil {
		t.Fatal(err)
	}
	_ = live.Close()
	inspection, err = r.Inspect(ctx, b.Reference)
	if err != nil || !inspection.Quiescent() || inspection.Version == "" {
		t.Fatalf("closed lease still blocks quiescence: %+v %v", inspection, err)
	}
	_ = s
}

func TestInspectTreatsCrashedLeaseAsLiveUntilExpiryPlusSkew(t *testing.T) {
	r, s, clock, b := testRegistry(t)
	prepare(t, r, b)
	ctx := context.Background()
	lease := acquire(t, r, b)
	// Simulate a crash: stop local renewal without releasing the remote entry.
	s.change(func(s *fakeStore) { s.casHook = func(context.Context, string, string, []byte) error { return ErrUnavailable } })
	lease.cancel(errors.New("crash"))
	if err := r.Retire(ctx, b.Reference); err == nil {
		t.Fatal("retire should fail while CAS is unavailable")
	}
	s.change(func(s *fakeStore) { s.casHook = nil })
	if err := r.Retire(ctx, b.Reference); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig("snapshots", "tenant")
	clock.Add(cfg.LeaseDuration)
	if got, err := r.Inspect(ctx, b.Reference); err != nil || got.Quiescent() {
		t.Fatalf("lease at expiry treated as quiescent: %+v %v", got, err)
	}
	clock.Add(cfg.ClockUncertainty * 2)
	if got, err := r.Inspect(ctx, b.Reference); err != nil || !got.Quiescent() {
		t.Fatalf("expired lease past skew margin still live: %+v %v", got, err)
	}
	// A fast provider clock means the lease may be still live: never quiescent early.
	s.change(func(s *fakeStore) { s.offset = -cfg.LeaseDuration })
	if got, err := r.Inspect(ctx, b.Reference); err != nil || got.Quiescent() {
		t.Fatalf("skewed provider clock allowed early quiescence: %+v %v", got, err)
	}
}

func TestInspectFailsClosedOnUncertainty(t *testing.T) {
	r, s, _, b := testRegistry(t)
	prepare(t, r, b)
	ctx := context.Background()
	if err := r.Retire(ctx, b.Reference); err != nil {
		t.Fatal(err)
	}
	s.change(func(s *fakeStore) { s.missingDate = true })
	if _, err := r.Inspect(ctx, b.Reference); !errors.Is(err, ErrClock) {
		t.Fatal("missing provider time must fail closed", err)
	}
	s.change(func(s *fakeStore) {
		s.missingDate = false
		s.getHook = func(context.Context, string) error { return errors.New("permission denied") }
	})
	if _, err := r.Inspect(ctx, b.Reference); err == nil {
		t.Fatal("unreadable registry must fail closed")
	}
	s.change(func(s *fakeStore) { s.getHook = nil })
	ref, err := r.Discover(ctx, b.Dataset, b.Generation)
	if err != nil || ref != b.Reference {
		t.Fatal("discover did not return stored incarnation", ref, err)
	}
	if _, err := r.Discover(ctx, b.Dataset, strings.Repeat("e", 32)); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
