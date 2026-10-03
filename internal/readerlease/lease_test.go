// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package readerlease

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReaderLeaseRenewPreservesOtherReadersAndExtendsConservativeCutoff(t *testing.T) {
	r, _, clock, b := testRegistry(t)
	prepare(t, r, b)
	first, second := acquire(t, r, b), acquire(t, r, b)
	oldCutoff := first.cutoff
	oldOther := second.reader
	clock.Add(20 * time.Second)
	if err := first.renew(); err != nil {
		t.Fatal(err)
	}
	if !first.cutoff.After(oldCutoff) || first.reader.Sequence != 2 {
		t.Fatal("renewal did not advance local fencing state")
	}
	if err := first.Check(); err != nil {
		t.Fatal(err)
	}
	d := contents(t, r, b)
	if len(d.Readers) != 2 || d.Readers[1] != oldOther {
		t.Fatal("renewal changed unrelated reader")
	}
	if !first.cutoff.Before(first.reader.ExpiresAt) {
		t.Fatal("local deadline omitted provider uncertainty")
	}
}

func TestReaderLeaseCheckAndRenewalPublicationAreSerialized(t *testing.T) {
	r, s, clock, b := testRegistry(t)
	checkEntered, resumeCheck := make(chan struct{}), make(chan struct{})
	var blockNextClock atomic.Bool
	r.now = func() time.Time {
		if blockNextClock.CompareAndSwap(true, false) {
			close(checkEntered)
			<-resumeCheck
		}
		return clock.Now()
	}
	prepare(t, r, b)
	l := acquire(t, r, b)
	oldCutoff := l.cutoff
	clock.Add(20 * time.Second)
	casCommitted, resumeCAS := make(chan struct{}), make(chan struct{})
	var checkOnce, casOnce sync.Once
	unblockCheck := func() { checkOnce.Do(func() { close(resumeCheck) }) }
	unblockCAS := func() { casOnce.Do(func() { close(resumeCAS) }) }
	t.Cleanup(func() { unblockCheck(); unblockCAS() })
	s.change(func(s *fakeStore) {
		s.afterCAS = func(context.Context, string) error {
			close(casCommitted)
			<-resumeCAS
			return nil
		}
	})
	renewed := make(chan error, 1)
	go func() { renewed <- l.renew() }()
	select {
	case <-casCommitted:
	case <-time.After(time.Second):
		t.Fatal("renewal did not reach publication barrier")
	}
	s.change(func(s *fakeStore) { s.afterCAS = nil })
	blockNextClock.Store(true)
	checked := make(chan error, 1)
	go func() { checked <- l.Check() }()
	select {
	case <-checkEntered:
	case <-time.After(time.Second):
		t.Fatal("checker did not reach expiry barrier")
	}
	unblockCAS()
	select {
	case err := <-renewed:
		t.Fatal("renewal published while an older expiry decision was unresolved", err)
	case <-time.After(50 * time.Millisecond):
	}
	unblockCheck()
	for name, result := range map[string]<-chan error{"check": checked, "renew": renewed} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(name, err)
			}
		case <-time.After(time.Second):
			t.Fatal(name, "did not finish after barrier release")
		}
	}
	if !l.cutoff.After(oldCutoff) || l.Check() != nil {
		t.Fatal("confirmed renewal was lost after a concurrent checker")
	}
}

func TestReaderLeaseRenewalLossCancelsWithoutResurrection(t *testing.T) {
	for _, kind := range []string{"response-loss", "expiry-during-cas", "missing-date", "forward-clock-jump", "backward-clock-jump", "missing-registry", "entry-replaced"} {
		t.Run(kind, func(t *testing.T) {
			r, s, clock, b := testRegistry(t)
			prepare(t, r, b)
			l := acquire(t, r, b)
			clock.Add(20 * time.Second)
			key, _ := r.key(b.Reference)
			s.change(func(s *fakeStore) {
				switch kind {
				case "response-loss":
					s.afterCAS = func(context.Context, string) error { return io.ErrUnexpectedEOF }
				case "expiry-during-cas":
					s.casHook = func(context.Context, string, string, []byte) error { clock.Add(time.Minute); return nil }
				case "missing-date":
					s.missingDate = true
				case "forward-clock-jump":
					s.offset = 30 * time.Second
				case "backward-clock-jump":
					s.offset = -30 * time.Second
				case "missing-registry":
					delete(s.objects, key)
				case "entry-replaced":
					o := s.objects[key]
					doc, err := decode(o.data, r.config)
					if err != nil {
						t.Fatal(err)
					}
					doc.Readers[0].Owner = strings.Repeat("f", 64)
					doc.Sequence++
					o.data, err = encode(doc, r.config)
					if err != nil {
						t.Fatal(err)
					}
					o.version = "replacement"
					s.objects[key] = o
				}
			})
			if err := l.renew(); err == nil {
				t.Fatal("failed renewal succeeded")
			}
			if l.Check() == nil || context.Cause(l.Context()) == nil {
				t.Fatal("lost renewal did not cancel consumer")
			}
			if kind == "expiry-during-cas" && !errors.Is(l.Check(), ErrExpired) {
				t.Fatal(l.Check())
			}
			s.change(func(s *fakeStore) { s.afterCAS = nil; s.casHook = nil; s.missingDate = false; s.offset = 0 })
			if l.Check() == nil {
				t.Fatal("restored provider revived old reader")
			}
			if kind == "response-loss" {
				if len(contents(t, r, b).Readers) != 1 {
					t.Fatal("ambiguous renewal erased durable pin")
				}
				if !errors.Is(l.Close(), ErrReleaseUnknown) {
					t.Fatal("unconfirmed renewed sequence was released")
				}
			}
		})
	}
}

func TestReaderLeaseRestartCannotAdoptOrReleaseOldIncarnation(t *testing.T) {
	r, s, clock, b := testRegistry(t)
	prepare(t, r, b)
	old := acquire(t, r, b)
	clock.Add(r.config.LeaseDuration + 10*time.Second)
	if !errors.Is(old.Check(), ErrExpired) {
		t.Fatal(old.Check())
	}
	restarted, err := New(s, r.config)
	if err != nil {
		t.Fatal(err)
	}
	restarted.now = clock.Now
	next := acquire(t, restarted, b)
	if next.reader.Owner == old.reader.Owner || next.reader.ID == old.reader.ID {
		t.Fatal("restart reused lease identity")
	}
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	d := contents(t, restarted, b)
	if len(d.Readers) != 1 || d.Readers[0] != next.reader {
		t.Fatal("old release removed restarted reader")
	}
	if err = next.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestReaderLeaseParentCancellationAndDeadline(t *testing.T) {
	r, s, _, b := testRegistry(t)
	prepare(t, r, b)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := s.calls.Load()
	if l, err := r.Acquire(ctx, b); l != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(l, err)
	}
	if s.calls.Load() != calls {
		t.Fatal("cancelled acquire reached provider")
	}
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
			}
			defer cancel()
			l, err := r.Acquire(ctx, b)
			if err != nil {
				t.Fatal(err)
			}
			if !deadline {
				cancel()
			}
			select {
			case <-l.Context().Done():
			case <-time.After(time.Second):
				t.Fatal("parent cancellation not observed")
			}
			if l.Check() == nil {
				t.Fatal("cancelled reader accepted")
			}
			if err = l.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func shortRegistry(t *testing.T) (*Registry, *fakeStore, Binding) {
	t.Helper()
	r, s, _, b := testRegistry(t)
	r.now = time.Now
	s.now = time.Now
	r.config.LeaseDuration = time.Second
	r.config.OperationTimeout = 50 * time.Millisecond
	r.config.ClockUncertainty = 20 * time.Millisecond
	r.config.RenewInterval = 200 * time.Millisecond
	r.config.MaxLeases = 1
	r.leases = make(chan struct{}, r.config.MaxLeases)
	if err := r.config.validate(); err != nil {
		t.Fatal(err)
	}
	prepare(t, r, b)
	return r, s, b
}

func TestReaderLeaseWatchdogAndCloseBoundStuckRenewal(t *testing.T) {
	r, s, b := shortRegistry(t)
	l := acquire(t, r, b)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.change(func(s *fakeStore) {
		s.casHook = func(context.Context, string, string, []byte) error {
			once.Do(func() { close(entered) })
			<-release
			return nil
		}
	})
	defer func() {
		close(release)
		_ = l.Close()
		waitQuiesced(t, l)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("renewal did not start")
	}
	select {
	case <-l.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("stuck provider defeated lease cutoff")
	}
	if !errors.Is(l.Check(), ErrExpired) {
		t.Fatal(l.Check())
	}
	start := time.Now()
	if !errors.Is(l.Close(), ErrReleaseUnknown) {
		t.Fatal("stuck renewal was reported released")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Close waited for non-cooperative renewal")
	}
	if len(contents(t, r, b).Readers) != 1 {
		t.Fatal("Close removed a pin while renewal was unjoined")
	}
	assertLocalCapacityHeld(t, r, s, b)
}

func TestReaderLeaseCloseBoundsStuckReleaseAndDoesNotExposeErrorText(t *testing.T) {
	r, s, b := shortRegistry(t)
	l := acquire(t, r, b)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.change(func(s *fakeStore) {
		s.getHook = func(context.Context, string) error {
			once.Do(func() { close(entered) })
			<-release
			return errors.New("private provider credentials")
		}
	})
	defer func() {
		close(release)
		_ = l.Close()
		waitQuiesced(t, l)
	}()
	start := time.Now()
	err := l.Close()
	if !errors.Is(err, ErrReleaseUnknown) || strings.Contains(err.Error(), "private") {
		t.Fatal(err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Close waited for non-cooperative release")
	}
	select {
	case <-entered:
	default:
		t.Fatal("release was not attempted")
	}
	assertLocalCapacityHeld(t, r, s, b)
}

func waitQuiesced(t *testing.T, l *Lease) {
	t.Helper()
	select {
	case <-l.quiesced:
	case <-time.After(time.Second):
		t.Error("lease work did not join after provider was unblocked")
	}
	if len(l.registry.leases) != 0 {
		t.Error("quiesced lease retained local capacity")
	}
}

func assertLocalCapacityHeld(t *testing.T, r *Registry, s *fakeStore, b Binding) {
	t.Helper()
	before := s.calls.Load()
	for range 128 {
		if l, err := r.Acquire(context.Background(), b); l != nil || !errors.Is(err, ErrCapacity) {
			t.Fatal("stalled work did not retain local capacity", l, err)
		}
	}
	if s.calls.Load() != before || len(r.leases) != 1 {
		t.Fatal("local capacity refusal reached provider or changed reservations")
	}
}

func TestReaderRegistryLocalCapacityCoversDifferentGenerations(t *testing.T) {
	r, s, b := shortRegistry(t)
	other := b
	other.Generation = strings.Repeat("d", 32)
	prepare(t, r, other)
	l := acquire(t, r, b)
	assertLocalCapacityHeld(t, r, s, other)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	waitQuiesced(t, l)
	next := acquire(t, r, other)
	if err := next.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestReaderRegistryBlockedAcquireReservesCapacityBeforeProviderIO(t *testing.T) {
	r, s, b := shortRegistry(t)
	entered, unblock := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once sync.Once
	s.change(func(s *fakeStore) {
		s.getHook = func(context.Context, string) error {
			once.Do(func() { close(entered) })
			<-unblock
			return nil
		}
	})
	result := make(chan error, 1)
	go func() {
		l, err := r.Acquire(ctx, b)
		if l != nil {
			_ = l.Close()
			err = errors.New("cancelled acquisition granted reader")
		}
		result <- err
	}()
	defer func() {
		cancel()
		close(unblock)
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("acquisition did not join after provider was unblocked")
		}
		if len(r.leases) != 0 {
			t.Error("failed acquisition retained local capacity")
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("acquisition did not reach provider")
	}
	assertLocalCapacityHeld(t, r, s, b)
	cancel()
	// Cancellation cannot free a slot while provider work still ignores it.
	assertLocalCapacityHeld(t, r, s, b)
}

func TestReaderLeaseAcquireLateCommitDoesNotGrantExpiredReader(t *testing.T) {
	r, s, clock, b := testRegistry(t)
	prepare(t, r, b)
	s.change(func(s *fakeStore) {
		s.casHook = func(context.Context, string, string, []byte) error { clock.Add(r.config.LeaseDuration); return nil }
	})
	if l, err := r.Acquire(context.Background(), b); l != nil || err == nil {
		t.Fatal("late response granted reader", err)
	}
	s.change(func(s *fakeStore) { s.casHook = nil })
	if len(contents(t, r, b).Readers) != 1 {
		t.Fatal("ambiguous late commit was hidden")
	}
}

func TestReaderRegistryEncodedByteCapacityCannotBeBypassed(t *testing.T) {
	r, _, _, b := testRegistry(t)
	r.config.MaxBytes = 1024
	prepare(t, r, b)
	var leases []*Lease
	defer func() {
		for _, l := range leases {
			_ = l.Close()
		}
	}()
	for i := 0; i < r.config.MaxReaders; i++ {
		l, err := r.Acquire(context.Background(), b)
		if err != nil {
			if !errors.Is(err, ErrCapacity) {
				t.Fatal(err)
			}
			if len(leases) == 0 {
				t.Fatal("fixture cannot fit one reader")
			}
			return
		}
		leases = append(leases, l)
	}
	t.Fatal("encoded byte budget failed to cap readers")
}
