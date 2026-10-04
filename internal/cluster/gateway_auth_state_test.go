// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/authstate"
)

type authStateFixture struct {
	admit      func(context.Context, authstate.Candidate) error
	close      func(context.Context) error
	admissions atomic.Int32
	closures   atomic.Int32
}

func (s *authStateFixture) Admit(ctx context.Context, candidate authstate.Candidate) error {
	s.admissions.Add(1)
	if s.admit != nil {
		return s.admit(ctx, candidate)
	}
	return nil
}
func (s *authStateFixture) Close(ctx context.Context) error {
	s.closures.Add(1)
	if s.close != nil {
		return s.close(ctx)
	}
	return nil
}

func authStateTestConfig() GatewayAuthenticationConfig {
	return GatewayAuthenticationConfig{KeysFile: "/private-key-fixture", ReloadInterval: time.Second, MinRevision: 1,
		State: &GatewayAuthenticationStateConfig{Directory: "/private-state-fixture", Scope: "gateway-a"}}
}

// No ticker runs unless the test starts it. Each operation still traverses the
// production parser, admission, publication and close paths.
func authStateLoader(t *testing.T, store *authStateFixture) (*gatewayAuthenticator, *atomic.Value) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	document := &atomic.Value{}
	document.Store(rotationDocument(t, 1, map[string][]string{"a": {rotationOld}, "b": {rotationOther}}))
	a := &gatewayAuthenticator{config: authStateTestConfig(), tenants: map[string]bool{"a": true, "b": true},
		ctx: ctx, cancel: cancel, done: make(chan struct{}), state: store,
		read: func(context.Context, string, int) ([]byte, error) {
			return append([]byte(nil), document.Load().([]byte)...), nil
		}}
	t.Cleanup(func() { _ = a.close() })
	if !a.applyRead(<-a.beginRead()) {
		t.Fatal("initial durable authority unavailable")
	}
	return a, document
}

func waitAuthState(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-deadline.C:
			t.Fatal("authentication state did not reach expected condition")
		case <-tick.C:
		}
	}
}

func TestGatewayAuthStateRotationPreservesOverlapUntilDurableAdmission(t *testing.T) {
	type blocked struct{ release chan struct{} }
	entered := make(chan blocked, 1)
	store := &authStateFixture{admit: func(ctx context.Context, c authstate.Candidate) error {
		if c.Revision == 1 {
			return nil
		}
		call := blocked{release: make(chan struct{})}
		entered <- call
		select {
		case <-call.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	a, document := authStateLoader(t, store)
	_, old, _ := a.lookup(rotationOld)
	_, other, _ := a.lookup(rotationOther)
	for _, revision := range []uint64{2, 3} {
		keys := []string{rotationOld, rotationNew}
		if revision == 3 {
			keys = []string{rotationNew}
		}
		document.Store(rotationDocument(t, revision, map[string][]string{"a": keys, "b": {rotationOther}}))
		originalDeadline := a.expiry()
		pending := a.beginRead()
		var call blocked
		select {
		case call = <-entered:
		case <-time.After(time.Second):
			t.Fatal("admission not entered")
		}
		if !a.expiry().Equal(originalDeadline) {
			t.Error("uncommitted rotation renewed authority")
		}
		if _, retained, ok := a.lookup(rotationOther); !ok || retained != other || other.Err() != nil {
			t.Error("unchanged key lost authority during persistence")
		}
		if revision == 2 {
			if _, retained, ok := a.lookup(rotationOld); !ok || retained != old {
				t.Error("overlap was canceled")
			}
			if _, _, ok := a.lookup(rotationNew); ok {
				t.Error("new key activated before durability")
			}
		} else if old.Err() == nil {
			t.Error("removed key survived pending durable admission")
		}
		close(call.release)
		if !a.applyRead(<-pending) {
			t.Fatal("durable rotation rejected")
		}
		if _, _, ok := a.lookup(rotationNew); !ok {
			t.Fatal("accepted key unavailable")
		}
		if _, retained, ok := a.lookup(rotationOther); !ok || retained != other {
			t.Fatal("rotation replaced unchanged context")
		}
	}
	// A caller cannot activate configured durable authority by using the old
	// memory-only publication helper without a successful admission result.
	if a.apply(rotationSet(t, 4, rotationOld), time.Now()) {
		t.Fatal("durable admission bypassed")
	}
}

func TestGatewayAuthStateBlockedAdmissionExpiresAndRejectsLateSuccess(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	store := &authStateFixture{admit: func(ctx context.Context, c authstate.Candidate) error {
		if c.Revision == 1 {
			return nil
		}
		entered <- ctx
		<-release // Deliberately ignore cancellation like a stuck fsync.
		return nil
	}}
	a, document := authStateLoader(t, store)
	_, active, _ := a.lookup(rotationOld)
	document.Store(rotationDocument(t, 2, map[string][]string{"a": {rotationOld, rotationNew}, "b": {rotationOther}}))
	a.config.ReloadInterval = 20 * time.Millisecond
	a.mu.Lock()
	a.validUntil = time.Now().Add(200 * time.Millisecond)
	a.mu.Unlock()
	go a.run()
	var operation context.Context
	select {
	case operation = <-entered:
	case <-time.After(time.Second):
		t.Fatal("admission not entered")
	}
	select {
	case <-active.Done():
	case <-time.After(time.Second):
		t.Fatal("blocked storage prevented background expiry")
	}
	if a.ready() || a.active(active) {
		t.Fatal("expired authority remained usable")
	}
	select {
	case <-operation.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("admission deadline missing")
	}
	if store.admissions.Load() != 2 {
		t.Fatal("replacement operation launched during blocked admission")
	}
	unblock()
	waitAuthState(t, func() bool { a.mu.Lock(); defer a.mu.Unlock(); return a.poisoned && !a.admitting })
	select {
	case <-entered:
		t.Fatal("poisoned storage admitted a replacement operation")
	case <-time.After(80 * time.Millisecond):
	}
	a.mu.Lock()
	revision := a.revision
	a.mu.Unlock()
	if revision != 1 || a.ready() {
		t.Fatal("late durable success activated expired candidate")
	}
	if err := a.close(); !errors.Is(err, authstate.ErrUncertain) {
		t.Fatal("uncertainty not propagated through close")
	}
}

func TestGatewayAuthStateRejectedCandidateRecoversButUncertaintyLatches(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejected", true: "uncertain"}[uncertain], func(t *testing.T) {
			store := &authStateFixture{admit: func(_ context.Context, c authstate.Candidate) error {
				if c.Revision != 2 {
					return nil
				}
				if uncertain {
					return authstate.ErrUncertain
				}
				return authstate.ErrRejected
			}}
			a, document := authStateLoader(t, store)
			_, old, _ := a.lookup(rotationOld)
			document.Store(rotationDocument(t, 2, map[string][]string{"a": {rotationOld, rotationNew}, "b": {rotationOther}}))
			if a.applyRead(<-a.beginRead()) || old.Err() == nil || a.ready() {
				t.Fatal("failure retained active authority")
			}
			document.Store(rotationDocument(t, 1, map[string][]string{"a": {rotationOld}, "b": {rotationOther}}))
			if recovered := a.applyRead(<-a.beginRead()); recovered == uncertain {
				t.Fatal("incorrect recovery after storage result")
			}
			want := int32(3)
			if uncertain {
				want = 2
			}
			if store.admissions.Load() != want {
				t.Fatal("poisoned storage was retried or rejected candidate could not recover")
			}
		})
	}
}

func TestGatewayAuthStateStartupDeadlineOwnsLateOpenAndRead(t *testing.T) {
	for _, blockedOpen := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "open"}[blockedOpen], func(t *testing.T) {
			release, closed := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			var reads atomic.Int32
			store := &authStateFixture{close: func(context.Context) error { close(closed); return nil }}
			opener := func(context.Context, string, authstate.Scope) (gatewayAuthenticationStore, error) {
				if blockedOpen {
					<-release
				}
				return store, nil
			}
			raw := rotationDocument(t, 1, map[string][]string{"a": {rotationOld}})
			reader := func(context.Context, string, int) ([]byte, error) {
				reads.Add(1)
				<-release
				return append([]byte(nil), raw...), nil
			}
			started := time.Now()
			a, err := newGatewayAuthenticatorWithState(authStateTestConfig(), map[string]bool{"a": true}, reader, opener)
			if a != nil || !errors.Is(err, errGatewayAuthUnavailable) || time.Since(started) > 3*time.Second {
				t.Fatal("startup failed to bound its original loader")
			}
			if store.closures.Load() != 0 {
				t.Fatal("startup released store while its loader was still executing")
			}
			unblock()
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("late startup loader leaked storage ownership")
			}
			if store.admissions.Load() != 0 || (blockedOpen && reads.Load() != 0) {
				t.Fatal("expired startup performed follow-up authority work")
			}
		})
	}
}

func TestGatewayAuthStateCloseCancelsAuthorityBeforeWaitingForStorage(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	store := &authStateFixture{admit: func(_ context.Context, c authstate.Candidate) error {
		if c.Revision == 1 {
			return nil
		}
		close(entered)
		<-release
		return nil
	}}
	a, document := authStateLoader(t, store)
	_, active, _ := a.lookup(rotationOld)
	document.Store(rotationDocument(t, 2, map[string][]string{"a": {rotationOld, rotationNew}, "b": {rotationOther}}))
	pending := a.beginRead()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("admission not entered")
	}
	result := make(chan error, 1)
	go func() { result <- a.close() }()
	select {
	case <-active.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown waited on storage before revoking keys")
	}
	select {
	case err := <-result:
		if !errors.Is(err, authstate.ErrUncertain) {
			t.Fatal("blocked close reported certainty")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close exceeded bound")
	}
	if store.closures.Load() != 0 {
		t.Fatal("closed storage before its sole loader quiesced")
	}
	unblock()
	if a.applyRead(<-pending) {
		t.Fatal("shutdown allowed late authority publication")
	}
	select {
	case <-a.closeDone:
	case <-time.After(time.Second):
		t.Fatal("late cleanup did not finish")
	}
	if store.closures.Load() != 1 {
		t.Fatal("store was not closed exactly once")
	}
}

func TestGatewayAuthStateGatewayClosePropagatesRedactedFailure(t *testing.T) {
	store := &authStateFixture{close: func(context.Context) error { return errors.New("private-storage-close-marker") }}
	a, _ := authStateLoader(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{ctx: ctx, cancel: cancel, auth: a}
	err := g.Close()
	if !errors.Is(err, authstate.ErrUncertain) || strings.Contains(err.Error(), "private-storage-close-marker") {
		t.Fatal("gateway lost uncertainty or exposed private close diagnostics")
	}
	if !errors.Is(g.Close(), authstate.ErrUncertain) || store.closures.Load() != 1 {
		t.Fatal("gateway close is not idempotent")
	}
}

func TestGatewayAuthStatePublicationRechecksOriginalDeadline(t *testing.T) {
	a, _ := authStateLoader(t, &authStateFixture{})
	_, active, _ := a.lookup(rotationOld)
	// The result was admitted, but publication now occurs past its read deadline
	// and before the longer authority lease would end. This is also the state
	// reached when applyRead was fresh before waiting for the publication mutex.
	started := time.Now().Add(-gatewayKeyReadTimeout - 100*time.Millisecond)
	if a.publish(rotationSet(t, 2, rotationOld, rotationNew), started, true) || active.Err() == nil || a.ready() {
		t.Fatal("publication admitted a durable result beyond its original read budget")
	}
	a.mu.Lock()
	revision := a.revision
	a.mu.Unlock()
	if revision != 1 {
		t.Fatal("late publication changed the accepted revision")
	}
}

func TestGatewayAuthStateOpenReadAndAdmissionShareOneDeadline(t *testing.T) {
	var deadlines []time.Time
	record := func(ctx context.Context) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("authority operation has no deadline")
		}
		deadlines = append(deadlines, deadline)
	}
	store := &authStateFixture{admit: func(ctx context.Context, _ authstate.Candidate) error { record(ctx); return nil }}
	opener := func(ctx context.Context, _ string, _ authstate.Scope) (gatewayAuthenticationStore, error) {
		record(ctx)
		return store, nil
	}
	raw := rotationDocument(t, 1, map[string][]string{"a": {rotationOld}})
	reader := func(ctx context.Context, _ string, _ int) ([]byte, error) {
		record(ctx)
		return append([]byte(nil), raw...), nil
	}
	config := authStateTestConfig()
	config.ReloadInterval = time.Minute
	a, err := newGatewayAuthenticatorWithState(config, map[string]bool{"a": true}, reader, opener)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.close(); err != nil {
		t.Fatal(err)
	}
	if len(deadlines) != 3 || !deadlines[0].Equal(deadlines[1]) || !deadlines[1].Equal(deadlines[2]) {
		t.Fatal("startup stages received separate authority budgets")
	}
}

func TestGatewayAuthStateOmissionPreservesMemoryOnlyShutdown(t *testing.T) {
	a := bareAuthenticator(t)
	if !a.apply(rotationSet(t, 1, rotationOld), time.Now()) {
		t.Fatal("initial memory authority unavailable")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	raw := rotationDocument(t, 2, map[string][]string{"a": {rotationOld}, "b": {rotationOther}})
	a.read = func(context.Context, string, int) ([]byte, error) {
		close(entered)
		<-release
		return append([]byte(nil), raw...), nil
	}
	pending := a.beginRead()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reader did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- a.close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal("memory-only shutdown acquired durable-state uncertainty")
		}
	case <-time.After(time.Second):
		t.Fatal("memory-only shutdown waited for a stuck private-file read")
	}
	unblock()
	if a.applyRead(<-pending) {
		t.Fatal("closed memory-only authority accepted late keys")
	}
}

func TestGatewayAuthStateBudgetStartsBeforeLoaderScheduling(t *testing.T) {
	var deadline time.Time
	store := &authStateFixture{admit: func(ctx context.Context, _ authstate.Candidate) error { deadline, _ = ctx.Deadline(); return nil }}
	a, _ := authStateLoader(t, store)
	result := <-a.beginRead()
	if !deadline.Equal(result.started.Add(gatewayKeyReadTimeout)) {
		t.Fatal("loader scheduling extended the authority operation deadline")
	}
	if !a.applyRead(result) {
		t.Fatal("fresh unchanged authority rejected")
	}
}
