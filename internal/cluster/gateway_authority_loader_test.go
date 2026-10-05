// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/authfence"
	"github.com/SYNEHQ/kelvo-go/internal/authstate"
	"go.yaml.in/yaml/v3"
)

// Unit fakes return only zero (invalid) observations. Successful authority
// publication must be exercised through the real protocol fixture.
type gatewayAuthorityVerifierFixture struct {
	verify func(context.Context, authfence.Attempt, authfence.Document) (authfence.VerifiedObservation, error)
	close  func(context.Context) error
	quiet  chan struct{}
	calls  atomic.Int32
	closes atomic.Int32
}

func (v *gatewayAuthorityVerifierFixture) Verify(ctx context.Context, attempt authfence.Attempt, document authfence.Document) (authfence.VerifiedObservation, error) {
	v.calls.Add(1)
	if v.verify != nil {
		return v.verify(ctx, attempt, document)
	}
	return authfence.VerifiedObservation{}, authfence.ErrConflict
}

func (v *gatewayAuthorityVerifierFixture) Close(ctx context.Context) error {
	v.closes.Add(1)
	if v.close != nil {
		return v.close(ctx)
	}
	close(v.quiet)
	return nil
}

func (v *gatewayAuthorityVerifierFixture) Quiesced() <-chan struct{} { return v.quiet }

func gatewayAuthorityLoaderDocument(t *testing.T, version int, revision uint64, disabled bool) []byte {
	t.Helper()
	if version == 1 {
		return rotationDocument(t, revision, map[string][]string{"a": {rotationOld}, "b": {}})
	}
	principals := map[string]map[string][]string{"a": {"analyst": {rotationOld}}, "b": {}}
	if disabled {
		principals["a"] = map[string][]string{}
	}
	raw, err := yaml.Marshal(gatewayKeyDocument{Version: version, Revision: revision, Principals: principals})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func gatewayAuthorityLoaderFixture(t *testing.T, raw []byte, factory gatewayAuthorityVerifierFactory) (*gatewayAuthenticator, *authStateFixture) {
	t.Helper()
	cfg := gatewayAuthorityBindingFixture(t)
	authority, err := bindGatewayKeyAuthority(cfg)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := cfg.Authentication.normalized()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	store := &authStateFixture{}
	a := &gatewayAuthenticator{config: normalized, tenants: map[string]bool{"a": true, "b": true},
		ctx: ctx, cancel: cancel, done: make(chan struct{}), state: store, authority: authority, verifier: factory,
		read: func(context.Context, string, int) ([]byte, error) { return append([]byte(nil), raw...), nil }}
	t.Cleanup(func() { _ = a.close() })
	return a, store
}

func TestGatewayAuthorityRequiresV2AndOpaqueProofBeforeAdmission(t *testing.T) {
	for _, tc := range []struct {
		name     string
		version  int
		disabled bool
		calls    int32
	}{
		{"legacy document", 1, false, 0},
		{"missing proof", 2, false, 1},
		{"disabled v2 missing proof", 2, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verifier := &gatewayAuthorityVerifierFixture{quiet: make(chan struct{}),
				verify: func(context.Context, authfence.Attempt, authfence.Document) (authfence.VerifiedObservation, error) {
					return authfence.VerifiedObservation{}, nil
				}}
			var constructed atomic.Int32
			a, store := gatewayAuthorityLoaderFixture(t, gatewayAuthorityLoaderDocument(t, tc.version, 1, tc.disabled),
				func(authfence.Config) (gatewayAuthorityVerifier, error) { constructed.Add(1); return verifier, nil })
			result := <-a.beginRead()
			if a.applyRead(result) || a.ready() || result.admitted || store.admissions.Load() != 0 {
				t.Fatal("invalid document/proof reached admission or publication")
			}
			if constructed.Load() != tc.calls || verifier.calls.Load() != tc.calls || verifier.closes.Load() != tc.calls {
				t.Fatal("unexpected verification or unclosed protocol client")
			}
		})
	}
}

func TestGatewayAuthorityOneOriginalAttemptCoversLocalAndProtocolWork(t *testing.T) {
	var deadlines []time.Time
	record := func(ctx context.Context) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("missing original deadline")
		}
		deadlines = append(deadlines, deadline)
	}
	var observed authfence.Attempt
	var document authfence.Document
	verifier := &gatewayAuthorityVerifierFixture{quiet: make(chan struct{}),
		verify: func(ctx context.Context, attempt authfence.Attempt, expected authfence.Document) (authfence.VerifiedObservation, error) {
			record(ctx)
			observed, document = attempt, expected
			return authfence.VerifiedObservation{}, authfence.ErrConflict
		}}
	verifier.close = func(ctx context.Context) error { record(ctx); close(verifier.quiet); return nil }
	raw := gatewayAuthorityLoaderDocument(t, 2, 7, false)
	a, store := gatewayAuthorityLoaderFixture(t, raw, func(authfence.Config) (gatewayAuthorityVerifier, error) { return verifier, nil })
	a.state = nil
	a.openState = func(ctx context.Context, _ string, _ authstate.Scope) (gatewayAuthenticationStore, error) {
		record(ctx)
		return store, nil
	}
	a.read = func(ctx context.Context, _ string, _ int) ([]byte, error) {
		record(ctx)
		return append([]byte(nil), raw...), nil
	}
	result := <-a.beginRead()
	if len(deadlines) != 4 || observed.ID() == [16]byte{} || observed.Started() != result.started || observed.ID() != result.attempt.ID() {
		t.Fatal("loader replaced or omitted its original attempt")
	}
	for _, deadline := range deadlines {
		if deadline != observed.Deadline() || deadline != result.started.Add(gatewayKeyReadTimeout) {
			t.Fatal("a local/protocol stage renewed the deadline")
		}
	}
	expected, err := gatewayAuthorityDocument(result.set)
	if err != nil || !document.Equal(expected) || store.admissions.Load() != 0 || a.applyRead(result) {
		t.Fatal("proof mismatch reached admission/publication or document binding changed")
	}
}

func TestGatewayAuthorityLocallyRejectedCandidateNeverVerifies(t *testing.T) {
	for _, mode := range []string{"revision", "owner"} {
		t.Run(mode, func(t *testing.T) {
			var constructed atomic.Int32
			a, store := gatewayAuthorityLoaderFixture(t, gatewayAuthorityLoaderDocument(t, 2, 3, false),
				func(authfence.Config) (gatewayAuthorityVerifier, error) {
					constructed.Add(1)
					return nil, authfence.ErrUnavailable
				})
			if mode == "revision" {
				a.revision = 4
			} else {
				set := principalKeySet(t, 3, map[string][]string{"analyst": {rotationOld}}, map[string][]string{})
				a.bindings = make(map[[32]byte]string)
				for hash := range set.keys {
					a.bindings[hash] = "b\x00other"
				}
			}
			if a.applyRead(<-a.beginRead()) || constructed.Load() != 0 || store.admissions.Load() != 0 {
				t.Fatal("locally rejected candidate reached broker or durable admission")
			}
		})
	}
}

func TestGatewayAuthorityUnjoinedCloseRetainsLoaderAndWriter(t *testing.T) {
	for _, closeErr := range []error{nil, authfence.ErrUnknown} {
		name := "nil before join"
		if closeErr != nil {
			name = "unknown before join"
		}
		t.Run(name, func(t *testing.T) {
			entered, quiet := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(quiet) }) }
			verifier := &gatewayAuthorityVerifierFixture{quiet: quiet, close: func(context.Context) error { close(entered); return closeErr }}
			a, store := gatewayAuthorityLoaderFixture(t, gatewayAuthorityLoaderDocument(t, 2, 1, false),
				func(authfence.Config) (gatewayAuthorityVerifier, error) { return verifier, nil })
			t.Cleanup(release)
			pending := a.beginRead()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("protocol close not entered")
			}
			if result := <-a.beginRead(); result.err == nil || verifier.calls.Load() != 1 {
				t.Fatal("another loader replaced owned protocol cleanup")
			}
			a.startClose()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if !errors.Is(a.awaitClose(ctx), authstate.ErrUncertain) || store.closures.Load() != 0 || store.admissions.Load() != 0 {
				t.Fatal("public close released writer or admitted before protocol quiescence")
			}
			release()
			if a.applyRead(<-pending) {
				t.Fatal("uncertain late protocol result published")
			}
			select {
			case <-a.closeDone:
			case <-time.After(time.Second):
				t.Fatal("joined cleanup did not release its writer")
			}
			if store.closures.Load() != 1 || !errors.Is(a.closeErr, authstate.ErrUncertain) {
				t.Fatal("cleanup uncertainty or exclusive writer ownership was lost")
			}
		})
	}
}

func TestGatewayAuthorityBlockedVerifyCannotReleaseOrReplaceLoader(t *testing.T) {
	entered, release := make(chan context.Context, 1), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	verifier := &gatewayAuthorityVerifierFixture{quiet: make(chan struct{}),
		verify: func(ctx context.Context, _ authfence.Attempt, _ authfence.Document) (authfence.VerifiedObservation, error) {
			entered <- ctx
			<-release // Models owned work that cannot immediately honor cancellation.
			return authfence.VerifiedObservation{}, authfence.ErrUnavailable
		}}
	a, store := gatewayAuthorityLoaderFixture(t, gatewayAuthorityLoaderDocument(t, 2, 1, false),
		func(authfence.Config) (gatewayAuthorityVerifier, error) { return verifier, nil })
	t.Cleanup(unblock)
	pending := a.beginRead()
	var operation context.Context
	select {
	case operation = <-entered:
	case <-time.After(time.Second):
		t.Fatal("verification not entered")
	}
	if result := <-a.beginRead(); result.err == nil || verifier.calls.Load() != 1 {
		t.Fatal("blocked protocol work acquired a replacement loader")
	}
	a.startClose()
	select {
	case <-operation.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel original protocol context")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if !errors.Is(a.awaitClose(ctx), authstate.ErrUncertain) || store.closures.Load() != 0 {
		t.Fatal("shutdown released a writer still owned by blocked verification")
	}
	unblock()
	if a.applyRead(<-pending) || store.admissions.Load() != 0 {
		t.Fatal("late verification reached admission/publication")
	}
	select {
	case <-a.closeDone:
	case <-time.After(time.Second):
		t.Fatal("late verification cleanup did not finish")
	}
	if verifier.closes.Load() != 1 || store.closures.Load() != 1 {
		t.Fatal("protocol/store ownership was not released exactly once")
	}
}

func TestGatewayAuthorityDefiniteFailureUsesFreshClientOnEachAttempt(t *testing.T) {
	var clients []*gatewayAuthorityVerifierFixture
	a, store := gatewayAuthorityLoaderFixture(t, gatewayAuthorityLoaderDocument(t, 2, 1, false),
		func(authfence.Config) (gatewayAuthorityVerifier, error) {
			client := &gatewayAuthorityVerifierFixture{quiet: make(chan struct{})}
			clients = append(clients, client)
			return client, nil
		})
	for range 2 {
		if a.applyRead(<-a.beginRead()) {
			t.Fatal("definite broker conflict published")
		}
	}
	if len(clients) != 2 || clients[0] == clients[1] || store.admissions.Load() != 0 {
		t.Fatal("renewal reused a protocol lifetime or admitted a conflict")
	}
	for _, client := range clients {
		if client.calls.Load() != 1 || client.closes.Load() != 1 {
			t.Fatal("client was reused or not closed")
		}
	}
}

func TestGatewayAuthorityDirectPublicationCannotBypassProofOrResetFloors(t *testing.T) {
	a, _ := gatewayAuthorityLoaderFixture(t, gatewayAuthorityLoaderDocument(t, 2, 4, false), nil)
	set := principalKeySet(t, 4, map[string][]string{"analyst": {rotationOld}}, map[string][]string{})
	a.revision, a.digest, a.authoritySequence, a.witnessSequence = set.revision, set.digest, 17, 23
	started := time.Now()
	attempt, err := authfence.NewAttempt(started, started.Add(gatewayKeyReadTimeout))
	if err != nil {
		t.Fatal(err)
	}
	if a.apply(set, started) || a.publish(set, started, true) ||
		a.applyRead(gatewayAuthRead{set: set, started: started, attempt: attempt, admitted: true}) {
		t.Fatal("direct helper bypassed opaque authority proof")
	}
	compiled := a.authority
	a.authority = nil
	if a.publish(set, started, true) {
		t.Fatal("raw authority configuration bypassed missing compiled binding")
	}
	a.authority = compiled
	a.invalidate()
	if a.revision != set.revision || a.digest != set.digest || a.authoritySequence != 17 || a.witnessSequence != 23 || a.ready() {
		t.Fatal("proof rejection/invalidation forgot accepted floors")
	}
}

func TestGatewayAuthorityLegacyConstructorsRejectUncompiledOptIn(t *testing.T) {
	cfg := gatewayAuthorityBindingFixture(t)
	var reads, opens atomic.Int32
	reader := func(context.Context, string, int) ([]byte, error) { reads.Add(1); return nil, nil }
	opener := func(context.Context, string, authstate.Scope) (gatewayAuthenticationStore, error) {
		opens.Add(1)
		return nil, nil
	}
	if _, err := newGatewayAuthenticator(*cfg.Authentication, map[string]bool{"a": true, "b": true}, reader); err == nil {
		t.Fatal("legacy constructor accepted raw authority configuration")
	}
	if _, err := newGatewayAuthenticatorWithState(*cfg.Authentication, map[string]bool{"a": true, "b": true}, reader, opener); err == nil {
		t.Fatal("state constructor accepted uncompiled authority configuration")
	}
	if reads.Load() != 0 || opens.Load() != 0 {
		t.Fatal("invalid constructor performed private file/state work")
	}
}
