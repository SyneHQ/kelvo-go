//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/authfence"
	"github.com/SYNEHQ/kelvo-go/internal/secrets"
)

type gatewayAuthorityPublicationFloor struct {
	revision          uint64
	digest            [32]byte
	authoritySequence uint64
	witnessSequence   uint64
}

func gatewayAuthorityPublicationFloors(a *gatewayAuthenticator) gatewayAuthorityPublicationFloor {
	a.mu.Lock()
	defer a.mu.Unlock()
	return gatewayAuthorityPublicationFloor{a.revision, a.digest, a.authoritySequence, a.witnessSequence}
}

// Every usable observation comes from beginRead: the production private reader,
// authority protocol and durable admission all run before these publication
// checks. The sole loader has no renewal loop, so test scheduling cannot replace
// the exact result under examination.
func gatewayAuthorityPublicationCases(t *testing.T, first *authorityGatewayFixture) {
	t.Helper()
	if err := first.gate.Close(); err != nil {
		t.Fatal("gateway retained ownership before publication checks", err)
	}
	first.gate = nil
	normalized, err := first.config.Authentication.normalized()
	if err != nil {
		t.Fatal(err)
	}
	bound, err := bindGatewayKeyAuthority(first.config)
	if err != nil || bound == nil {
		t.Fatal("publication fixture lost its bound authority", err)
	}
	tenants := make(map[string]bool, len(first.config.Tenants))
	for _, tenant := range first.config.Tenants {
		tenants[tenant.Policy.TenantID] = true
	}
	if err := validateGatewayKeyAuthorityRuntime(normalized, tenants, bound); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &gatewayAuthenticator{config: normalized, tenants: tenants, authority: bound,
		ctx: ctx, cancel: cancel, done: make(chan struct{}), read: secrets.ReadPrivateDocument,
		openState: openGatewayAuthState, verifier: newGatewayAuthorityVerifier}
	t.Cleanup(func() {
		if err := a.close(); err != nil {
			t.Error("publication fixture retained its state writer", err)
		}
	})

	read := func(t *testing.T) gatewayAuthRead {
		t.Helper()
		select {
		case result := <-a.beginRead():
			document, err := gatewayAuthorityDocument(result.set)
			if result.err != nil || !result.admitted || err != nil ||
				!result.proof.ValidFor(bound.config.Scope, result.attempt, document, time.Now()) {
				t.Fatal("publication fixture did not obtain a fresh admitted protocol proof", result.err)
			}
			return result
		case <-time.After(gatewayKeyReadTimeout + time.Second):
			t.Fatal("publication fixture loader exceeded its original budget")
			return gatewayAuthRead{}
		}
	}
	accept := func(t *testing.T, result gatewayAuthRead) {
		t.Helper()
		if !a.applyRead(result) || !a.ready() {
			t.Fatal("exact fresh admitted protocol proof did not publish")
		}
		if a.expiry() != result.started.Add(gatewayAuthorityLease) {
			t.Fatal("publication renewed the original lease start")
		}
	}
	assertFloors := func(t *testing.T, want gatewayAuthorityPublicationFloor) {
		t.Helper()
		if gatewayAuthorityPublicationFloors(a) != want {
			t.Fatal("rejected publication changed accepted revision or sequence floors")
		}
	}
	var initial gatewayAuthRead
	var initialKey context.Context
	if !t.Run("ExactProofAndReplay", func(t *testing.T) {
		initial = read(t)
		accept(t, initial)
		_, initialKey, _ = a.lookup(rotationNew)
		if initialKey == nil || initialKey.Err() != nil {
			t.Fatal("accepted proof did not activate the current key")
		}
		floor := gatewayAuthorityPublicationFloors(a)
		if floor.authoritySequence == 0 || floor.witnessSequence <= floor.authoritySequence {
			t.Fatal("accepted publication did not retain protocol sequence floors")
		}
		a.invalidate()
		assertFloors(t, floor)
		document, err := gatewayAuthorityDocument(initial.set)
		if err != nil || !initial.proof.ValidFor(bound.config.Scope, initial.attempt, document, time.Now()) {
			t.Fatal("replay proof expired before its rejection could be tested")
		}
		if a.applyRead(initial) || a.ready() || initialKey.Err() == nil {
			t.Fatal("replayed witness revived invalidated authority")
		}
		assertFloors(t, floor)
	}) {
		return
	}
	if !t.Run("FreshWitnessRetainsAuthoritySequence", func(t *testing.T) {
		before := gatewayAuthorityPublicationFloors(a)
		fresh := read(t)
		accept(t, fresh)
		after := gatewayAuthorityPublicationFloors(a)
		_, key, ok := a.lookup(rotationNew)
		if after.revision != before.revision || after.digest != before.digest ||
			after.authoritySequence != before.authoritySequence || after.witnessSequence <= before.witnessSequence ||
			!ok || key == initialKey || initialKey.Err() == nil {
			t.Fatal("fresh same-document witness changed authority or revived an old request context")
		}
	}) {
		return
	}
	for _, name := range []string{"DifferentAttempt", "DifferentDocument", "DifferentScope"} {
		if !t.Run(name, func(t *testing.T) {
			result := read(t)
			before := gatewayAuthorityPublicationFloors(a)
			wrong := result
			switch name {
			case "DifferentAttempt":
				// Keep identical timestamps; only the random attempt identity differs.
				wrong.attempt, err = authfence.NewAttempt(result.started, result.attempt.Deadline())
				if err != nil || wrong.attempt.ID() == result.attempt.ID() {
					t.Fatal("unable to create a distinct attempt", err)
				}
			case "DifferentDocument":
				raw := principalStateDocument(t, result.set.revision+1,
					map[string][]string{"reports": {rotationNew}}, map[string][]string{"reports": {rotationOther}})
				wrong.set, err = parseGatewayKeys(raw, tenants, normalized.MinRevision)
				clear(raw)
				if err != nil {
					t.Fatal(err)
				}
			case "DifferentScope":
				other, err := authfence.NewScope("other-fleet", bound.config.Scope.Tenants(), bound.config.Scope.Gateways())
				if err != nil {
					t.Fatal(err)
				}
				changed := *bound
				changed.config.Scope, changed.binding = other, keyAuthorityBinding(other)
				a.authority = &changed
			}
			// Alter the receiving envelope, never the opaque observation. The
			// genuine admission/proof belongs only to the unmodified result.
			accepted := a.publishRead(wrong)
			a.authority = bound
			if accepted || a.ready() {
				t.Fatal("valid proof escaped its attempt, document or scope")
			}
			assertFloors(t, before)
			// Acceptance of the original result makes expiry or a zero proof
			// insufficient to explain the preceding rejection.
			accept(t, result)
		}) {
			return
		}
	}
	if !t.Run("PublicationLockCannotExtendDeadline", func(t *testing.T) {
		result := read(t)
		before := gatewayAuthorityPublicationFloors(a)
		started, finished := make(chan struct{}), make(chan bool, 1)
		a.mu.Lock()
		locked := true
		unlock := func() {
			if locked {
				locked = false
				a.mu.Unlock()
			}
		}
		defer unlock()
		go func() {
			close(started)
			finished <- a.publishRead(result)
		}()
		<-started
		if !result.attempt.ValidAt(time.Now()) {
			unlock()
			<-finished
			t.Fatal("publication proof expired before mutex contention")
		}
		timer := time.NewTimer(time.Until(result.attempt.Deadline()) + 10*time.Millisecond)
		<-timer.C
		unlock()
		select {
		case accepted := <-finished:
			if accepted || a.ready() {
				t.Fatal("waiting for the publication lock extended the original attempt")
			}
		case <-time.After(time.Second):
			t.Fatal("publication goroutine did not join after its lock was released")
		}
		assertFloors(t, before)
		accept(t, read(t))
	}) {
		return
	}
	if err := a.close(); err != nil {
		t.Fatal("publication checks did not release their state writer", err)
	}
	first.start(t)
}
