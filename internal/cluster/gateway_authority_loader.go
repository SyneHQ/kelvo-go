// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/authfence"
)

const gatewayAuthorityLease = 3 * time.Second

// This private seam permits negative/custody tests without constructing a
// verified observation. Production always constructs a fresh protocol Client.
type gatewayAuthorityVerifier interface {
	Verify(context.Context, authfence.Attempt, authfence.Document) (authfence.VerifiedObservation, error)
	Close(context.Context) error
	Quiesced() <-chan struct{}
}

type gatewayAuthorityVerifierFactory func(authfence.Config) (gatewayAuthorityVerifier, error)

func newGatewayAuthorityVerifier(config authfence.Config) (gatewayAuthorityVerifier, error) {
	client, err := authfence.NewClient(config)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func gatewayAuthorityDocument(set gatewayKeySet) (authfence.Document, error) {
	if set.version != 2 {
		return authfence.Document{}, errGatewayAuthUnavailable
	}
	return authfence.NewDocument(set.revision, hex.EncodeToString(set.digest[:]))
}

// validAuthorityProofLocked checks only opaque protocol evidence and accepted
// process-local floors. Publication additionally requires durable admission.
// The caller supplies its one sampled time while holding a.mu.
func (a *gatewayAuthenticator) validAuthorityProofLocked(result gatewayAuthRead, now time.Time) bool {
	if a.authority == nil || a.config.Authority == nil || result.started != result.attempt.Started() || !result.attempt.ValidAt(now) {
		return false
	}
	document, err := gatewayAuthorityDocument(result.set)
	if err != nil || !result.proof.ValidFor(a.authority.config.Scope, result.attempt, document, now) {
		return false
	}
	snapshot := result.proof.Snapshot()
	if !snapshot.Record().Scope().Equal(a.authority.config.Scope) || !snapshot.Record().Document().Equal(document) ||
		result.proof.WitnessSequence() <= a.witnessSequence {
		return false
	}
	sequence := snapshot.AuthoritySequence()
	if a.authoritySequence == 0 {
		return sequence > 0
	}
	if result.set.revision == a.revision {
		return result.set.digest == a.digest && sequence == a.authoritySequence
	}
	return result.set.revision > a.revision && sequence > a.authoritySequence
}

func (a *gatewayAuthenticator) poisonAuthority() {
	a.mu.Lock()
	a.poisoned = true
	prior := a.keys
	a.keys, a.validUntil = nil, time.Time{}
	a.mu.Unlock()
	cancelGatewayKeys(prior)
}

// verifyKeyAuthority keeps the sole loader and local writer occupied through
// Close and actual Quiesced, even if the original attempt has already expired.
// No caller, timer or failed public close releases that custody early.
func (a *gatewayAuthenticator) verifyKeyAuthority(ctx context.Context, result *gatewayAuthRead) error {
	document, err := gatewayAuthorityDocument(result.set)
	a.mu.Lock()
	eligible := err == nil && gatewayAuthAttemptFresh(ctx) && result.attempt.ValidAt(time.Now()) && a.validCandidateLocked(result.set)
	a.mu.Unlock()
	if !eligible {
		return errGatewayAuthUnavailable
	}
	factory := a.verifier
	if factory == nil {
		factory = newGatewayAuthorityVerifier
	}
	client, verifyErr := factory(a.authority.config)
	if client == nil {
		return errGatewayAuthUnavailable
	}
	if verifyErr == nil {
		result.proof, verifyErr = client.Verify(ctx, result.attempt, document)
	}
	a.mu.Lock()
	a.authorityClosing = true
	a.mu.Unlock()
	closeErr := client.Close(ctx)
	quiet := client.Quiesced()
	select {
	case <-quiet:
	default:
		// Close returning before its owned lifetime joins is uncertain even
		// if it reports nil. Keep the loader and writer until this exact join.
		a.poisonAuthority()
		<-quiet
	}
	a.mu.Lock()
	a.authorityClosing = false
	a.mu.Unlock()
	if closeErr != nil {
		a.poisonAuthority()
	}
	if verifyErr != nil || closeErr != nil || !gatewayAuthAttemptFresh(ctx) {
		return errGatewayAuthUnavailable
	}
	a.mu.Lock()
	valid := a.validCandidateLocked(result.set) && a.validAuthorityProofLocked(*result, time.Now())
	a.mu.Unlock()
	if !valid {
		return errGatewayAuthUnavailable
	}
	return nil
}
