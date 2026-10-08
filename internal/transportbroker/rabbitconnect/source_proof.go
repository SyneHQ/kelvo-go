// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"context"
	"crypto/ed25519"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/sourceproof"
)

// SourceProofConfig belongs to one trusted parent execution and source.
// Scope comes from verified admission and current metadata, never from the
// returned proof. Refresh must repeat resolver authorization for each call.
// It must obey ctx and retain admission until its work stops.
type SourceProofConfig struct {
	PublicKey ed25519.PublicKey
	Scope     sourceproof.Scope
	Refresh   func(context.Context) (sourceproof.Envelope, error)
}

// NewWithSourceProof adds fresh proof verification before each physical open.
// It never caches proofs or extends execution custody. The original grant and
// proof enter only the parent-owned issuer request, not driver input.
func NewWithSourceProof(config Config, issuer Issuer, proof SourceProofConfig) (*Opener, error) {
	if len(proof.PublicKey) != ed25519.PublicKeySize || proof.Refresh == nil {
		return nil, transportbroker.ErrInvalid
	}
	opener, err := New(config, issuer)
	if err != nil {
		return nil, err
	}
	proof.PublicKey = append(ed25519.PublicKey(nil), proof.PublicKey...)
	opener.sourceProof = &proof
	return opener, nil
}

func (o *Opener) refreshSourceProof(ctx context.Context, request IssueRequest) (IssueRequest, error) {
	p := o.sourceProof
	s := p.Scope
	b, e := request.Binding, request.Binding.Execution
	expected := s
	expected.Issuer = b.Issuer
	expected.Audience = b.Audience
	expected.ClusterTenant = b.ClusterTenant
	expected.ServicePrincipal = b.ServicePrincipal
	expected.Tenant = b.Tenant
	expected.Source = b.Source
	expected.SourceRevision = b.SourceRevision
	expected.Authority = b.Authority
	expected.Kind = e.Kind
	expected.ExecutionID = e.ID
	expected.GrantSHA256 = e.GrantSHA256
	expected.Worker = e.Worker
	expected.Owner = e.Owner
	expected.Claim = e.Claim
	expected.WorkerIdentity = request.WorkerIdentity
	expected.WorkerCertSHA256 = request.WorkerCertSHA256
	if expected != s || ctx.Err() != nil {
		return IssueRequest{}, transportbroker.ErrScope
	}
	envelope, err := p.Refresh(ctx)
	if err != nil || ctx.Err() != nil {
		return IssueRequest{}, transportbroker.ErrScope
	}
	if _, err := sourceproof.Verify(p.PublicKey, envelope, expected, time.Now()); err != nil {
		return IssueRequest{}, transportbroker.ErrScope
	}
	request.PrivateSource = &envelope
	request.RouteID = s.RouteID
	request.TokenID = s.TokenID
	request.BindingVersion = s.BindingVersion
	return request, nil
}
