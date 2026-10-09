// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/sourceproof"
)

// This policy belongs to the trusted parent. It cannot be installed by a query,
// a resolver response, or a child process. No startup path constructs it yet.
type privateOperationPolicy struct {
	resolver                                   *ConnectionResolver
	trust                                      operations.GrantTrust
	proofKey                                   ed25519.PublicKey
	route, worker, identity, certificateSHA256 string
	certificate                                []byte
}
type privateOperationSelection struct {
	input   adapter.ProcessRequest
	proof   sourceproof.Envelope
	scope   sourceproof.Scope
	binding transportbroker.Binding
}

// certificate is the same leaf used by the parent's Rabbit opener and issuer.
// Pinning the resolver to that leaf prevents delivery to a different worker key.
func newPrivateOperationPolicy(r *ConnectionResolver, trust operations.GrantTrust, proofKey ed25519.PublicKey, route, worker, identity string, certificate []byte) (*privateOperationPolicy, error) {
	if r == nil || len(trust.PublicKey) != ed25519.PublicKeySize || len(proofKey) != ed25519.PublicKeySize || route == "" || worker == "" || identity == "" || len(certificate) == 0 {
		return nil, connectionUnavailable()
	}
	sum := sha256.Sum256(certificate)
	trust.PublicKey = append(ed25519.PublicKey(nil), trust.PublicKey...)
	p := &privateOperationPolicy{resolver: r, trust: trust, proofKey: append(ed25519.PublicKey(nil), proofKey...), route: route, worker: worker, identity: identity, certificate: append([]byte(nil), certificate...), certificateSHA256: hex.EncodeToString(sum[:])}
	if _, ok := p.workerCertificate(time.Now()); !ok {
		return nil, connectionUnavailable()
	}
	return p, nil
}
func (p *privateOperationPolicy) workerCertificate(now time.Time) (time.Time, bool) {
	if p == nil || p.resolver == nil || p.resolver.client == nil {
		return time.Time{}, false
	}
	transport, ok := p.resolver.client.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil {
		return time.Time{}, false
	}
	config := transport.TLSClientConfig
	if config.MinVersion < tls.VersionTLS13 || config.InsecureSkipVerify || config.GetClientCertificate != nil || len(config.Certificates) != 1 || len(config.Certificates[0].Certificate) == 0 || !bytes.Equal(config.Certificates[0].Certificate[0], p.certificate) {
		return time.Time{}, false
	}
	var until time.Time
	for i, der := range config.Certificates[0].Certificate {
		cert, err := x509.ParseCertificate(der)
		if err != nil || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
			return time.Time{}, false
		}
		if i == 0 {
			if cert.IsCA || len(cert.URIs) != 1 || cert.URIs[0].String() != p.identity {
				return time.Time{}, false
			}
			usage := false
			for _, value := range cert.ExtKeyUsage {
				usage = usage || value == x509.ExtKeyUsageClientAuth
			}
			if !usage {
				return time.Time{}, false
			}
			until = cert.NotAfter
		} else if cert.NotAfter.Before(until) {
			until = cert.NotAfter
		}
	}
	return until, true
}

func (p *privateOperationPolicy) resolve(ctx context.Context, e *Executor, record operationstore.Record, request operations.Request, payload []byte) (privateOperationSelection, error) {
	bad := func() (privateOperationSelection, error) { return privateOperationSelection{}, connectionUnavailable() }
	if ctx == nil || ctx.Err() != nil || e == nil || p == nil || e.connectionResolvers[record.Scope.Issuer] != p.resolver || adapter.ValidateOperationInput(request, record.Scope.AppTeam, payload) != nil || record.State != operationstore.Running || record.Scope.Validate() != nil || record.Binding.Validate() != nil || record.Binding.WorkerID != p.worker || record.AuthoritySHA256 != operations.GrantDigest(record.AuthorityToken) {
		return bad()
	}
	now := time.Now()
	peerUntil, ok := p.workerCertificate(now)
	if !ok {
		return bad()
	}
	grant, err := operations.VerifyGrant(record.AuthorityToken, p.trust, request, now)
	if err != nil || grant.Issuer != record.Scope.Issuer || grant.ClusterTenant != record.Scope.ClusterTenant || grant.ServicePrincipal != record.Scope.ServicePrincipal || grant.AppTeam != record.Scope.AppTeam || grant.Subject.Kind != record.Scope.SubjectKind || grant.Subject.ID != record.Scope.SubjectID || grant.Subject.JobID != record.Scope.SubjectJobID || grant.ConnectionID != record.Scope.ConnectionID || grant.Operation != record.Kind || grant.RequestSHA256 != record.RequestSHA256 || record.AuthorityUntil.Unix() > grant.ExpiresAt || !now.Before(record.AuthorityUntil) || !now.Before(record.ExecuteBefore) || record.ExecuteBefore.After(record.AuthorityUntil) {
		return bad()
	}
	switch grant.Authorization.Kind {
	case "watcher", "ingestion", "approved_change":
		return bad()
	}
	response, err := p.resolver.resolveOperationMode(ctx, record, request, true)
	if err != nil {
		return bad()
	}
	defer clear(response.Secrets)
	if response.privateSource == nil || response.privateSource.Grant != record.AuthorityToken || (response.Source.Type != "postgres" && response.Source.Type != "postgresql" && response.Source.Type != "mysql") {
		return bad()
	}
	input, err := e.operationInput(ctx, record, request, payload, response)
	if err != nil {
		return bad()
	}
	// Catalog validation uses postgres; the native child uses postgresql.
	if input.Source.Engine == "postgres" {
		input.Source.Engine = "postgresql"
	}
	authority, err := privateSourceAuthority(input.Source)
	if err != nil {
		return bad()
	}
	expected := sourceproof.Scope{Issuer: p.trust.Issuer, Audience: p.trust.Audience, ClusterTenant: p.trust.ClusterTenant, ServicePrincipal: p.trust.ServicePrincipal, Tenant: record.Scope.AppTeam, Source: record.Scope.ConnectionID, SourceRevision: response.SourceRevision, Authority: authority, RouteID: p.route, Kind: "operation", ExecutionID: record.ID, GrantSHA256: record.AuthoritySHA256, Worker: record.Binding.WorkerID, Owner: record.Binding.Owner, Claim: record.Binding.Claim, WorkerIdentity: p.identity, WorkerCertSHA256: p.certificateSHA256}
	verified, err := sourceproof.VerifySelection(p.proofKey, *response.privateSource, expected, time.Now())
	if err != nil {
		return bad()
	}
	claims, err := sourceproof.Verify(p.proofKey, *response.privateSource, verified.Scope(), time.Now())
	if err != nil || claims.ExpiresAt > response.ValidUntil || claims.ExpiresAt > peerUntil.Unix() || ctx.Err() != nil {
		return bad()
	}
	binding := transportbroker.Binding{Issuer: expected.Issuer, Audience: expected.Audience, ClusterTenant: expected.ClusterTenant, ServicePrincipal: expected.ServicePrincipal, Tenant: expected.Tenant, Source: expected.Source, SourceRevision: expected.SourceRevision, Authority: authority, ExpiresAt: record.ExecuteBefore, Execution: transportbroker.Execution{Kind: "operation", ID: record.ID, GrantSHA256: record.AuthoritySHA256, Worker: record.Binding.WorkerID, Owner: record.Binding.Owner, Claim: record.Binding.Claim}}
	if !privateInputScope(input, binding) || input.Validate() != nil {
		return bad()
	}
	return privateOperationSelection{input: input, proof: *response.privateSource, scope: verified.Scope(), binding: binding}, nil
}
