//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestApplicationFileCallbacksLoadRunningRecordAfterPrepare(t *testing.T) {
	f := newOperationHTTPFixture(t)
	ctx := context.Background()
	request := operations.Request{Version: 1, Kind: operations.QueryRead,
		Connection: operations.ConnectionRef{ID: "saved-a", Database: "main"},
		Spec:       operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT 1"}}}
	digest, err := operations.Digest(request)
	if err != nil {
		t.Fatal(err)
	}
	claims := f.claims
	claims.Operation, claims.RequestSHA256 = request.Kind, digest
	claims.Subject = operations.Subject{Kind: "user", ID: "owner-a"}
	claims.Authorization = operations.Authorization{Kind: "read"}
	grant, err := operations.SignGrant(claims, f.key)
	if err != nil {
		t.Fatal(err)
	}
	a := &Application{
		cfg: ApplicationConfig{AppScope: claims.AppTeam, WriteMode: "disabled"},
		trust: operations.GrantTrust{Issuer: claims.Issuer, Audience: claims.Audience, ClusterTenant: claims.ClusterTenant,
			ServicePrincipal: claims.ServicePrincipal, PublicKey: f.key.Public().(ed25519.PublicKey)},
		owner: strings.Repeat("a", 32), ctx: ctx, ledger: f.state.store,
	}
	ref, _, err := operations.SealRequest(request, "file-request")
	if err != nil {
		t.Fatal(err)
	}
	scope := operationScope(claims)
	submitted, _, err := a.ledger.Submit(ctx, operationstore.Submission{Scope: scope, Request: request, RequestRef: ref,
		AuthorityToken: grant, AuthoritySHA256: operations.GrantDigest(grant), AuthorityUntil: time.Unix(claims.ExpiresAt, 0)})
	if err != nil {
		t.Fatal(err)
	}
	binding := operationstore.Binding{WorkerID: "application", Owner: a.owner, Claim: strings.Repeat("b", 32)}
	assigned, err := a.ledger.Claim(ctx, scope, submitted.Record.ID, binding)
	if err != nil {
		t.Fatal(err)
	}
	p := &preparedApplicationOperation{app: a, record: assigned.Record, request: request}
	if _, err := p.currentRecord(ctx); err == nil {
		t.Fatal("file callback authorized before the durable start barrier")
	}
	if _, err := a.ledger.Start(ctx, scope, submitted.Record.ID, binding); err != nil {
		t.Fatal(err)
	}
	current, err := p.currentRecord(ctx)
	if err != nil || current.State != operationstore.Running || p.record.State != operationstore.Assigned {
		t.Fatal("file callback did not replace the stale prepared record", err)
	}
	a.failed.Store(true)
	if _, err := p.currentRecord(ctx); err == nil {
		t.Fatal("file callback bypassed current application authority")
	}
	a.failed.Store(false)
	if _, err := a.ledger.Complete(ctx, scope, current.ID, binding, operations.Receipt{Version: 1, OperationID: current.ID,
		RequestSHA256: digest, Outcome: operations.Completed, Effect: operations.EffectNone}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.currentRecord(ctx); err == nil {
		t.Fatal("file callback authorized after operation completion")
	}
}
