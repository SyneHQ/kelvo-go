//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

func cleanupHTTPFixture(t *testing.T) (operationHTTPFixture, ledger.Record, resolver.CleanupBinding) {
	t.Helper()
	f := newOperationHTTPFixture(t)
	f.request.Kind = operations.QueryRead
	f.request.Spec = operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT 1"}}
	f.request.IdempotencyKey = ""
	principal := f.policy.Access.Principals["api"]
	principal.Operations = append(principal.Operations, operations.QueryRead)
	f.policy.Access.Principals["api"] = principal
	f.cluster.policy = f.policy
	authority, _ := authorityForPrincipal(f.policy, "api")
	f.context = context.WithValue(f.context, jobAuthorityKey{}, authority)
	f.claims.Operation = f.request.Kind
	f.claims.RequestSHA256, _ = operations.Digest(f.request)
	f.grant, _ = operations.SignGrant(f.claims, f.key)
	response := f.submit(t)
	scope := operationScope(f.claims)
	b := ledger.Binding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}
	if _, err := f.state.store.Claim(f.context, scope, response.ID, b); err != nil {
		t.Fatal(err)
	}
	if _, err := f.state.store.Start(f.context, scope, response.ID, b); err != nil {
		t.Fatal(err)
	}
	wire := resolver.CleanupBinding{WorkerID: b.WorkerID, Owner: b.Owner, Claim: b.Claim, DataTicketSHA256: strings.Repeat("c", 64), AcceptanceID: strings.Repeat("d", 32)}
	if _, err := f.state.store.RegisterAcceptedCleanup(f.context, scope, response.ID, b, wire.DataTicketSHA256, wire.AcceptanceID, time.Now().Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	cancelled, err := f.state.store.Cancel(f.context, scope, response.ID)
	if err != nil {
		t.Fatal(err)
	}
	return f, cancelled.Record, wire
}

func TestOperationCleanupLeaseIsSeparateAndCannotExtend(t *testing.T) {
	f, record, proof := cleanupHTTPFixture(t)
	raw, _ := json.Marshal(proof)
	path := "/v1/operations/" + record.ID + "/cleanup-lease"
	var first resolver.CleanupLeaseResponse
	for i := 0; i < 2; i++ {
		response := f.call(http.MethodPost, path, raw, f.grant)
		if response.Code != http.StatusOK {
			t.Fatalf("cleanup lease rejected: %d %s", response.Code, response.Body.String())
		}
		var lease resolver.CleanupLeaseResponse
		if json.Unmarshal(response.Body.Bytes(), &lease) != nil || lease.ValidateAt(time.Now()) != nil {
			t.Fatal("invalid cleanup lease response")
		}
		if i == 0 {
			first = lease
		} else if lease != first {
			t.Fatal("lease read changed cancellation deadline")
		}
	}
	ordinary, _ := json.Marshal(resolver.Binding{WorkerID: proof.WorkerID, Owner: proof.Owner, Claim: proof.Claim})
	if response := f.call(http.MethodPost, "/v1/operations/"+record.ID+"/connection-lease", ordinary, f.grant); response.Code == http.StatusOK {
		t.Fatal("cleanup restored execution authority")
	}
}
func TestOperationCleanupLeaseRechecksWorkerAndCompletion(t *testing.T) {
	for _, name := range []string{"worker", "completion", "claim", "digest", "acceptance", "unknown-field"} {
		t.Run(name, func(t *testing.T) {
			f, record, proof := cleanupHTTPFixture(t)
			if name == "worker" {
				f.cluster.lease = func(context.Context, string, string) (time.Time, error) { return time.Now().Add(-time.Second), nil }
			}
			if name == "completion" {
				f.cluster.lease = func(ctx context.Context, _, _ string) (time.Time, error) {
					_, err := f.state.store.CompleteCleanup(ctx, record.Scope, record.ID, record.Binding, proof.DataTicketSHA256, proof.AcceptanceID, true)
					return time.Now().Add(time.Minute), err
				}
			}
			if name == "claim" {
				proof.Claim = strings.Repeat("e", 32)
			}
			if name == "digest" {
				proof.DataTicketSHA256 = strings.Repeat("e", 64)
			}
			if name == "acceptance" {
				proof.AcceptanceID = strings.Repeat("e", 32)
			}
			raw, _ := json.Marshal(proof)
			if name == "unknown-field" {
				raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"sql":"SELECT 1"}`)
			}
			if response := f.call(http.MethodPost, "/v1/operations/"+record.ID+"/cleanup-lease", raw, f.grant); response.Code == http.StatusOK {
				t.Fatal("invalid cleanup authority accepted")
			}
		})
	}
}
