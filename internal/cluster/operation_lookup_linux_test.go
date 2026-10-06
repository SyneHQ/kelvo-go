//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestOperationLookupNeedsNoInputStoreAndNeverSubmits(t *testing.T) {
	f := newOperationHTTPFixture(t)
	backend := &operationMemoryBackend{values: map[string]ledger.Entry{}}
	store, err := ledger.New(backend, operationStorePolicy(f.policy))
	if err != nil {
		t.Fatal(err)
	}
	f.state.store = store
	first := f.submit(t)
	revision := backend.revision
	// Source inputs can have expired or become unavailable. Recovery reads
	// only the retained ledger identity, never a new or existing InputRef.
	f.state.inputs = nil
	request := operations.LookupRequest{Version: operations.Version, IdempotencyKey: f.request.IdempotencyKey, RequestSHA256: f.claims.RequestSHA256}
	raw, _ := json.Marshal(request)
	for range 2 {
		w := f.call(http.MethodPost, "/v1/operations/lookup", raw, f.grant)
		var result operations.Response
		if w.Code != 200 || operations.DecodeStrict(w.Body.Bytes(), &result, operations.MaxReceiptBytes+4096) != nil || result.Validate() != nil || result.ID != first.ID || result.State != ledger.Queued {
			t.Fatal("retained identity recovery failed", w.Code)
		}
	}
	fresh := f.claims
	fresh.ID = "fresh-status-grant"
	grant, err := operations.SignGrant(fresh, f.key)
	if err != nil {
		t.Fatal(err)
	}
	if w := f.call(http.MethodPost, "/v1/operations/lookup", raw, grant); w.Code != 200 {
		t.Fatal("fresh scoped recovery grant rejected", w.Code)
	}
	request.IdempotencyKey = "never-submitted"
	raw, _ = json.Marshal(request)
	if w := f.call(http.MethodPost, "/v1/operations/lookup", raw, grant); w.Code != 404 {
		t.Fatal("missing lookup created another attempt", w.Code)
	}
	if backend.revision != revision {
		t.Fatal("lookup performed a ledger write")
	}
}

func TestOperationLookupChecksCurrentGrantScopeAndDigest(t *testing.T) {
	f := newOperationHTTPFixture(t)
	f.submit(t)
	request := operations.LookupRequest{Version: operations.Version, IdempotencyKey: f.request.IdempotencyKey, RequestSHA256: f.claims.RequestSHA256}
	raw, _ := json.Marshal(request)
	for _, tc := range []struct {
		name   string
		change func(*operations.GrantClaims)
		status int
	}{
		{"foreign_team", func(c *operations.GrantClaims) { c.AppTeam = "another-team" }, 404},
		{"foreign_subject", func(c *operations.GrantClaims) { c.Subject.ID = "another-key" }, 404},
		{"foreign_connection", func(c *operations.GrantClaims) { c.ConnectionID = "another-source" }, 404},
		{"wrong_digest", func(c *operations.GrantClaims) { c.RequestSHA256 = strings.Repeat("f", 64) }, 403},
		{"expired_grant", func(c *operations.GrantClaims) {
			c.IssuedAt = time.Now().Add(-2 * time.Minute).Unix()
			c.ExpiresAt = time.Now().Add(-time.Minute).Unix()
		}, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claims := f.claims
			claims.ID = tc.name
			tc.change(&claims)
			grant, err := operations.SignGrant(claims, f.key)
			if err != nil {
				t.Fatal(err)
			}
			if w := f.call(http.MethodPost, "/v1/operations/lookup", raw, grant); w.Code != tc.status {
				t.Fatal("lookup authority mismatch", w.Code)
			}
		})
	}
	changed := f.claims
	changed.ID = "changed-body"
	changed.RequestSHA256 = strings.Repeat("e", 64)
	grant, _ := operations.SignGrant(changed, f.key)
	request.RequestSHA256 = changed.RequestSHA256
	raw, _ = json.Marshal(request)
	if w := f.call(http.MethodPost, "/v1/operations/lookup", raw, grant); w.Code != 409 {
		t.Fatal("same key different request was not fenced", w.Code)
	}
	if w := f.call(http.MethodPost, "/v1/operations/lookup", []byte(`{"version":1,"extra":true}`), f.grant); w.Code != 400 {
		t.Fatal("unbound lookup fields accepted", w.Code)
	}
}
