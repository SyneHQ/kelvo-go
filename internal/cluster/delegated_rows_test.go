// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type delegatedRowsFixture struct {
	policy Policy
	key    ed25519.PrivateKey
	auth   JobAuthority
}

func newDelegatedRows(t *testing.T) delegatedRowsFixture {
	t.Helper()
	p, key, _, _, a := delegatedTestGrant(t)
	return delegatedRowsFixture{p, key, a}
}

func userPolicy(account string) *delegation.DelegatedPolicy {
	return &delegation.DelegatedPolicy{Revision: strings.Repeat("e", 64), Sources: map[string]delegation.DelegatedSourcePolicy{"src": {Tables: map[string]delegation.DelegatedTablePolicy{
		"orders": {Columns: []string{"amount"}, Rows: &delegation.DelegatedPredicate{Kind: "comparison", Column: "account_id", Type: "int64", Op: "eq", Value: account}}}}}}
}

// grant signs a federated grant for one user; mutate may adjust claims before signing.
func (f delegatedRowsFixture) grant(t *testing.T, user string, policy *delegation.DelegatedPolicy, mode string, mutate func(*delegation.Claims)) query.Request {
	t.Helper()
	r := query.Request{Mode: mode, SQL: "SELECT amount FROM src.orders", Sources: []string{"src"}}
	source := delegation.Source{Alias: "src", ConnectionID: "saved-a", Database: "db", Tables: []delegation.Table{{Name: "orders", Table: "orders"}}}
	if mode == "native" {
		r = query.Request{Mode: "native", ConnectionID: "src", SQL: "SELECT 1"}
		source.Tables = nil
	}
	digest, err := delegation.QueryDigest(r)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	c := delegation.Claims{Version: 2, Issuer: "api-resolver", Audience: "analytics", ClusterTenant: "a", ServicePrincipal: "bridge", AppTeam: "team-a", Subject: delegation.Subject{Kind: "user", ID: user},
		ID: strings.Repeat("a", 48), IssuedAt: now - 1, ExpiresAt: now + 120, QuerySHA256: digest, Sources: []delegation.Source{source}, RowColumnPolicy: policy}
	if mutate != nil {
		mutate(&c)
	}
	r.Delegation = signUnchecked(t, c, f.key)
	return r
}

// signUnchecked signs claims the way a faulty or hostile issuer could, so tests reach the node's own checks.
func signUnchecked(t *testing.T, c delegation.Claims, key ed25519.PrivateKey) string {
	t.Helper()
	h, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "JWT", "kid": c.Issuer})
	p, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(unsigned)))
}

func publicCode(err error) string {
	var public *query.Error
	if errors.As(err, &public) {
		return public.Code
	}
	return ""
}

func TestDelegatedGrantPolicyBecomesTheExecutionRestriction(t *testing.T) {
	f := newDelegatedRows(t)
	policies := map[string]string{}
	for _, user := range []struct{ id, account string }{{"user-a", "42"}, {"user-b", "43"}} {
		r := f.grant(t, user.id, userPolicy(user.account), "federated", nil)
		if _, err := submissionAuthority(delegatedRequestContext(f.auth, r.Delegation), f.policy, r); err != nil {
			t.Fatal(err)
		}
		bound, _, err := delegatedAuthority(f.policy, f.auth, r, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		ctx, err := executionAuthorityContext(context.Background(), f.policy, &bound, r)
		if err != nil {
			t.Fatal(err)
		}
		got, restricted := access.PolicyFromContext(ctx)
		if !restricted {
			t.Fatalf("%s executes unrestricted", user.id)
		}
		table, _, allowed := access.Lookup(ctx, "src", "orders")
		if !allowed || table.Rows == nil || got.Sources["src"].Tables["orders"].Columns[0] != "amount" {
			t.Fatalf("%s lost the policy", user.id)
		}
		policies[user.id] = table.Rows.Value
		if _, _, allowed := access.Lookup(ctx, "src", "customers"); allowed {
			t.Fatal("a table missing from the policy is visible")
		}
	}
	if policies["user-a"] != "42" || policies["user-b"] != "43" {
		t.Fatalf("users did not receive their own predicates: %v", policies)
	}
}

func TestDelegatedGrantWithoutPolicyStaysUnrestricted(t *testing.T) {
	f := newDelegatedRows(t)
	r := f.grant(t, "user-a", nil, "federated", func(c *delegation.Claims) { c.Version = 1 })
	bound, _, err := delegatedAuthority(f.policy, f.auth, r, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := executionAuthorityContext(context.Background(), f.policy, &bound, r)
	if err != nil || access.Restricted(ctx) {
		t.Fatalf("a version 1 grant gained a restriction: %v", err)
	}
}

func TestRestrictedDelegatedGrantRefusesNativeMode(t *testing.T) {
	f := newDelegatedRows(t)
	// A native request under a grant that carries a policy: the grant's source is
	// table-scoped, which is what a policy needs and what native grants never have.
	r := f.grant(t, "user-a", userPolicy("42"), "native", func(c *delegation.Claims) {
		c.Sources[0].Tables = []delegation.Table{{Name: "orders", Table: "orders"}}
	})
	for name, call := range map[string]func() error{
		"gateway admission": func() error {
			_, err := submissionAuthority(delegatedRequestContext(f.auth, r.Delegation), f.policy, r)
			return err
		},
		"node execution": func() error {
			_, err := executionAuthorityContext(context.Background(), f.policy, &f.auth, r)
			return err
		},
	} {
		err := call()
		if name == "node execution" {
			// Defense in depth: the node's own re-check refuses it too (as a denial).
			if err == nil {
				t.Fatal("node executed a native request under a restricted grant")
			}
			continue
		}
		if publicCode(err) != "UNSUPPORTED" || err.Error() != errDelegatedNativeRestricted.Error() || !strings.Contains(err.Error(), "mode federated") {
			t.Fatalf("%s: want UNSUPPORTED naming federated mode, got %v", name, err)
		}
	}
	// A policy that cannot be keyed to the native source's (absent) tables is plainly invalid.
	tableless := f.grant(t, "user-a", userPolicy("42"), "native", nil)
	if _, err := submissionAuthority(delegatedRequestContext(f.auth, tableless.Delegation), f.policy, tableless); err == nil || publicCode(err) == "UNSUPPORTED" {
		t.Fatalf("invalid native grant: %v", err)
	}
	// An unrestricted native grant is untouched by all of this.
	plain := f.grant(t, "user-a", nil, "native", func(c *delegation.Claims) { c.Version = 1 })
	if _, err := submissionAuthority(delegatedRequestContext(f.auth, plain.Delegation), f.policy, plain); err != nil {
		t.Fatalf("native grant without a policy refused: %v", err)
	}
}

func TestInvalidDelegatedPolicyIsRejectedBeforeExecution(t *testing.T) {
	f := newDelegatedRows(t)
	cases := map[string]func(*delegation.DelegatedTablePolicy){
		"unknown operator": func(p *delegation.DelegatedTablePolicy) { p.Rows.Op = "like" },
		"star column":      func(p *delegation.DelegatedTablePolicy) { p.Columns = []string{"*"} },
		"bad integer":      func(p *delegation.DelegatedTablePolicy) { p.Rows.Value = "4 2" },
		"oversized column": func(p *delegation.DelegatedTablePolicy) { p.Columns = []string{strings.Repeat("c", 1025)} },
	}
	for name, mutate := range cases {
		policy := userPolicy("42")
		table := policy.Sources["src"].Tables["orders"]
		mutate(&table)
		policy.Sources["src"].Tables["orders"] = table
		r := f.grant(t, "user-a", policy, "federated", nil)
		if _, err := submissionAuthority(delegatedRequestContext(f.auth, r.Delegation), f.policy, r); err == nil {
			t.Errorf("%s: admitted at the gateway", name)
		}
		if _, err := executionAuthorityContext(context.Background(), f.policy, &f.auth, r); err == nil {
			t.Errorf("%s: reached execution", name)
		}
	}
}

func TestTamperedDelegatedPolicyIsRejectedByTheNode(t *testing.T) {
	f := newDelegatedRows(t)
	r := f.grant(t, "user-a", userPolicy("42"), "federated", nil)
	parts := strings.Split(r.Delegation, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	loosened := strings.Replace(string(payload), `"value":"42"`, `"value":"43"`, 1)
	if loosened == string(payload) {
		t.Fatal("fixture predicate not found")
	}
	r.Delegation = parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(loosened)) + "." + parts[2]
	if _, err := submissionAuthority(delegatedRequestContext(f.auth, r.Delegation), f.policy, r); err == nil {
		t.Fatal("tampered policy admitted")
	}
	if _, err := executionAuthorityContext(context.Background(), f.policy, &f.auth, r); err == nil {
		t.Fatal("tampered policy executed")
	}
}

// Exports run the stored request through executionAuthorityContext with the
// stored authority (export_runtime.go), so they get the grant's restriction. Once the
// grant has expired the same call fails: authority is never re-derived as unrestricted.
func TestDelegatedExportExecutionCarriesThePolicyAndFailsClosedAfterExpiry(t *testing.T) {
	f := newDelegatedRows(t)
	now := time.Now().Unix()
	r := f.grant(t, "user-a", userPolicy("42"), "federated", func(c *delegation.Claims) { c.IssuedAt, c.ExpiresAt = now-5, now+1 })
	stored := ExportAuthority{Principal: f.auth}
	bound, _, err := delegatedAuthority(f.policy, stored.Principal, r, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	stored.Principal = bound
	ctx, err := executionAuthorityContext(context.Background(), f.policy, &stored.Principal, r)
	if err != nil || !access.Restricted(ctx) {
		t.Fatalf("export runs unrestricted: %v", err)
	}
	time.Sleep(1200 * time.Millisecond)
	if _, err := executionAuthorityContext(context.Background(), f.policy, &stored.Principal, r); err == nil {
		t.Fatal("expired delegated authority produced an execution context")
	}
}
