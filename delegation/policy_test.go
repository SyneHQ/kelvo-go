// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package delegation_test

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/query"
)

func policyClaims(t *testing.T, version int) (delegation.Claims, ed25519.PrivateKey, delegation.Trust, query.Request) {
	t.Helper()
	_, key, trust, _, _ := fixture(t)
	r := query.Request{Mode: "federated", SQL: "SELECT * FROM src.orders", Sources: []string{"src"}}
	digest, err := delegation.QueryDigest(r)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	c := delegation.Claims{Version: version, Issuer: trust.Issuer, Audience: trust.Audience, ClusterTenant: trust.ClusterTenant, ServicePrincipal: trust.ServicePrincipal,
		AppTeam: "team-a", Subject: delegation.Subject{Kind: "user", ID: "user-a"}, ID: strings.Repeat("a", 48), IssuedAt: now - 1, ExpiresAt: now + 120, QuerySHA256: digest,
		Sources: []delegation.Source{{Alias: "src", ConnectionID: "saved-a", Database: "db", Tables: []delegation.Table{{Name: "orders", Table: "orders"}}}}}
	if version == 2 {
		c.RowColumnPolicy = &delegation.DelegatedPolicy{Revision: strings.Repeat("b", 64), Sources: map[string]delegation.DelegatedSourcePolicy{"src": {Tables: map[string]delegation.DelegatedTablePolicy{
			"orders": {Columns: []string{"order_id"}, Rows: &delegation.DelegatedPredicate{Kind: "comparison", Column: "account_id", Type: "int64", Op: "eq", Value: "42"}}}}}}
	}
	return c, key, trust, r
}

func TestVersionTwoPolicyRoundTripsAndClones(t *testing.T) {
	c, key, trust, r := policyClaims(t, 2)
	token, err := delegation.Sign(c, key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := delegation.Verify(token, trust, r, time.Unix(c.IssuedAt+1, 0))
	if err != nil || got.RowColumnPolicy == nil || got.RowColumnPolicy.Revision != c.RowColumnPolicy.Revision || got.RowColumnPolicy.Sources["src"].Tables["orders"].Rows.Value != "42" {
		t.Fatalf("policy did not survive the signed round trip: %v", err)
	}
	clone := got.RowColumnPolicy.Clone()
	clone.Sources["src"].Tables["orders"].Rows.Value = "7"
	if got.RowColumnPolicy.Sources["src"].Tables["orders"].Rows.Value != "42" {
		t.Fatal("Clone shares predicate storage")
	}
}

func TestInvalidVersionPolicyCombinationsAreRefusedBySignAndVerify(t *testing.T) {
	good2, key, trust, _ := policyClaims(t, 2)
	good1, _, _, _ := policyClaims(t, 1)
	with := func(mutate func(*delegation.Claims)) delegation.Claims {
		c, _, _, _ := policyClaims(t, 2)
		mutate(&c)
		return c
	}
	setTable := func(c *delegation.Claims, mutate func(*delegation.DelegatedTablePolicy)) {
		s := c.RowColumnPolicy.Sources["src"].Tables["orders"]
		mutate(&s)
		c.RowColumnPolicy.Sources["src"].Tables["orders"] = s
	}
	cases := map[string]delegation.Claims{
		"v1 carrying a policy": func() delegation.Claims { c := good1; c.RowColumnPolicy = good2.RowColumnPolicy; return c }(),
		"v2 without a policy":  func() delegation.Claims { c := good2; c.RowColumnPolicy = nil; return c }(),
		"v3":                   with(func(c *delegation.Claims) { c.Version = 3 }),
		"v0":                   with(func(c *delegation.Claims) { c.Version = 0 }),
		"unknown source alias": with(func(c *delegation.Claims) {
			c.RowColumnPolicy.Sources["other"] = c.RowColumnPolicy.Sources["src"]
			delete(c.RowColumnPolicy.Sources, "src")
		}),
		"alias differing by case": with(func(c *delegation.Claims) {
			c.RowColumnPolicy.Sources["SRC"] = c.RowColumnPolicy.Sources["src"]
			delete(c.RowColumnPolicy.Sources, "src")
		}),
		"unknown table alias": with(func(c *delegation.Claims) {
			s := c.RowColumnPolicy.Sources["src"]
			s.Tables["customers"] = s.Tables["orders"]
			delete(s.Tables, "orders")
		}),
		"empty policy sources": with(func(c *delegation.Claims) { c.RowColumnPolicy.Sources = map[string]delegation.DelegatedSourcePolicy{} }),
		"empty tables":         with(func(c *delegation.Claims) { c.RowColumnPolicy.Sources["src"] = delegation.DelegatedSourcePolicy{} }),
		"short revision":       with(func(c *delegation.Claims) { c.RowColumnPolicy.Revision = "abc" }),
		"upper revision":       with(func(c *delegation.Claims) { c.RowColumnPolicy.Revision = strings.Repeat("B", 64) }),
		"no columns":           with(func(c *delegation.Claims) { setTable(c, func(p *delegation.DelegatedTablePolicy) { p.Columns = nil }) }),
		"neither rows nor all_rows": with(func(c *delegation.Claims) {
			setTable(c, func(p *delegation.DelegatedTablePolicy) { p.Rows = nil })
		}),
		"both rows and all_rows": with(func(c *delegation.Claims) {
			setTable(c, func(p *delegation.DelegatedTablePolicy) { p.AllRows = true })
		}),
	}
	for name, c := range cases {
		if _, err := delegation.Sign(c, key); err == nil {
			t.Errorf("%s: Sign accepted it", name)
		}
		// A correctly signed token for the invalid claims proves verification refuses it independently of Sign.
		if _, err := delegation.VerifyClaims(forge(t, c, key), trust, time.Unix(c.IssuedAt+1, 0)); err == nil {
			t.Errorf("%s: VerifyClaims accepted a correctly signed token", name)
		}
	}
}

// forge signs claims without validation, as a compromised or buggy issuer might.
func forge(t *testing.T, c delegation.Claims, key ed25519.PrivateKey) string {
	t.Helper()
	h, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "JWT", "kid": c.Issuer})
	p, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(unsigned)))
}

func TestTamperedPolicyByteFailsSignature(t *testing.T) {
	c, key, trust, r := policyClaims(t, 2)
	token, err := delegation.Sign(c, key)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	start := strings.Index(string(payload), `"row_column_policy"`)
	if start < 0 {
		t.Fatal("fixture policy is not in the payload")
	}
	// Change every byte of the policy object in turn: none may verify.
	for i := start; i < len(payload); i++ {
		mutated := append([]byte(nil), payload...)
		mutated[i] ^= 1
		forged := parts[0] + "." + base64.RawURLEncoding.EncodeToString(mutated) + "." + parts[2]
		if _, err := delegation.Verify(forged, trust, r, time.Unix(c.IssuedAt+1, 0)); err == nil {
			t.Fatalf("byte %d of the policy was changed and the grant still verified", i)
		}
	}
	// Stripping the policy and downgrading to version 1 also fails: the signature covers both.
	stripped := c
	stripped.Version, stripped.RowColumnPolicy = 1, nil
	p, _ := json.Marshal(stripped)
	forged := parts[0] + "." + base64.RawURLEncoding.EncodeToString(p) + "." + parts[2]
	if _, err := delegation.Verify(forged, trust, r, time.Unix(c.IssuedAt+1, 0)); err == nil {
		t.Fatal("a policy stripped from a signed grant still verified")
	}
}
