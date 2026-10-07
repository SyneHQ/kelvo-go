// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package delegation_test

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/query"
)

func fixture(t *testing.T) (delegation.Claims, ed25519.PrivateKey, delegation.Trust, query.Request, string) {
	t.Helper()
	var f struct {
		SeedHex   string            `json:"seed_hex"`
		PublicKey string            `json:"public_key"`
		Claims    delegation.Claims `json:"claims"`
		Token     string            `json:"token"`
	}
	raw, err := os.ReadFile("testdata/signed-grant.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	seed, err := hex.DecodeString(f.SeedHex)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := base64.StdEncoding.DecodeString(f.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	r := query.Request{Mode: "native", SQL: "SELECT ? AS exact", ConnectionID: "source_1", Parameters: []query.Parameter{{Type: "int64", Value: json.RawMessage(`"9007199254740993"`)}}}
	trust := delegation.Trust{
		Issuer: f.Claims.Issuer, Audience: f.Claims.Audience,
		ClusterTenant: f.Claims.ClusterTenant, ServicePrincipal: f.Claims.ServicePrincipal,
		PublicKey: ed25519.PublicKey(pub),
	}
	return f.Claims, ed25519.NewKeyFromSeed(seed), trust, r, f.Token
}

func TestGoldenQueryDigests(t *testing.T) {
	var cases []struct {
		Name      string
		Request   query.Request
		Canonical string
		SHA256    string
	}
	raw, err := os.ReadFile("testdata/query-digests.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			digest, err := delegation.QueryDigest(c.Request)
			if err != nil || digest != c.SHA256 || delegation.Digest(c.Canonical) != c.SHA256 {
				t.Fatal("query digest changed", digest, err)
			}
		})
	}
}
func TestExternalEd25519GoldenGrant(t *testing.T) {
	c, key, trust, r, token := fixture(t)
	actual, err := delegation.Sign(c, key)
	if err != nil || actual != token {
		t.Fatal("signed grant differs from independent Ed25519 fixture", err)
	}
	verified, err := delegation.Verify(token, trust, r, time.Unix(c.IssuedAt+1, 0))
	if err != nil || verified.AppTeam != c.AppTeam {
		t.Fatal("golden grant was rejected", err)
	}
	for name, change := range map[string]func(*query.Request){"sql": func(r *query.Request) { r.SQL = "SELECT 2" }, "connection": func(r *query.Request) { r.ConnectionID = "another" }, "parameter": func(r *query.Request) {
		r.Parameters = []query.Parameter{{Type: "int64", Value: json.RawMessage(`"9007199254740992"`)}}
	}, "mode": func(r *query.Request) { r.Mode = "federated"; r.ConnectionID = ""; r.Sources = []string{"source_1"} }} {
		t.Run(name, func(t *testing.T) {
			other := r
			change(&other)
			if _, err := delegation.Verify(token, trust, other, time.Unix(c.IssuedAt+1, 0)); err == nil {
				t.Fatal("altered query authorized")
			}
		})
	}
	for _, at := range []int64{c.IssuedAt - 31, c.ExpiresAt, c.ExpiresAt + 1} {
		if _, err := delegation.Verify(token, trust, r, time.Unix(at, 0)); err == nil {
			t.Fatal("invalid grant lifetime authorized")
		}
	}
	for _, change := range []func(*delegation.Trust){func(t *delegation.Trust) { t.Issuer = "other" }, func(t *delegation.Trust) { t.Audience = "other" }, func(t *delegation.Trust) { t.ClusterTenant = "other" }, func(t *delegation.Trust) { t.ServicePrincipal = "other" }, func(t *delegation.Trust) { t.PublicKey = make(ed25519.PublicKey, 32) }} {
		other := trust
		change(&other)
		if _, err := delegation.Verify(token, other, r, time.Unix(c.IssuedAt+1, 0)); err == nil {
			t.Fatal("grant crossed trust boundary")
		}
	}
}
func signedParts(key ed25519.PrivateKey, header, payload string) string {
	unsigned := base64.RawURLEncoding.EncodeToString([]byte(header)) + "." + base64.RawURLEncoding.EncodeToString([]byte(payload))
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(unsigned)))
}
func TestRejectAmbiguousAndUntrustedEnvelopes(t *testing.T) {
	c, key, trust, r, token := fixture(t)
	raw, _ := json.Marshal(c)
	for _, header := range []string{`{"ALG":"EdDSA","typ":"JWT","kid":"api-resolver"}`, `{"alg":"none","typ":"JWT","kid":"api-resolver"}`, `{"alg":"EdDSA","typ":"JWT","kid":"api-resolver","jku":"https://untrusted.test"}`, `{"alg":"EdDSA","ALG":"EdDSA","typ":"JWT","kid":"api-resolver"}`, `{"alg":"EdDSA","typ":"JWT","kid":"other"}`} {
		if _, err := delegation.Verify(signedParts(key, header, string(raw)), trust, r, time.Unix(c.IssuedAt, 0)); err == nil {
			t.Fatal("ambiguous JOSE header authorized")
		}
	}
	header := `{"alg":"EdDSA","typ":"JWT","kid":"api-resolver"}`
	for _, payload := range []string{strings.Replace(string(raw), `"version":1`, `"version":1,"version":1`, 1), strings.Replace(string(raw), `"version":1`, `"version":1,"VERSION":1`, 1), strings.Replace(string(raw), `"version":1`, `"version":1,"extra":true`, 1), string(raw) + `{}`, strings.Replace(string(raw), `team-a`, string([]byte{0xff}), 1)} {
		if _, err := delegation.Verify(signedParts(key, header, payload), trust, r, time.Unix(c.IssuedAt, 0)); err == nil {
			t.Fatal("ambiguous claims authorized")
		}
	}
	for _, bad := range []string{"", token + ".", strings.Repeat("x", delegation.MaxTokenBytes+1), token[:len(token)-1] + "!"} {
		if _, err := delegation.Verify(bad, trust, r, time.Unix(c.IssuedAt, 0)); err == nil {
			t.Fatal("malformed token authorized")
		}
	}
}

func TestPrivateEnvelopeJSONLimits(t *testing.T) {
	var value struct {
		Value string `json:"value"`
	}
	raw := []byte(`{"value":"` + strings.Repeat("x", delegation.MaxTokenBytes) + `"}`)
	if delegation.StrictJSON(raw, &value) == nil {
		t.Fatal("public grant bound was exceeded")
	}
	if delegation.StrictJSONLimit(raw, &value, 1<<20) != nil || len(value.Value) != delegation.MaxTokenBytes {
		t.Fatal("bounded private envelope was rejected")
	}
	for _, limit := range []int{0, 1, 1<<20 + 1} {
		if delegation.StrictJSONLimit(raw, &value, limit) == nil {
			t.Fatal("invalid envelope limit")
		}
	}
	if delegation.StrictJSONLimit([]byte(`{"value":"a","VALUE":"b"}`), &value, 1<<20) == nil {
		t.Fatal("duplicate private envelope member accepted")
	}
}
func TestScopeAndJobGrantsFailClosed(t *testing.T) {
	for name, mutate := range map[string]func(*delegation.Claims){
		"empty sources": func(c *delegation.Claims) { c.Sources = nil }, "invalid nonce": func(c *delegation.Claims) { c.ID = "short" }, "long lifetime": func(c *delegation.Claims) { c.ExpiresAt = c.IssuedAt + 301 }, "reserved subject": func(c *delegation.Claims) { c.Subject.Kind = "service" },
		"foreign job fields": func(c *delegation.Claims) { c.Subject.JobID = "job" }, "duplicate alias": func(c *delegation.Claims) {
			c.Sources = append(c.Sources, delegation.Source{Alias: "SOURCE_1", ConnectionID: "saved-b"})
		},
		"duplicate connection": func(c *delegation.Claims) {
			c.Sources = append(c.Sources, delegation.Source{Alias: "another", ConnectionID: "saved-a"})
		}, "duplicate table": func(c *delegation.Claims) {
			c.Sources[0].Tables = []delegation.Table{{Name: "orders", Table: "orders"}, {Name: "ORDERS", Table: "other"}}
		},
		"job scope mismatch": func(c *delegation.Claims) {
			c.Subject = delegation.Subject{Kind: "job", ID: "user-a", JobID: "job-a", JobExpiresAt: c.ExpiresAt, JobConnections: map[string]string{"other": "read"}}
		},
		"job expiry": func(c *delegation.Claims) {
			c.Subject = delegation.Subject{Kind: "job", ID: "user-a", JobID: "job-a", JobExpiresAt: c.ExpiresAt - 1, JobConnections: map[string]string{"saved-a": "read"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			other, key, _, _, _ := fixture(t)
			mutate(&other)
			if _, err := delegation.Sign(other, key); err == nil {
				t.Fatal("invalid scope signed")
			}
		})
	}
}
