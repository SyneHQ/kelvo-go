// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package delegation

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func fixture(t *testing.T) (Claims, ed25519.PrivateKey, Trust, query.Request, string) {
	t.Helper()
	var f struct {
		SeedHex   string `json:"seed_hex"`
		PublicKey string `json:"public_key"`
		Claims    Claims `json:"claims"`
		Token     string `json:"token"`
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
	trust := Trust{f.Claims.Issuer, f.Claims.Audience, f.Claims.ClusterTenant, f.Claims.ServicePrincipal, ed25519.PublicKey(pub)}
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
			digest, err := QueryDigest(c.Request)
			if err != nil || digest != c.SHA256 || Digest(c.Canonical) != c.SHA256 {
				t.Fatal("query digest changed", digest, err)
			}
		})
	}
}
func TestExternalEd25519GoldenGrant(t *testing.T) {
	c, key, trust, r, token := fixture(t)
	actual, err := Sign(c, key)
	if err != nil || actual != token {
		t.Fatal("signed grant differs from independent Ed25519 fixture", err)
	}
	verified, err := Verify(token, trust, r, time.Unix(c.IssuedAt+1, 0))
	if err != nil || verified.AppTeam != c.AppTeam {
		t.Fatal("golden grant was rejected", err)
	}
	for name, change := range map[string]func(*query.Request){"sql": func(r *query.Request) { r.SQL = "SELECT 2" }, "connection": func(r *query.Request) { r.ConnectionID = "another" }, "parameter": func(r *query.Request) {
		r.Parameters = []query.Parameter{{Type: "int64", Value: json.RawMessage(`"9007199254740992"`)}}
	}, "mode": func(r *query.Request) { r.Mode = "federated"; r.ConnectionID = ""; r.Sources = []string{"source_1"} }} {
		t.Run(name, func(t *testing.T) {
			other := r
			change(&other)
			if _, err := Verify(token, trust, other, time.Unix(c.IssuedAt+1, 0)); err == nil {
				t.Fatal("altered query authorized")
			}
		})
	}
	for _, at := range []int64{c.IssuedAt - 31, c.ExpiresAt, c.ExpiresAt + 1} {
		if _, err := Verify(token, trust, r, time.Unix(at, 0)); err == nil {
			t.Fatal("invalid grant lifetime authorized")
		}
	}
	for _, change := range []func(*Trust){func(t *Trust) { t.Issuer = "other" }, func(t *Trust) { t.Audience = "other" }, func(t *Trust) { t.ClusterTenant = "other" }, func(t *Trust) { t.ServicePrincipal = "other" }, func(t *Trust) { t.PublicKey = make(ed25519.PublicKey, 32) }} {
		other := trust
		change(&other)
		if _, err := Verify(token, other, r, time.Unix(c.IssuedAt+1, 0)); err == nil {
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
		if _, err := Verify(signedParts(key, header, string(raw)), trust, r, time.Unix(c.IssuedAt, 0)); err == nil {
			t.Fatal("ambiguous JOSE header authorized")
		}
	}
	header := `{"alg":"EdDSA","typ":"JWT","kid":"api-resolver"}`
	for _, payload := range []string{strings.Replace(string(raw), `"version":1`, `"version":1,"version":1`, 1), strings.Replace(string(raw), `"version":1`, `"version":1,"VERSION":1`, 1), strings.Replace(string(raw), `"version":1`, `"version":1,"extra":true`, 1), string(raw) + `{}`, strings.Replace(string(raw), `team-a`, string([]byte{0xff}), 1)} {
		if _, err := Verify(signedParts(key, header, payload), trust, r, time.Unix(c.IssuedAt, 0)); err == nil {
			t.Fatal("ambiguous claims authorized")
		}
	}
	for _, bad := range []string{"", token + ".", strings.Repeat("x", MaxTokenBytes+1), token[:len(token)-1] + "!"} {
		if _, err := Verify(bad, trust, r, time.Unix(c.IssuedAt, 0)); err == nil {
			t.Fatal("malformed token authorized")
		}
	}
}

func TestPrivateEnvelopeJSONLimits(t *testing.T) {
	var value struct {
		Value string `json:"value"`
	}
	raw := []byte(`{"value":"` + strings.Repeat("x", MaxTokenBytes) + `"}`)
	if StrictJSON(raw, &value) == nil {
		t.Fatal("public grant bound was exceeded")
	}
	if StrictJSONLimit(raw, &value, 1<<20) != nil || len(value.Value) != MaxTokenBytes {
		t.Fatal("bounded private envelope was rejected")
	}
	for _, limit := range []int{0, 1, 1<<20 + 1} {
		if StrictJSONLimit(raw, &value, limit) == nil {
			t.Fatal("invalid envelope limit")
		}
	}
	if StrictJSONLimit([]byte(`{"value":"a","VALUE":"b"}`), &value, 1<<20) == nil {
		t.Fatal("duplicate private envelope member accepted")
	}
}
func TestScopeAndJobGrantsFailClosed(t *testing.T) {
	c, key, _, _, _ := fixture(t)
	for name, mutate := range map[string]func(*Claims){
		"empty sources": func(c *Claims) { c.Sources = nil }, "invalid nonce": func(c *Claims) { c.ID = "short" }, "long lifetime": func(c *Claims) { c.ExpiresAt = c.IssuedAt + 301 }, "reserved subject": func(c *Claims) { c.Subject.Kind = "service" },
		"foreign job fields": func(c *Claims) { c.Subject.JobID = "job" }, "duplicate alias": func(c *Claims) { c.Sources = append(c.Sources, Source{Alias: "SOURCE_1", ConnectionID: "saved-b"}) },
		"duplicate connection": func(c *Claims) { c.Sources = append(c.Sources, Source{Alias: "another", ConnectionID: "saved-a"}) }, "duplicate table": func(c *Claims) {
			c.Sources[0].Tables = []Table{{Name: "orders", Table: "orders"}, {Name: "ORDERS", Table: "other"}}
		},
		"job scope mismatch": func(c *Claims) {
			c.Subject = Subject{Kind: "job", ID: "user-a", JobID: "job-a", JobExpiresAt: c.ExpiresAt, JobConnections: map[string]string{"other": "read"}}
		},
		"job expiry": func(c *Claims) {
			c.Subject = Subject{Kind: "job", ID: "user-a", JobID: "job-a", JobExpiresAt: c.ExpiresAt - 1, JobConnections: map[string]string{"saved-a": "read"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			other := cloneClaims(c)
			mutate(&other)
			if _, err := Sign(other, key); err == nil {
				t.Fatal("invalid scope signed")
			}
		})
	}
}
func TestExecutionContextOwnsClaims(t *testing.T) {
	c, _, _, _, token := fixture(t)
	c.Subject = Subject{Kind: "job", ID: "user-a", JobConnections: map[string]string{"saved-a": "read"}}
	c.Sources[0].Tables = []Table{{Name: "orders", Table: "orders"}}
	ctx := WithExecution(context.Background(), c, token, ExecutionBinding{JobID: "job-a", WorkerID: "worker-a"})
	c.Subject.JobConnections["saved-a"] = "write"
	c.Sources[0].Tables[0].Table = "other"
	a, ok := ExecutionFromContext(ctx)
	if !ok || a.Claims.Subject.JobConnections["saved-a"] != "read" || a.Claims.Sources[0].Tables[0].Table != "orders" {
		t.Fatal("caller mutated execution authority")
	}
	a.Claims.Subject.JobConnections["saved-a"] = "write"
	a.Claims.Sources[0].Tables[0].Table = "other"
	b, _ := ExecutionFromContext(ctx)
	if b.Claims.Subject.JobConnections["saved-a"] != "read" || b.Claims.Sources[0].Tables[0].Table != "orders" {
		t.Fatal("reader mutated execution authority")
	}
}
