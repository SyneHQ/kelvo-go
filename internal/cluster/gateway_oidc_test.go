// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/oidc"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestGatewayOIDCTokenGetsExactlyTheMappedPrincipalAuthority(t *testing.T) {
	enc := base64.RawURLEncoding
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/openid-configuration" {
			json.NewEncoder(w).Encode(map[string]string{"issuer": srv.URL, "jwks_uri": srv.URL + "/jwks"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{"kty": "RSA", "kid": "k", "n": enc.EncodeToString(key.N.Bytes()), "e": enc.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
	}))
	defer srv.Close()
	token := func(sub string) string {
		h, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": "k"})
		c, _ := json.Marshal(map[string]any{"iss": srv.URL, "aud": "kelvo", "sub": sub, "exp": time.Now().Add(time.Hour).Unix()})
		in := enc.EncodeToString(h) + "." + enc.EncodeToString(c)
		sum := sha256.Sum256([]byte(in))
		sig, _ := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
		return in + "." + enc.EncodeToString(sig)
	}

	p := rowPrincipalPolicy()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := GatewayConfig{Tenants: []TenantConfig{{Policy: p}}, Authentication: &GatewayAuthenticationConfig{OIDC: &oidc.Config{
		Issuer: srv.URL, Audiences: []string{"kelvo"}, Tenant: "a", Principals: map[string]string{"alice": "analyst"}, HTTPClient: srv.Client()}}}
	v, err := newGatewayOIDC(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Authentication.OIDC.Principals = map[string]string{"alice": "ghost"}
	if _, err := newGatewayOIDC(ctx, cfg); err == nil {
		t.Fatal("mapping to an unconfigured principal accepted")
	}

	analyst, _ := authorityForPrincipal(p, "analyst")
	reports, _ := authorityForPrincipal(p, "reports")
	job := func(id, source string, a JobAuthority) Snapshot {
		return Snapshot{Job: Job{ID: id, TenantID: "a", Authority: &a, State: Queued, ExpiresAt: time.Now().Add(time.Minute), Request: query.Request{Mode: "federated", Sources: []string{source}, SQL: "SELECT * FROM " + source}}}
	}
	st := &gatewayStore{policy: p, jobs: map[string]Snapshot{"mine": job("mine", "sales", analyst), "theirs": job("theirs", "daily_sales", reports)}}
	auth := bareAuthenticator(t)
	if !auth.apply(principalKeySet(t, 1, map[string][]string{"reports": {rotationNew}}, map[string][]string{}), time.Now()) {
		t.Fatal("auth")
	}
	g := &Gateway{tenants: map[string]gatewayTenant{"a": {store: st}}, auth: auth, oidc: v, permits: make(chan struct{}, 2), resultWaiters: make(chan struct{}, 2), ctx: context.Background()}
	do := func(method, path, bearer, body string) int {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+bearer)
		r.Header.Set("X-Kelvo-Principal", "reports")
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		return w.Code
	}
	// The job owned by analyst's exact authority (principal + policy digest,
	// which covers its grants and row/column policy) is visible; another
	// principal's job is not, and headers cannot change that.
	if c := do("GET", "/v1/queries/mine", token("alice"), ""); c != 200 {
		t.Fatalf("owner via OIDC: %d", c)
	}
	if c := do("GET", "/v1/queries/theirs", token("alice"), ""); c != 404 {
		t.Fatalf("foreign job via OIDC: %d", c)
	}
	if c := do("POST", "/v1/queries", token("alice"), `{"mode":"native","connection_id":"payroll","sql":"SELECT 1"}`); c != 403 {
		t.Fatalf("ungranted source via OIDC: %d", c)
	}
	if c := do("POST", "/v1/queries", token("alice"), `{"mode":"native","connection_id":"sales","sql":"SELECT 1"}`); c != 403 {
		t.Fatalf("row-restricted source natively via OIDC: %d", c)
	}
	if c := do("GET", "/v1/queries/mine", token("mallory"), ""); c != 401 {
		t.Fatalf("unmapped identity: %d", c)
	}
	// API keys keep working alongside OIDC.
	if c := do("GET", "/v1/queries/theirs", rotationNew, ""); c != 200 {
		t.Fatalf("API key: %d", c)
	}
}
