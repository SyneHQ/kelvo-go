// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package oidc

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var enc = base64.RawURLEncoding

type provider struct {
	srv  *httptest.Server
	mu   sync.Mutex
	keys []map[string]string
	down bool
}

func newProvider(t *testing.T) *provider {
	p := &provider{}
	p.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.down {
			w.WriteHeader(503)
			return
		}
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			json.NewEncoder(w).Encode(map[string]string{"issuer": p.srv.URL, "jwks_uri": p.srv.URL + "/jwks"})
		case "/jwks":
			json.NewEncoder(w).Encode(map[string]any{"keys": p.keys})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *provider) set(keys ...map[string]string) {
	p.mu.Lock()
	p.keys = keys
	p.mu.Unlock()
}

func rsaJWK(kid string, k *rsa.PrivateKey) map[string]string {
	return map[string]string{"kty": "RSA", "kid": kid, "n": enc.EncodeToString(k.N.Bytes()), "e": enc.EncodeToString(big.NewInt(int64(k.E)).Bytes())}
}

func sign(t *testing.T, alg, kid string, key any, claims map[string]any) string {
	t.Helper()
	h := map[string]string{"alg": alg, "typ": "JWT"}
	if kid != "" {
		h["kid"] = kid
	}
	hb, _ := json.Marshal(h)
	cb, _ := json.Marshal(claims)
	in := enc.EncodeToString(hb) + "." + enc.EncodeToString(cb)
	sum := sha256.Sum256([]byte(in))
	var sig []byte
	var err error
	switch k := key.(type) {
	case *rsa.PrivateKey:
		sig, err = rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, sum[:])
	case *ecdsa.PrivateKey:
		r, s, e := ecdsa.Sign(rand.Reader, k, sum[:])
		err = e
		sig = append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...)
	case ed25519.PrivateKey:
		sig = ed25519.Sign(k, []byte(in))
	case []byte:
		m := hmac.New(sha256.New, k)
		m.Write([]byte(in))
		sig = m.Sum(nil)
	case nil:
	}
	if err != nil {
		t.Fatal(err)
	}
	return in + "." + enc.EncodeToString(sig)
}

func setup(t *testing.T, mutate func(*Config)) (*provider, *Verifier, *rsa.PrivateKey) {
	t.Helper()
	p := newProvider(t)
	k, _ := rsa.GenerateKey(rand.Reader, 2048)
	p.set(rsaJWK("k1", k))
	cfg := Config{Issuer: p.srv.URL, Audiences: []string{"kelvo"}, Tenant: "a", Principals: map[string]string{"alice": "analyst", "bob": "reports"},
		Groups: map[string]string{"finance": "analyst", "ops": "reports"}, HTTPClient: p.srv.Client()}
	if mutate != nil {
		mutate(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	v, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p, v, k
}

func base(iss string, now time.Time) map[string]any {
	return map[string]any{"iss": iss, "aud": "kelvo", "sub": "alice", "exp": now.Add(time.Hour).Unix(), "iat": now.Unix(), "nbf": now.Unix()}
}

func with(c map[string]any, k string, v any) map[string]any {
	out := map[string]any{}
	for a, b := range c {
		out[a] = b
	}
	if v == nil {
		delete(out, k)
	} else {
		out[k] = v
	}
	return out
}

func TestVerifyClaimsAndAlgorithms(t *testing.T) {
	p, v, k := setup(t, nil)
	now := time.Now()
	iss := p.srv.URL
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	der := x509.MarshalPKCS1PublicKey(&k.PublicKey)
	ok := base(iss, now)
	for _, tt := range []struct {
		name  string
		token string
		want  string
	}{
		{"valid", sign(t, "RS256", "k1", k, ok), "analyst"},
		{"valid without kid, single key", sign(t, "RS256", "", k, ok), "analyst"},
		{"aud array", sign(t, "RS256", "k1", k, with(ok, "aud", []string{"x", "kelvo"})), "analyst"},
		{"group only", sign(t, "RS256", "k1", k, with(with(ok, "sub", "nobody"), "groups", []string{"ops"})), "reports"},
		{"same principal twice", sign(t, "RS256", "k1", k, with(ok, "groups", []string{"finance"})), "analyst"},
		{"expired", sign(t, "RS256", "k1", k, with(ok, "exp", now.Add(-time.Minute).Unix())), ""},
		{"missing exp", sign(t, "RS256", "k1", k, with(ok, "exp", nil)), ""},
		{"nbf future", sign(t, "RS256", "k1", k, with(ok, "nbf", now.Add(time.Hour).Unix())), ""},
		{"iat future", sign(t, "RS256", "k1", k, with(ok, "iat", now.Add(time.Hour).Unix())), ""},
		{"wrong iss", sign(t, "RS256", "k1", k, with(ok, "iss", "https://evil.example")), ""},
		{"wrong aud", sign(t, "RS256", "k1", k, with(ok, "aud", "other")), ""},
		{"alg none", sign(t, "none", "k1", nil, ok), ""},
		{"HS256 with RSA public key as secret", sign(t, "HS256", "k1", der, ok), ""},
		{"HS256 with JWK n as secret", sign(t, "HS256", "k1", k.N.Bytes(), ok), ""},
		{"unknown kid", sign(t, "RS256", "k9", k, ok), ""},
		{"wrong signer", sign(t, "RS256", "k1", other, ok), ""},
		{"ES256 header on RSA key", sign(t, "ES256", "k1", k, ok), ""},
		{"unmapped identity", sign(t, "RS256", "k1", k, with(ok, "sub", "mallory")), ""},
		{"ambiguous mapping", sign(t, "RS256", "k1", k, with(ok, "groups", []string{"ops"})), ""},
		{"garbage", "a.b.c", ""},
	} {
		got, _, err := v.Verify(context.Background(), tt.token, now)
		if got != tt.want || (tt.want == "") != (err != nil) {
			t.Errorf("%s: got %q, %v", tt.name, got, err)
		}
	}
}

func TestRotationAndKidRequirement(t *testing.T) {
	p, v, k1 := setup(t, nil)
	now := time.Now()
	k2, _ := rsa.GenerateKey(rand.Reader, 2048)
	claims := base(p.srv.URL, now)
	// A new kid is accepted once a refresh observes it.
	p.set(rsaJWK("k1", k1), rsaJWK("k2", k2))
	v.mu.Lock()
	v.lastTry = time.Time{}
	v.mu.Unlock()
	if got, _, err := v.Verify(context.Background(), sign(t, "RS256", "k2", k2, claims), now); err != nil || got != "analyst" {
		t.Fatalf("rotated key rejected: %v", err)
	}
	// More than one key: kid is mandatory.
	if _, _, err := v.Verify(context.Background(), sign(t, "RS256", "", k1, claims), now); err == nil {
		t.Fatal("missing kid accepted with two keys")
	}
	// The provider drops k1; after refresh it no longer verifies.
	p.set(rsaJWK("k2", k2))
	if !v.refresh(context.Background()) {
		t.Fatal("refresh")
	}
	if _, _, err := v.Verify(context.Background(), sign(t, "RS256", "k1", k1, claims), now); err == nil {
		t.Fatal("removed kid accepted")
	}
	// Provider outage: cached keys last for CacheTTL, then everything fails closed.
	p.mu.Lock()
	p.down = true
	p.mu.Unlock()
	tok := sign(t, "RS256", "k2", k2, claims)
	if _, _, err := v.Verify(context.Background(), tok, now); err != nil {
		t.Fatalf("cached key rejected during outage: %v", err)
	}
	later := now.Add(v.cfg.CacheTTL + time.Second)
	if _, _, err := v.Verify(context.Background(), sign(t, "RS256", "k2", k2, with(claims, "exp", later.Add(time.Hour).Unix())), later); err == nil {
		t.Fatal("stale key set accepted after TTL")
	}
}

func TestOtherKeyTypesAndExplicitHMAC(t *testing.T) {
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	edPub, edPriv, _ := ed25519.GenerateKey(rand.Reader)
	secret := []byte("0123456789abcdef0123456789abcdef")
	file := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(file, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	p, v, _ := setup(t, func(c *Config) {
		c.Algorithms = []string{"ES256", "EdDSA", "HS256"}
		c.HMACSecretFile = file
	})
	p.set(map[string]string{"kty": "EC", "crv": "P-256", "kid": "e", "x": enc.EncodeToString(ec.X.FillBytes(make([]byte, 32))), "y": enc.EncodeToString(ec.Y.FillBytes(make([]byte, 32)))},
		map[string]string{"kty": "OKP", "crv": "Ed25519", "kid": "d", "x": enc.EncodeToString(edPub)})
	if !v.refresh(context.Background()) {
		t.Fatal("refresh")
	}
	now := time.Now()
	c := base(p.srv.URL, now)
	for name, tok := range map[string]string{"ES256": sign(t, "ES256", "e", ec, c), "EdDSA": sign(t, "EdDSA", "d", edPriv, c), "HS256": sign(t, "HS256", "", secret, c)} {
		if got, _, err := v.Verify(context.Background(), tok, now); err != nil || got != "analyst" {
			t.Errorf("%s rejected: %v", name, err)
		}
	}
	if _, _, err := v.Verify(context.Background(), sign(t, "RS256", "e", ec, c), now); err == nil {
		t.Error("RS256 accepted outside the allowlist")
	}
}

func TestConfigFailsClosed(t *testing.T) {
	good := Config{Issuer: "https://idp.example", Audiences: []string{"kelvo"}, Tenant: "a", Principals: map[string]string{"x": "y"}}
	for name, mutate := range map[string]func(*Config){
		"http issuer":          func(c *Config) { c.Issuer = "http://idp.example" },
		"no audience":          func(c *Config) { c.Audiences = nil },
		"no mapping":           func(c *Config) { c.Principals = nil },
		"HS256 without file":   func(c *Config) { c.Algorithms = []string{"HS256"} },
		"none algorithm":       func(c *Config) { c.Algorithms = []string{"none"} },
		"HS512":                func(c *Config) { c.Algorithms = []string{"HS512"} },
		"skew too large":       func(c *Config) { c.ClockSkew = time.Hour },
		"ttl too small":        func(c *Config) { c.CacheTTL = time.Second },
		"secret without HS256": func(c *Config) { c.HMACSecretFile = "/nonexistent" },
	} {
		c := good
		mutate(&c)
		if _, err := New(context.Background(), c); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
