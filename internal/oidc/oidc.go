// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package oidc verifies OIDC/JWT bearer tokens and maps a validated identity
// onto one existing, operator-configured principal. It never provisions
// principals and never logs tokens. Every failure is a rejection.
package oidc

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

// Config is the gateway's `authentication.oidc` block.
type Config struct {
	Issuer    string   `yaml:"issuer"`
	JWKSURL   string   `yaml:"jwks_url,omitempty"` // Empty: OIDC discovery from the issuer.
	Audiences []string `yaml:"audiences"`
	Tenant    string   `yaml:"tenant"`
	// Claim names the identity claim (default "sub"); Principals maps its exact
	// values to principal IDs. GroupsClaim (default "groups") values map through Groups.
	Claim       string            `yaml:"claim,omitempty"`
	Principals  map[string]string `yaml:"principals,omitempty"`
	GroupsClaim string            `yaml:"groups_claim,omitempty"`
	Groups      map[string]string `yaml:"groups,omitempty"`
	Algorithms  []string          `yaml:"algorithms,omitempty"` // Default RS256, ES256, EdDSA.
	// HMACSecretFile enables HS256 only when HS256 is also listed in Algorithms.
	HMACSecretFile string        `yaml:"hmac_secret_file,omitempty"`
	ClockSkew      time.Duration `yaml:"clock_skew,omitempty"` // Default 30s, at most 5m.
	CacheTTL       time.Duration `yaml:"cache_ttl,omitempty"`  // Default 10m, 1m..24h.
	HTTPClient     *http.Client  `yaml:"-"`
}

var (
	ErrConfig   = errors.New("oidc: invalid configuration")
	ErrRejected = errors.New("oidc: token rejected")
)

const (
	maxToken      = 16 << 10
	maxDocument   = 1 << 20
	maxKeys       = 64
	maxMappings   = 1024
	refetchFloor  = 10 * time.Second
	fetchDeadline = 10 * time.Second
)

type Verifier struct {
	cfg    Config
	secret []byte
	client *http.Client

	mu        sync.Mutex
	keys      map[string]crypto.PublicKey
	fetchedAt time.Time
	lastTry   time.Time
	jwksURL   string
}

func validURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.Fragment == "" && len(raw) <= 2048
}

// New validates cfg, performs a best-effort initial key fetch and refreshes
// keys in the background until ctx ends. An unreachable provider leaves the
// verifier rejecting every token, not failing gateway startup.
func New(ctx context.Context, cfg Config) (*Verifier, error) {
	if cfg.Claim == "" {
		cfg.Claim = "sub"
	}
	if cfg.GroupsClaim == "" {
		cfg.GroupsClaim = "groups"
	}
	if len(cfg.Algorithms) == 0 {
		cfg.Algorithms = []string{"RS256", "ES256", "EdDSA"}
	}
	if cfg.ClockSkew == 0 {
		cfg.ClockSkew = 30 * time.Second
	}
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = 10 * time.Minute
	}
	if !validURL(cfg.Issuer) || (cfg.JWKSURL != "" && !validURL(cfg.JWKSURL)) || cfg.Tenant == "" ||
		len(cfg.Audiences) == 0 || len(cfg.Audiences) > 16 || slices.Contains(cfg.Audiences, "") ||
		len(cfg.Principals)+len(cfg.Groups) == 0 || len(cfg.Principals)+len(cfg.Groups) > maxMappings ||
		cfg.ClockSkew < 0 || cfg.ClockSkew > 5*time.Minute || cfg.CacheTTL < time.Minute || cfg.CacheTTL > 24*time.Hour {
		return nil, ErrConfig
	}
	for _, alg := range cfg.Algorithms {
		switch alg {
		case "RS256", "ES256", "EdDSA":
		case "HS256":
			if cfg.HMACSecretFile == "" {
				return nil, ErrConfig
			}
		default:
			return nil, ErrConfig
		}
	}
	v := &Verifier{cfg: cfg, client: cfg.HTTPClient, jwksURL: cfg.JWKSURL}
	if cfg.HMACSecretFile != "" {
		if !slices.Contains(cfg.Algorithms, "HS256") {
			return nil, ErrConfig
		}
		raw, err := os.ReadFile(cfg.HMACSecretFile)
		v.secret = []byte(strings.TrimSpace(string(raw)))
		if err != nil || len(v.secret) < 32 {
			return nil, ErrConfig
		}
	}
	if v.client == nil {
		v.client = &http.Client{Timeout: fetchDeadline}
	}
	v.refresh(ctx)
	go func() {
		t := time.NewTicker(cfg.CacheTTL / 2)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				v.refresh(ctx)
			}
		}
	}()
	return v, nil
}

// Tenant is the gateway tenant whose principals the mapping names.
func (v *Verifier) Tenant() string { return v.cfg.Tenant }

// MappedPrincipals lists every principal ID the mapping can produce, for
// startup validation against the tenant policy.
func (v *Verifier) MappedPrincipals() []string {
	var out []string
	for _, m := range []map[string]string{v.cfg.Principals, v.cfg.Groups} {
		for _, p := range m {
			out = append(out, p)
		}
	}
	return out
}

func (v *Verifier) getJSON(ctx context.Context, raw string, out any) error {
	ctx, cancel := context.WithTimeout(ctx, fetchDeadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return err
	}
	res, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return errors.New("oidc: provider status")
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxDocument+1))
	if err != nil || len(body) > maxDocument {
		return errors.New("oidc: provider document")
	}
	return json.Unmarshal(body, out)
}

// refresh replaces the whole key set on success, so a key the provider removed
// stops verifying after the next refresh. On failure the prior set survives
// only until fetchedAt+CacheTTL.
func (v *Verifier) refresh(ctx context.Context) bool {
	v.mu.Lock()
	v.lastTry = time.Now()
	jwks := v.jwksURL
	v.mu.Unlock()
	if jwks == "" {
		var doc struct {
			Issuer  string `json:"issuer"`
			JWKSURI string `json:"jwks_uri"`
		}
		if v.getJSON(ctx, strings.TrimSuffix(v.cfg.Issuer, "/")+"/.well-known/openid-configuration", &doc) != nil ||
			doc.Issuer != v.cfg.Issuer || !validURL(doc.JWKSURI) {
			return false
		}
		jwks = doc.JWKSURI
	}
	var set struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if v.getJSON(ctx, jwks, &set) != nil || len(set.Keys) == 0 || len(set.Keys) > maxKeys {
		return false
	}
	keys := map[string]crypto.PublicKey{}
	for _, raw := range set.Keys {
		kid, key, ok := parseJWK(raw)
		if !ok {
			continue
		}
		if _, dup := keys[kid]; dup {
			return false // Two keys under one kid are ambiguous.
		}
		keys[kid] = key
	}
	if len(keys) == 0 {
		return false
	}
	v.mu.Lock()
	v.keys, v.fetchedAt, v.jwksURL = keys, time.Now(), jwks
	v.mu.Unlock()
	return true
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Crv string `json:"crv"`
	N   string `json:"n"`
	E   string `json:"e"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func b64(s string) []byte {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

// parseJWK accepts public signature keys only; the key's own alg, when set,
// is recorded by type so a header cannot select a different family.
func parseJWK(raw json.RawMessage) (string, crypto.PublicKey, bool) {
	var k jwk
	if json.Unmarshal(raw, &k) != nil || (k.Use != "" && k.Use != "sig") || len(k.Kid) > 256 {
		return "", nil, false
	}
	switch k.Kty {
	case "RSA":
		n, e := b64(k.N), b64(k.E)
		if len(n) < 256 || len(n) > 1024 || len(e) == 0 || len(e) > 4 || (k.Alg != "" && k.Alg != "RS256") {
			return "", nil, false
		}
		exp := int(new(big.Int).SetBytes(e).Int64())
		if exp < 3 || exp%2 == 0 {
			return "", nil, false
		}
		return k.Kid, &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: exp}, true
	case "EC":
		x, y := b64(k.X), b64(k.Y)
		if k.Crv != "P-256" || len(x) != 32 || len(y) != 32 || (k.Alg != "" && k.Alg != "ES256") {
			return "", nil, false
		}
		key, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), append(append([]byte{4}, x...), y...))
		if err != nil {
			return "", nil, false
		}
		return k.Kid, key, true
	case "OKP":
		x := b64(k.X)
		if k.Crv != "Ed25519" || len(x) != ed25519.PublicKeySize || (k.Alg != "" && k.Alg != "EdDSA") {
			return "", nil, false
		}
		return k.Kid, ed25519.PublicKey(x), true
	}
	return "", nil, false
}

// key returns the verification key for kid, refetching at most once per
// refetchFloor when kid is unknown so a newly rotated key is accepted promptly.
func (v *Verifier) key(ctx context.Context, kid string, now time.Time) (crypto.PublicKey, bool) {
	for attempt := 0; attempt < 2; attempt++ {
		v.mu.Lock()
		fresh := v.keys != nil && now.Before(v.fetchedAt.Add(v.cfg.CacheTTL))
		var key crypto.PublicKey
		found := false
		if fresh {
			if kid == "" {
				if len(v.keys) == 1 {
					for _, k := range v.keys {
						key, found = k, true
					}
				}
			} else {
				key, found = v.keys[kid]
			}
		}
		retry := time.Since(v.lastTry) >= refetchFloor
		v.mu.Unlock()
		if found {
			return key, true
		}
		if attempt > 0 || !retry || (fresh && kid == "") || !v.refresh(ctx) {
			return nil, false
		}
	}
	return nil, false
}

type claims struct {
	Iss string          `json:"iss"`
	Aud json.RawMessage `json:"aud"`
	Exp *json.Number    `json:"exp"`
	Nbf *json.Number    `json:"nbf"`
	Iat *json.Number    `json:"iat"`
}

func unix(n *json.Number) (time.Time, bool) {
	if n == nil {
		return time.Time{}, false
	}
	f, err := n.Float64()
	if err != nil || f < 0 || f > 1<<40 {
		return time.Time{}, false
	}
	return time.Unix(int64(f), 0), true
}

// Verify validates token and returns the single principal it maps to and the
// token expiry. Any failure, including ambiguity, returns ErrRejected.
func (v *Verifier) Verify(ctx context.Context, token string, now time.Time) (string, time.Time, error) {
	principal, exp, ok := v.verify(ctx, token, now)
	if !ok {
		return "", time.Time{}, ErrRejected
	}
	return principal, exp, nil
}

func (v *Verifier) verify(ctx context.Context, token string, now time.Time) (string, time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(token) > maxToken || len(parts) != 3 {
		return "", time.Time{}, false
	}
	var header struct {
		Alg  string          `json:"alg"`
		Kid  string          `json:"kid"`
		Crit json.RawMessage `json:"crit"`
	}
	rawHeader, rawClaims, sig := b64(parts[0]), b64(parts[1]), b64(parts[2])
	if rawHeader == nil || rawClaims == nil || sig == nil || json.Unmarshal(rawHeader, &header) != nil ||
		header.Crit != nil || !slices.Contains(v.cfg.Algorithms, header.Alg) {
		return "", time.Time{}, false
	}
	signed := []byte(parts[0] + "." + parts[1])
	digest := sha256.Sum256(signed)
	if header.Alg == "HS256" {
		// The shared secret is the only HS256 key; a JWKS public key never is.
		mac := hmac.New(sha256.New, v.secret)
		mac.Write(signed)
		if len(v.secret) == 0 || !hmac.Equal(sig, mac.Sum(nil)) {
			return "", time.Time{}, false
		}
	} else {
		key, ok := v.key(ctx, header.Kid, now)
		if !ok {
			return "", time.Time{}, false
		}
		switch k := key.(type) {
		case *rsa.PublicKey:
			if header.Alg != "RS256" || rsa.VerifyPKCS1v15(k, crypto.SHA256, digest[:], sig) != nil {
				return "", time.Time{}, false
			}
		case *ecdsa.PublicKey:
			if header.Alg != "ES256" || len(sig) != 64 ||
				!ecdsa.Verify(k, digest[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
				return "", time.Time{}, false
			}
		case ed25519.PublicKey:
			if header.Alg != "EdDSA" || !ed25519.Verify(k, signed, sig) {
				return "", time.Time{}, false
			}
		default:
			return "", time.Time{}, false
		}
	}
	var c claims
	d := json.NewDecoder(strings.NewReader(string(rawClaims)))
	d.UseNumber()
	if d.Decode(&c) != nil || c.Iss != v.cfg.Issuer {
		return "", time.Time{}, false
	}
	skew := v.cfg.ClockSkew
	exp, ok := unix(c.Exp)
	if !ok || !now.Before(exp.Add(skew)) {
		return "", time.Time{}, false
	}
	if nbf, ok := unix(c.Nbf); c.Nbf != nil && (!ok || now.Add(skew).Before(nbf)) {
		return "", time.Time{}, false
	}
	if iat, ok := unix(c.Iat); c.Iat != nil && (!ok || now.Add(skew).Before(iat)) {
		return "", time.Time{}, false
	}
	var audiences []string
	var single string
	if json.Unmarshal(c.Aud, &single) == nil {
		audiences = []string{single}
	} else if json.Unmarshal(c.Aud, &audiences) != nil {
		return "", time.Time{}, false
	}
	if !slices.ContainsFunc(audiences, func(a string) bool { return slices.Contains(v.cfg.Audiences, a) }) {
		return "", time.Time{}, false
	}
	principal, ok := v.mapIdentity(rawClaims)
	return principal, exp, ok
}

// mapIdentity resolves exactly one principal from the identity claim and the
// groups claim. Zero or two distinct principals both reject.
func (v *Verifier) mapIdentity(rawClaims []byte) (string, bool) {
	var all map[string]json.RawMessage
	if json.Unmarshal(rawClaims, &all) != nil {
		return "", false
	}
	found := map[string]bool{}
	var identity string
	if raw, ok := all[v.cfg.Claim]; ok && json.Unmarshal(raw, &identity) == nil && identity != "" {
		if p, ok := v.cfg.Principals[identity]; ok {
			found[p] = true
		}
	}
	if raw, ok := all[v.cfg.GroupsClaim]; ok && len(v.cfg.Groups) != 0 {
		var groups []string
		if json.Unmarshal(raw, &groups) != nil || len(groups) > 256 {
			return "", false
		}
		for _, g := range groups {
			if p, ok := v.cfg.Groups[g]; ok {
				found[p] = true
			}
		}
	}
	if len(found) != 1 {
		return "", false
	}
	for p := range found {
		return p, true
	}
	return "", false
}
