// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package sourceproof defines a short-lived, application-signed source selection.
// Proofs belong to the trusted parent. They do not replace original grant
// verification, current source authorization or a live execution lease.
package sourceproof

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/delegation"
)

const (
	Version          = 1
	MaxGrantBytes    = 32768
	MaxTokenBytes    = 8192
	MaxEnvelopeBytes = 48 << 10
	MaxLifetime      = 15 * time.Second
	contextLabel     = "kelvo.private-source-proof.v1."
)

var ErrInvalid = errors.New("private source proof does not match the required scope")

// Scope is exact, immutable authority. Callers obtain Expected scope from a
// verified grant, current source metadata and the authenticated TLS peer.
type Scope struct {
	Issuer           string `json:"issuer"`
	Audience         string `json:"audience"`
	ClusterTenant    string `json:"cluster_tenant"`
	ServicePrincipal string `json:"service_principal"`
	Tenant           string `json:"tenant"`
	Source           string `json:"source"`
	SourceRevision   string `json:"source_revision"`
	Authority        string `json:"authority"`
	Kind             string `json:"kind"`
	ExecutionID      string `json:"execution_id"`
	GrantSHA256      string `json:"grant_sha256"`
	Worker           string `json:"worker_id"`
	Owner            string `json:"owner"`
	Claim            string `json:"claim"`
	WorkerIdentity   string `json:"worker_identity"`
	WorkerCertSHA256 string `json:"worker_cert_sha256"`
}

type Claims struct {
	Version   int   `json:"version"`
	Scope     Scope `json:"scope"`
	IssuedAt  int64 `json:"issued_at"`
	ExpiresAt int64 `json:"expires_at"`
}

// Envelope carries the original signed authority for the issuer's existing
// verifier and lease API. Never copy it into a child source, environment or file.
type Envelope struct {
	Grant string `json:"grant"`
	Token string `json:"token"`
}

func GrantDigest(grant string) string {
	digest := sha256.Sum256([]byte(grant))
	return hex.EncodeToString(digest[:])
}

// Sign requires prior grant, source and live-lease verification by the caller.
// expires must not exceed the grant, lease, source or peer certificate deadline.
func Sign(key ed25519.PrivateKey, scope Scope, grant string, now, expires time.Time) (Envelope, error) {
	claims := Claims{Version: Version, Scope: scope, IssuedAt: now.Unix(), ExpiresAt: expires.Unix()}
	if len(key) != ed25519.PrivateKeySize || validGrant(grant) != nil || scope.GrantSHA256 != GrantDigest(grant) || validate(claims, now) != nil {
		return Envelope{}, ErrInvalid
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return Envelope{}, ErrInvalid
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	message := contextLabel + encoded
	token := message + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(message)))
	if len(token) > MaxTokenBytes {
		return Envelope{}, ErrInvalid
	}
	return Envelope{Grant: grant, Token: token}, nil
}

// Verify pins both the signing key and every scope field. It does not verify
// the original grant or refresh a lease. The issuer must perform those checks.
func Verify(key ed25519.PublicKey, envelope Envelope, expected Scope, now time.Time) (Claims, error) {
	bad := func() (Claims, error) { return Claims{}, ErrInvalid }
	if len(key) != ed25519.PublicKeySize || validGrant(envelope.Grant) != nil || expected.GrantSHA256 != GrantDigest(envelope.Grant) || len(envelope.Token) > MaxTokenBytes || !strings.HasPrefix(envelope.Token, contextLabel) {
		return bad()
	}
	parts := strings.Split(strings.TrimPrefix(envelope.Token, contextLabel), ".")
	if len(parts) != 2 {
		return bad()
	}
	payload, err := base64.RawURLEncoding.Strict().DecodeString(parts[0])
	if err != nil {
		return bad()
	}
	signature, err := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(key, []byte(contextLabel+parts[0]), signature) {
		return bad()
	}
	var claims Claims
	if delegation.StrictJSONLimit(payload, &claims, MaxTokenBytes) != nil || validate(claims, now) != nil || claims.Scope != expected {
		return bad()
	}
	return claims, nil
}

func validGrant(grant string) error {
	if len(grant) == 0 || len(grant) > MaxGrantBytes {
		return ErrInvalid
	}
	for _, ch := range grant {
		if ch <= 32 || ch >= 127 {
			return ErrInvalid
		}
	}
	return nil
}

func validate(c Claims, now time.Time) error {
	s := c.Scope
	if c.Version != Version || c.IssuedAt > now.Unix() || c.ExpiresAt <= now.Unix() || c.ExpiresAt <= c.IssuedAt || c.ExpiresAt-c.IssuedAt > int64(MaxLifetime/time.Second) {
		return ErrInvalid
	}
	for _, value := range []string{s.Issuer, s.Audience, s.ClusterTenant, s.ServicePrincipal, s.Tenant, s.Source, s.ExecutionID} {
		if !text(value, 128) {
			return ErrInvalid
		}
	}
	if !text(s.Worker, 32) || !digest(s.SourceRevision, 64) || !digest(s.GrantSHA256, 64) || !digest(s.WorkerCertSHA256, 64) || !digest(s.Owner, 32) || !digest(s.Claim, 32) || (s.Kind != "query" && s.Kind != "operation") {
		return ErrInvalid
	}
	identity, err := url.Parse(s.WorkerIdentity)
	if err != nil || !text(s.WorkerIdentity, 512) || identity.Scheme != "spiffe" || identity.Host == "" || identity.User != nil || identity.RawQuery != "" || identity.ForceQuery || identity.Fragment != "" || identity.Opaque != "" || identity.RawPath != "" {
		return ErrInvalid
	}
	if !authority(s.Authority) {
		return ErrInvalid
	}
	return nil
}

func text(value string, max int) bool {
	if len(value) == 0 || len(value) > max {
		return false
	}
	for _, ch := range value {
		if ch <= 32 || ch >= 127 {
			return false
		}
	}
	return true
}

func digest(value string, size int) bool {
	if len(value) != size {
		return false
	}
	for _, ch := range value {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}

func authority(value string) bool {
	host, port, err := net.SplitHostPort(value)
	number, perr := strconv.Atoi(port)
	if err != nil || perr != nil || number < 1 || number > 65535 || strconv.Itoa(number) != port || net.JoinHostPort(host, port) != value {
		return false
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Zone() == "" && ip.String() == host && !ip.IsUnspecified() && !ip.IsMulticast()
	}
	if len(host) == 0 || len(host) > 253 || host != strings.ToLower(host) {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, ch := range label {
			if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-') {
				return false
			}
		}
	}
	return true
}
