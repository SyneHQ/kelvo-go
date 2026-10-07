// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package delegation signs and verifies query-scoped saved-connection authority.
// Grants bind a query to saved connection identifiers and never carry credentials.
package delegation

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/query"
)

// MaxTokenBytes bounds a serialized delegation token and its strict JSON envelope.
const MaxTokenBytes = 32768

// Header is the HTTP header used to transport query delegation tokens.
const Header = "X-Kelvo-Delegation"

// ErrInvalid covers malformed, expired, untrusted, or mismatched authority.
var ErrInvalid = errors.New("delegated query authority is invalid or expired")
var aliasPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// Subject identifies the user, API key, administrator, or job behind a grant.
type Subject struct {
	Kind           string            `json:"kind"`
	ID             string            `json:"id"`
	JobID          string            `json:"job_id,omitempty"`
	JobExpiresAt   int64             `json:"job_expires_at,omitempty"`
	JobConnections map[string]string `json:"job_connections,omitempty"`
}

// Table maps a federated query table name to an authorized source table.
type Table struct {
	Name     string `json:"name"`
	Table    string `json:"table"`
	Database string `json:"database,omitempty"`
	Schema   string `json:"schema,omitempty"`
}

// Source binds a query alias to a saved connection and optional table scope.
type Source struct {
	Alias        string  `json:"alias"`
	ConnectionID string  `json:"connection_id"`
	Database     string  `json:"database,omitempty"`
	Schema       string  `json:"schema,omitempty"`
	Tables       []Table `json:"tables,omitempty"`
}

// Claims is the version 1 query delegation payload. A grant lasts at most five
// minutes and binds QuerySHA256 to the exact request produced by QueryDigest.
type Claims struct {
	Version          int      `json:"version"`
	Issuer           string   `json:"iss"`
	Audience         string   `json:"aud"`
	ClusterTenant    string   `json:"cluster_tenant"`
	ServicePrincipal string   `json:"service_principal"`
	AppTeam          string   `json:"app_team"`
	Subject          Subject  `json:"subject"`
	ID               string   `json:"jti"`
	IssuedAt         int64    `json:"iat"`
	ExpiresAt        int64    `json:"exp"`
	QuerySHA256      string   `json:"query_sha256"`
	Sources          []Source `json:"sources"`
}

// Trust pins the issuer, audience, cluster tenant, service principal, and signing key.
type Trust struct {
	Issuer, Audience, ClusterTenant, ServicePrincipal string
	PublicKey                                         ed25519.PublicKey
}

// ExecutionBinding identifies the running job associated with a resolver request.
// It is a wire envelope only; constructing it does not establish execution authority.
type ExecutionBinding struct {
	JobID    string `json:"job_id"`
	WorkerID string `json:"worker_id"`
	Owner    string `json:"owner"`
	Claim    string `json:"claim"`
}

type joseHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
	KeyID     string `json:"kid"`
}

// QueryDigest computes the version 1 canonical query digest. It preserves SQL
// bytes, source order, and parameter values as JSON numbers or strings. The
// delegation token is excluded; MongoDB and scan diagnostics are not supported.
func QueryDigest(r query.Request) (string, error) {
	if r.Mongo != nil || r.ScanDiagnostics || query.ValidateRequest(r) != nil {
		return "", ErrInvalid
	}
	parameters := make([]map[string]any, 0, len(r.Parameters))
	for _, p := range r.Parameters {
		value := p.Value
		if len(value) == 0 && p.Type == "null" {
			value = json.RawMessage("null")
		}
		parameters = append(parameters, map[string]any{"type": p.Type, "value": value})
	}
	sources := append([]string{}, r.Sources...)
	b, err := json.Marshal(map[string]any{"mode": r.Mode, "sql": r.SQL, "parameters": parameters, "sources": sources, "connection_id": r.ConnectionID})
	if err != nil {
		return "", ErrInvalid
	}
	return Digest(string(b)), nil
}

// Digest returns the lowercase hexadecimal SHA-256 digest of the supplied bytes.
func Digest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// Sign validates the claims at their issue time and signs a compact Ed25519 JWT.
// The issuer supplies all claims, including the nonce, timestamps, and query digest.
func Sign(c Claims, key ed25519.PrivateKey) (string, error) {
	if len(key) != ed25519.PrivateKeySize || validateClaims(c, time.Unix(c.IssuedAt, 0)) != nil {
		return "", ErrInvalid
	}
	h, _ := json.Marshal(joseHeader{"EdDSA", "JWT", c.Issuer})
	p, err := json.Marshal(c)
	if err != nil {
		return "", ErrInvalid
	}
	unsigned := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	token := unsigned + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(unsigned)))
	if len(token) > MaxTokenBytes {
		return "", ErrInvalid
	}
	return token, nil
}

// VerifyClaims authenticates the envelope. Execution must also call Verify to bind the query.
func VerifyClaims(token string, trust Trust, now time.Time) (Claims, error) {
	if len(token) == 0 || len(token) > MaxTokenBytes || len(trust.PublicKey) != ed25519.PublicKeySize {
		return Claims{}, ErrInvalid
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, ErrInvalid
	}
	decode := func(s string) ([]byte, error) {
		b, e := base64.RawURLEncoding.Strict().DecodeString(s)
		if e != nil || base64.RawURLEncoding.EncodeToString(b) != s {
			return nil, ErrInvalid
		}
		return b, nil
	}
	hb, err := decode(parts[0])
	if err != nil || len(hb) > 1024 {
		return Claims{}, ErrInvalid
	}
	var h joseHeader
	if StrictJSON(hb, &h) != nil || h.Algorithm != "EdDSA" || h.Type != "JWT" || h.KeyID != trust.Issuer {
		return Claims{}, ErrInvalid
	}
	var headerFields map[string]json.RawMessage
	if json.Unmarshal(hb, &headerFields) != nil || len(headerFields) != 3 || headerFields["alg"] == nil || headerFields["typ"] == nil || headerFields["kid"] == nil {
		return Claims{}, ErrInvalid
	}
	sig, err := decode(parts[2])
	if err != nil || !ed25519.Verify(trust.PublicKey, []byte(parts[0]+"."+parts[1]), sig) {
		return Claims{}, ErrInvalid
	}
	payload, err := decode(parts[1])
	if err != nil {
		return Claims{}, ErrInvalid
	}
	var c Claims
	if StrictJSON(payload, &c) != nil || validateClaims(c, now) != nil || c.Issuer != trust.Issuer || c.Audience != trust.Audience || c.ClusterTenant != trust.ClusterTenant || c.ServicePrincipal != trust.ServicePrincipal {
		return Claims{}, ErrInvalid
	}
	return c, nil
}

// Verify authenticates the grant and binds it to the exact query and source scope.
func Verify(token string, trust Trust, r query.Request, now time.Time) (Claims, error) {
	c, err := VerifyClaims(token, trust, now)
	if err != nil {
		return Claims{}, ErrInvalid
	}
	digest, err := QueryDigest(r)
	if err != nil || digest != c.QuerySHA256 {
		return Claims{}, ErrInvalid
	}
	ids := r.Sources
	if r.Mode == "native" {
		ids = []string{r.ConnectionID}
	}
	if len(ids) != len(c.Sources) {
		return Claims{}, ErrInvalid
	}
	for i, s := range c.Sources {
		if ids[i] != s.Alias || (r.Mode == "native" && len(s.Tables) != 0) {
			return Claims{}, ErrInvalid
		}
	}
	return c, nil
}
func text(s string, max int) bool {
	return s != "" && len(s) <= max && utf8.ValidString(s) && !strings.ContainsAny(s, "\x00\r\n")
}
func optionalText(s string, max int) bool { return s == "" || text(s, max) }
func hexText(s string, n int) bool {
	if len(s) != n || strings.ToLower(s) != s {
		return false
	}
	_, e := hex.DecodeString(s)
	return e == nil
}
func validateClaims(c Claims, now time.Time) error {
	if c.Version != 1 || !text(c.Issuer, 128) || !text(c.Audience, 128) || !text(c.ClusterTenant, 32) || !text(c.ServicePrincipal, 32) || !text(c.AppTeam, 256) || !text(c.Subject.ID, 256) || !hexText(c.ID, 48) || !hexText(c.QuerySHA256, 64) || c.IssuedAt <= 0 || c.ExpiresAt <= c.IssuedAt || c.ExpiresAt-c.IssuedAt > 300 || c.IssuedAt > now.Unix()+30 || c.ExpiresAt <= now.Unix() || len(c.Sources) == 0 || len(c.Sources) > 32 {
		return ErrInvalid
	}
	switch c.Subject.Kind {
	case "user", "api_key", "admin":
		if c.Subject.JobID != "" || c.Subject.JobExpiresAt != 0 || len(c.Subject.JobConnections) != 0 {
			return ErrInvalid
		}
	case "job":
		if !text(c.Subject.JobID, 256) || c.Subject.JobExpiresAt < c.ExpiresAt || len(c.Subject.JobConnections) == 0 || len(c.Subject.JobConnections) > 32 {
			return ErrInvalid
		}
		for id, scope := range c.Subject.JobConnections {
			if !text(id, 256) || (scope != "read" && scope != "write") {
				return ErrInvalid
			}
		}
	default:
		return ErrInvalid
	}
	aliases, connections := map[string]bool{}, map[string]bool{}
	tables := 0
	for _, s := range c.Sources {
		if !aliasPattern.MatchString(s.Alias) || aliases[strings.ToLower(s.Alias)] || !text(s.ConnectionID, 256) || connections[s.ConnectionID] || !optionalText(s.Database, 256) || !optionalText(s.Schema, 256) {
			return ErrInvalid
		}
		aliases[strings.ToLower(s.Alias)], connections[s.ConnectionID] = true, true
		if c.Subject.Kind == "job" {
			if _, ok := c.Subject.JobConnections[s.ConnectionID]; !ok {
				return ErrInvalid
			}
		}
		seen := map[string]bool{}
		for _, t := range s.Tables {
			tables++
			if tables > 32 || !aliasPattern.MatchString(t.Name) || !aliasPattern.MatchString(t.Table) || seen[strings.ToLower(t.Name)] || !optionalText(t.Database, 256) || !optionalText(t.Schema, 256) {
				return ErrInvalid
			}
			seen[strings.ToLower(t.Name)] = true
		}
	}
	return nil
}

// StrictJSON rejects case-insensitive duplicate members, unknown fields, trailing
// data, invalid UTF-8, nesting deeper than 16 levels, and input over MaxTokenBytes.
func StrictJSON(raw []byte, dst any) error {
	return StrictJSONLimit(raw, dst, MaxTokenBytes)
}

// StrictJSONLimit applies the same parsing rules to bounded private resolver envelopes.
func StrictJSONLimit(raw []byte, dst any, limit int) error {
	if limit < 1 || limit > 1<<20 || len(raw) > limit || !utf8.Valid(raw) {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 16 {
			return ErrInvalid
		}
		v, e := d.Token()
		if e != nil {
			return ErrInvalid
		}
		if delim, ok := v.(json.Delim); ok {
			switch delim {
			case '{':
				seen := map[string]bool{}
				for d.More() {
					k, e := d.Token()
					key, ok := k.(string)
					if e != nil || !ok || seen[strings.ToLower(key)] {
						return ErrInvalid
					}
					seen[strings.ToLower(key)] = true
					if walk(depth+1) != nil {
						return ErrInvalid
					}
				}
				end, e := d.Token()
				if e != nil || end != json.Delim('}') {
					return ErrInvalid
				}
			case '[':
				for d.More() {
					if walk(depth+1) != nil {
						return ErrInvalid
					}
				}
				end, e := d.Token()
				if e != nil || end != json.Delim(']') {
					return ErrInvalid
				}
			default:
				return ErrInvalid
			}
		}
		return nil
	}
	if walk(0) != nil {
		return ErrInvalid
	}
	if _, e := d.Token(); e != io.EOF {
		return ErrInvalid
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(dst) != nil || d.Decode(new(any)) != io.EOF {
		return ErrInvalid
	}
	return nil
}
