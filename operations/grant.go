// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

const GrantVersion = 2
const MaxGrantBytes = 32768

type Subject struct {
	Kind           string            `json:"kind"`
	ID             string            `json:"id"`
	JobID          string            `json:"job_id,omitempty"`
	JobExpiresAt   int64             `json:"job_expires_at,omitempty"`
	JobConnections map[string]string `json:"job_connections,omitempty"`
}

// Authorization describes a verified upstream authority, not a user-supplied
// promise. Direct API-key/job write grants do not pretend to be approval tickets.
// The private resolver must recheck the current upstream authority before use.
type Authorization struct {
	Kind           string              `json:"kind"` // read, api_key, job, trusted_app, approved_change, ingestion, watcher
	ApprovalID     string              `json:"approval_id,omitempty"`
	ApprovedSHA256 string              `json:"approved_sha256,omitempty"`
	Ingestion      *IngestionAuthority `json:"ingestion,omitempty"`
	Watcher        *WatcherAuthority   `json:"watcher,omitempty"`
}

// IngestionAuthority is the verified upstream job identity. It carries no
// reusable job bearer token. The private resolver must recheck its live lease,
// source revision, installation and exact request scope before execution.
type IngestionAuthority struct {
	RunID          string `json:"run_id"`
	LeaseID        string `json:"lease_id"`
	SourceRevision int    `json:"source_revision"`
	AllowInstall   bool   `json:"allow_install"`
	ExpiresAt      int64  `json:"expires_at"`
}

func (k Kind) Ingestion() bool {
	return k == IngestionInstall || k == IngestionState || k == IngestionCommit
}

// WatcherAuthority binds internal polling and source lifecycle work to the
// current persisted watcher configuration. The resolver must verify it live.
type WatcherAuthority struct {
	ID                  string `json:"id"`
	Generation          string `json:"generation"`
	ConfigurationSHA256 string `json:"configuration_sha256"`
	Cleanup             bool   `json:"cleanup,omitempty"`
}

func (k Kind) Watcher() bool {
	return k == WatchInstall || k == WatchRead || k == WatchAck || k == WatchRemove
}

type GrantClaims struct {
	Version          int           `json:"version"`
	Issuer           string        `json:"iss"`
	Audience         string        `json:"aud"`
	ClusterTenant    string        `json:"cluster_tenant"`
	ServicePrincipal string        `json:"service_principal"`
	AppTeam          string        `json:"app_team"`
	Subject          Subject       `json:"subject"`
	ID               string        `json:"jti"`
	IssuedAt         int64         `json:"iat"`
	ExpiresAt        int64         `json:"exp"`
	ConnectionID     string        `json:"connection_id"`
	Operation        Kind          `json:"operation"`
	RequestSHA256    string        `json:"request_sha256"`
	Authorization    Authorization `json:"authorization"`
}

type GrantTrust struct {
	Issuer, Audience, ClusterTenant, ServicePrincipal string
	PublicKey                                         ed25519.PublicKey
}
type grantHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
	KeyID     string `json:"kid"`
}

func validateGrant(c GrantClaims, now time.Time) error {
	if c.Version != GrantVersion || !text(c.Issuer, 128) || !text(c.Audience, 128) || !text(c.ClusterTenant, 128) || !text(c.ServicePrincipal, 128) || !text(c.AppTeam, 256) || !text(c.Subject.ID, 256) ||
		!ValidID(c.ID) || !text(c.ConnectionID, 256) || !c.Operation.Valid() || !ValidDigest(c.RequestSHA256) || c.IssuedAt <= 0 || c.ExpiresAt <= c.IssuedAt || c.ExpiresAt-c.IssuedAt > 300 || c.IssuedAt > now.Unix()+30 || c.ExpiresAt <= now.Unix() {
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
		scope, exists := c.Subject.JobConnections[c.ConnectionID]
		if !exists || (c.Operation.Mutating() && scope != "write") {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	a := c.Authorization
	if (a.Kind == "ingestion" && !c.Operation.Ingestion()) || (a.Kind != "ingestion" && a.Ingestion != nil) {
		return ErrInvalid
	}
	if (a.Kind == "watcher" && !c.Operation.Watcher() && c.Operation != QueryRead) || (a.Kind != "watcher" && a.Watcher != nil) {
		return ErrInvalid
	}
	if a.Kind != "approved_change" && (a.ApprovalID != "" || a.ApprovedSHA256 != "") {
		return ErrInvalid
	}
	switch a.Kind {
	case "watcher":
		v := a.Watcher
		if c.Subject.Kind != "admin" || v == nil || !text(v.ID, 256) || !ValidID(v.Generation) || !ValidDigest(v.ConfigurationSHA256) || v.Cleanup != (c.Operation == WatchRemove) {
			return ErrInvalid
		}
	case "ingestion":
		v := a.Ingestion
		if c.Subject.Kind != "user" || v == nil || !text(v.RunID, 128) || !text(v.LeaseID, 128) || v.SourceRevision < 1 || v.ExpiresAt < c.ExpiresAt || v.ExpiresAt > c.IssuedAt+900 || (c.Operation == IngestionInstall && !v.AllowInstall) {
			return ErrInvalid
		}
	case "read":
		if c.Operation.Mutating() {
			return ErrInvalid
		}
	case "api_key":
		if c.Subject.Kind != "api_key" {
			return ErrInvalid
		}
	case "job":
		if c.Subject.Kind != "job" {
			return ErrInvalid
		}
	case "trusted_app":
		if c.Subject.Kind != "user" && c.Subject.Kind != "admin" {
			return ErrInvalid
		}
	case "approved_change":
		if (c.Subject.Kind != "user" && c.Subject.Kind != "admin") || !c.Operation.Mutating() || !text(a.ApprovalID, 256) || a.ApprovedSHA256 != c.RequestSHA256 {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

func SignGrant(c GrantClaims, key ed25519.PrivateKey) (string, error) {
	if len(key) != ed25519.PrivateKeySize || validateGrant(c, time.Unix(c.IssuedAt, 0)) != nil {
		return "", ErrInvalid
	}
	header, _ := json.Marshal(grantHeader{"EdDSA", "kelvo-operation+jwt", c.Issuer})
	payload, err := json.Marshal(c)
	if err != nil {
		return "", ErrInvalid
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	token := unsigned + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(unsigned)))
	if len(token) > MaxGrantBytes {
		return "", ErrInvalid
	}
	return token, nil
}

// VerifyGrantClaims authenticates only the envelope. Execution must additionally
// call VerifyGrant to bind the exact operation, target and approval reference.
func VerifyGrantClaims(token string, trust GrantTrust, now time.Time) (GrantClaims, error) {
	if len(token) == 0 || len(token) > MaxGrantBytes || len(trust.PublicKey) != ed25519.PublicKeySize {
		return GrantClaims{}, ErrInvalid
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return GrantClaims{}, ErrInvalid
	}
	decode := func(value string) ([]byte, error) {
		raw, err := base64.RawURLEncoding.Strict().DecodeString(value)
		if err != nil || base64.RawURLEncoding.EncodeToString(raw) != value {
			return nil, ErrInvalid
		}
		return raw, nil
	}
	header, err := decode(parts[0])
	if err != nil || len(header) > 1024 {
		return GrantClaims{}, ErrInvalid
	}
	var h grantHeader
	if DecodeStrict(header, &h, 1024) != nil || h.Algorithm != "EdDSA" || h.Type != "kelvo-operation+jwt" || h.KeyID != trust.Issuer {
		return GrantClaims{}, ErrInvalid
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(header, &fields) != nil || len(fields) != 3 || fields["alg"] == nil || fields["typ"] == nil || fields["kid"] == nil {
		return GrantClaims{}, ErrInvalid
	}
	signature, err := decode(parts[2])
	if err != nil || !ed25519.Verify(trust.PublicKey, []byte(parts[0]+"."+parts[1]), signature) {
		return GrantClaims{}, ErrInvalid
	}
	payload, err := decode(parts[1])
	if err != nil {
		return GrantClaims{}, ErrInvalid
	}
	var c GrantClaims
	if DecodeStrict(payload, &c, MaxGrantBytes) != nil || validateGrant(c, now) != nil || c.Issuer != trust.Issuer || c.Audience != trust.Audience || c.ClusterTenant != trust.ClusterTenant || c.ServicePrincipal != trust.ServicePrincipal {
		return GrantClaims{}, ErrInvalid
	}
	return c, nil
}

func VerifyGrant(token string, trust GrantTrust, r Request, now time.Time) (GrantClaims, error) {
	c, err := VerifyGrantClaims(token, trust, now)
	if err != nil {
		return GrantClaims{}, ErrInvalid
	}
	digest, err := Digest(r)
	if err != nil || c.RequestSHA256 != digest || c.ConnectionID != r.Connection.ID || c.Operation != r.Kind {
		return GrantClaims{}, ErrInvalid
	}
	if c.Authorization.Kind == "watcher" && r.Kind.Watcher() && (r.Spec.Watch == nil || r.Spec.Watch.ID != c.Authorization.Watcher.ID || r.Spec.Watch.Generation != c.Authorization.Watcher.Generation) {
		return GrantClaims{}, ErrInvalid
	}
	if c.Authorization.Kind == "approved_change" {
		if r.ApprovalID != c.Authorization.ApprovalID {
			return GrantClaims{}, ErrInvalid
		}
	} else if r.ApprovalID != "" {
		return GrantClaims{}, ErrInvalid
	}
	return c, nil
}

func GrantDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
