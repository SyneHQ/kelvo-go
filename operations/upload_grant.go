// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

const InputUploadVersion = 1

// SealedInputOperation is the only operation permitted to consume a format.
// An empty result means that public uploads of the format are unsupported.
func SealedInputOperation(format string) Kind {
	switch format {
	case "ingestion_batch_v1":
		return IngestionCommit
	case "watch_checkpoint_v1":
		return WatchAck
	case "mongo_watch_resume_v1":
		return WatchInstall
	case "migration_plan_v1":
		return MigrationApply
	default:
		return ""
	}
}

// InputUploadClaims authorize only temporary storage of these exact bytes.
// Execution needs a separate operation grant containing the returned InputRef.
type InputUploadClaims struct {
	Version          int     `json:"version"`
	Issuer           string  `json:"iss"`
	Audience         string  `json:"aud"`
	ClusterTenant    string  `json:"cluster_tenant"`
	ServicePrincipal string  `json:"service_principal"`
	AppTeam          string  `json:"app_team"`
	Subject          Subject `json:"subject"`
	ID               string  `json:"jti"`
	IssuedAt         int64   `json:"iat"`
	ExpiresAt        int64   `json:"exp"`
	ConnectionID     string  `json:"connection_id"`
	Format           string  `json:"format"`
	SHA256           string  `json:"sha256"`
	Bytes            int64   `json:"bytes"`
}

func validateInputUpload(c InputUploadClaims, now time.Time) error {
	if c.Version != InputUploadVersion || SealedInputOperation(c.Format) == "" || c.Bytes < 1 || c.Bytes > MaxSealedInputBytes {
		return ErrInvalid
	}
	// Reuse the envelope's current identity, job-scope and lifetime rules without
	// granting any source operation or accepting operation-approval assertions.
	authorization := "trusted_app"
	if c.Subject.Kind == "api_key" || c.Subject.Kind == "job" {
		authorization = c.Subject.Kind
	}
	return validateGrant(GrantClaims{Version: GrantVersion, Issuer: c.Issuer, Audience: c.Audience,
		ClusterTenant: c.ClusterTenant, ServicePrincipal: c.ServicePrincipal, AppTeam: c.AppTeam, Subject: c.Subject,
		ID: c.ID, IssuedAt: c.IssuedAt, ExpiresAt: c.ExpiresAt, ConnectionID: c.ConnectionID,
		Operation: StatementExecute, RequestSHA256: c.SHA256, Authorization: Authorization{Kind: authorization}}, now)
}

func SignInputUploadGrant(c InputUploadClaims, key ed25519.PrivateKey) (string, error) {
	if len(key) != ed25519.PrivateKeySize || validateInputUpload(c, time.Unix(c.IssuedAt, 0)) != nil {
		return "", ErrInvalid
	}
	header, _ := json.Marshal(grantHeader{"EdDSA", "kelvo-operation-input+jwt", c.Issuer})
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

func VerifyInputUploadGrant(token string, trust GrantTrust, now time.Time) (InputUploadClaims, error) {
	if len(token) == 0 || len(token) > MaxGrantBytes || len(trust.PublicKey) != ed25519.PublicKeySize {
		return InputUploadClaims{}, ErrInvalid
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return InputUploadClaims{}, ErrInvalid
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
		return InputUploadClaims{}, ErrInvalid
	}
	var h grantHeader
	if DecodeStrict(header, &h, 1024) != nil || h.Algorithm != "EdDSA" || h.Type != "kelvo-operation-input+jwt" || h.KeyID != trust.Issuer {
		return InputUploadClaims{}, ErrInvalid
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(header, &fields) != nil || len(fields) != 3 || fields["alg"] == nil || fields["typ"] == nil || fields["kid"] == nil {
		return InputUploadClaims{}, ErrInvalid
	}
	signature, err := decode(parts[2])
	if err != nil || !ed25519.Verify(trust.PublicKey, []byte(parts[0]+"."+parts[1]), signature) {
		return InputUploadClaims{}, ErrInvalid
	}
	payload, err := decode(parts[1])
	if err != nil {
		return InputUploadClaims{}, ErrInvalid
	}
	var c InputUploadClaims
	if DecodeStrict(payload, &c, MaxGrantBytes) != nil || validateInputUpload(c, now) != nil || c.Issuer != trust.Issuer || c.Audience != trust.Audience || c.ClusterTenant != trust.ClusterTenant || c.ServicePrincipal != trust.ServicePrincipal {
		return InputUploadClaims{}, ErrInvalid
	}
	return c, nil
}
