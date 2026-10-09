// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package transportissuer

import (
	"crypto/ed25519"
	"net/url"
	"time"
)

const postgresAbortType = "rabbit-postgres-abort+jwt"

type PostgresAbortClaims struct {
	Version               int    `json:"version"`
	Issuer                string `json:"iss"`
	Audience              string `json:"aud"`
	ID                    string `json:"jti"`
	IssuedAt              int64  `json:"iat"`
	ExpiresAt             int64  `json:"exp"`
	DataTicketSHA256      string `json:"data_ticket_sha256"`
	AcceptanceID          string `json:"acceptance_id"`
	WorkerIdentity        string `json:"worker_identity"`
	WorkerCertSHA256      string `json:"worker_cert_sha256"`
	CancellationStartedAt int64  `json:"cancellation_started_at"`
	Protocol              string `json:"protocol"`
}
type AbortTrust struct {
	Issuer, Audience string
	PublicKey        ed25519.PublicKey
}
type AbortBinding struct{ DataTicketSHA256, AcceptanceID, WorkerIdentity, WorkerCertSHA256 string }

func (c PostgresAbortClaims) ValidateAt(now time.Time) error {
	u, err := url.Parse(c.WorkerIdentity)
	if c.Version != CleanupVersion || !cleanupName(c.Issuer) || !cleanupName(c.Audience) || !cleanupHex(c.ID, 64) || !cleanupHex(c.DataTicketSHA256, 64) || !cleanupHex(c.AcceptanceID, 32) || !cleanupHex(c.WorkerCertSHA256, 64) || c.Protocol != PostgresCancel || len(c.WorkerIdentity) > 512 || err != nil || u.Scheme != "spiffe" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || c.IssuedAt <= 0 || c.IssuedAt > now.Unix() || c.CancellationStartedAt > c.IssuedAt || (CleanupLeaseResponse{ValidUntil: c.ExpiresAt, CancellationStartedAt: c.CancellationStartedAt}).ValidateAt(now) != nil {
		return ErrCleanup
	}
	return nil
}
func SignPostgresAbort(c PostgresAbortClaims, key ed25519.PrivateKey) (string, error) {
	if c.ValidateAt(time.Unix(c.IssuedAt, 0)) != nil {
		return "", ErrCleanup
	}
	return signCleanupClaims(c, postgresAbortType, c.Issuer, key)
}

// Verify performs static checks only. The caller must consume ID once, retain
// the same accepted physical parent, enforce the original source/route/attempt
// scope and check current hard revocations and the fixed cleanup lease. Never
// send a raw database descriptor to a child because this function succeeded.
func VerifyPostgresAbort(token string, trust AbortTrust, binding AbortBinding, now time.Time) (PostgresAbortClaims, error) {
	var c PostgresAbortClaims
	if verifyCleanupClaims(token, postgresAbortType, trust.Issuer, trust.PublicKey, &c) != nil || c.ValidateAt(now) != nil || c.Issuer != trust.Issuer || c.Audience != trust.Audience || (AbortBinding{c.DataTicketSHA256, c.AcceptanceID, c.WorkerIdentity, c.WorkerCertSHA256}) != binding {
		return PostgresAbortClaims{}, ErrCleanup
	}
	return c, nil
}
