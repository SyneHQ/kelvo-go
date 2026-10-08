// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/sourceproof"
)

const maxTicketBytes = 8192

// Keep this explicit wire contract separate from Rabbit's non-importable server
// module. Changes require protocol and actual-source compatibility qualification.
type ticketClaims struct {
	Version          int             `json:"version"`
	Issuer           string          `json:"iss"`
	Audience         string          `json:"aud"`
	ID               string          `json:"jti"`
	IssuedAt         int64           `json:"iat"`
	ExpiresAt        int64           `json:"exp"`
	SessionExpiresAt int64           `json:"session_expires_at"`
	ClusterTenant    string          `json:"cluster_tenant"`
	ServicePrincipal string          `json:"service_principal"`
	Tenant           string          `json:"tenant"`
	Source           string          `json:"source"`
	SourceRevision   string          `json:"source_revision"`
	TokenID          string          `json:"token_id"`
	TokenGeneration  string          `json:"token_generation"`
	TunnelID         string          `json:"tunnel_id"`
	ControlOwner     string          `json:"control_owner"`
	Authority        string          `json:"authority"`
	WorkerIdentity   string          `json:"worker_identity"`
	WorkerCertSHA256 string          `json:"worker_cert_sha256"`
	Execution        ticketExecution `json:"execution"`
}

type ticketExecution struct {
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	GrantSHA256 string `json:"grant_sha256"`
	Worker      string `json:"worker_id"`
	Owner       string `json:"owner"`
	Claim       string `json:"claim"`
}

type ticketHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
	KeyID     string `json:"kid"`
}

func (o *Opener) verifyTicket(token string, request IssueRequest, now time.Time) error {
	if len(token) == 0 || len(token) > maxTicketBytes {
		return transportbroker.ErrOpen
	}
	// Go's base64 decoder ignores CR/LF even in strict mode. Compact JWTs may
	// contain neither; reject them before a ticket can reach an HTTP header.
	for _, char := range token {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-' || char == '.') {
			return transportbroker.ErrOpen
		}
	}
	parts := strings.SplitN(token, ".", 4)
	if len(parts) != 3 {
		return transportbroker.ErrOpen
	}
	decode := base64.RawURLEncoding.Strict().DecodeString
	headerBytes, e1 := decode(parts[0])
	payload, e2 := decode(parts[1])
	signature, e3 := decode(parts[2])
	if e1 != nil || e2 != nil || e3 != nil || len(signature) != ed25519.SignatureSize ||
		!ed25519.Verify(o.key, []byte(parts[0]+"."+parts[1]), signature) {
		return transportbroker.ErrOpen
	}
	var header ticketHeader
	var claims ticketClaims
	if strictJSON(headerBytes, &header) != nil || strictJSON(payload, &claims) != nil ||
		header.Algorithm != "EdDSA" || header.Type != "rabbit-connect+jwt" || header.KeyID != o.issuer {
		return transportbroker.ErrOpen
	}
	if o.sourceProof != nil {
		if request.PrivateSource == nil {
			return transportbroker.ErrScope
		}
		proof, err := sourceproof.Verify(o.sourceProof.PublicKey, *request.PrivateSource, o.sourceProof.Scope, now)
		if err != nil || claims.ExpiresAt > proof.ExpiresAt {
			return transportbroker.ErrScope
		}
	}
	if request.PrivateSource != nil && claims.TokenID != request.TokenID {
		return transportbroker.ErrScope
	}
	binding := request.Binding
	execution := binding.Execution
	if claims.Version != 1 || claims.Issuer != o.issuer || claims.Audience != o.audience ||
		claims.ClusterTenant != o.cluster || claims.ServicePrincipal != o.principal ||
		claims.Tenant != binding.Tenant || claims.Source != binding.Source || claims.SourceRevision != binding.SourceRevision ||
		claims.Authority != binding.Authority || claims.ID != request.OpenID ||
		claims.WorkerIdentity != o.identity || claims.WorkerCertSHA256 != o.certDigest ||
		claims.Execution != (ticketExecution{execution.Kind, execution.ID, execution.GrantSHA256, execution.Worker, execution.Owner, execution.Claim}) ||
		!textValue(claims.TokenID, 128) || !hexValue(claims.TokenGeneration, 64) ||
		!hexValue(claims.TunnelID, 64) || !hexValue(claims.ControlOwner, 64) {
		return transportbroker.ErrOpen
	}
	if claims.IssuedAt <= 0 || claims.IssuedAt > now.Unix() || claims.ExpiresAt <= now.Unix() ||
		claims.ExpiresAt <= claims.IssuedAt || claims.ExpiresAt-claims.IssuedAt > 60 ||
		claims.SessionExpiresAt < claims.ExpiresAt || claims.SessionExpiresAt-claims.IssuedAt > 24*60*60 ||
		time.Unix(claims.SessionExpiresAt, 0).After(binding.ExpiresAt) ||
		time.Unix(claims.SessionExpiresAt, 0).After(o.certificateUntil) {
		return transportbroker.ErrOpen
	}
	return nil
}

func strictJSON(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(output) != nil {
		return transportbroker.ErrOpen
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return transportbroker.ErrOpen
	}
	return uniqueJSON(json.NewDecoder(bytes.NewReader(data)), 0)
}

func uniqueJSON(decoder *json.Decoder, depth int) error {
	if depth > 4 {
		return transportbroker.ErrOpen
	}
	token, err := decoder.Token()
	if err != nil {
		return transportbroker.ErrOpen
	}
	if token != json.Delim('{') {
		if _, compound := token.(json.Delim); compound {
			return transportbroker.ErrOpen
		}
		return nil
	}
	seen := make(map[string]bool)
	for decoder.More() {
		key, err := decoder.Token()
		name, ok := key.(string)
		if err != nil || !ok || name != strings.ToLower(name) || seen[name] {
			return transportbroker.ErrOpen
		}
		seen[name] = true
		if uniqueJSON(decoder, depth+1) != nil {
			return transportbroker.ErrOpen
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return transportbroker.ErrOpen
	}
	return nil
}
