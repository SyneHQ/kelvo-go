// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package transportissuer

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/SYNEHQ/kelvo-go/delegation"
)

type cleanupHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
	KeyID     string `json:"kid"`
}

func signCleanupClaims(value any, kind, keyID string, key ed25519.PrivateKey) (string, error) {
	if len(key) != ed25519.PrivateKeySize || !cleanupName(keyID) {
		return "", ErrCleanup
	}
	header, err := json.Marshal(cleanupHeader{"EdDSA", kind, keyID})
	if err != nil {
		return "", ErrCleanup
	}
	body, err := json.Marshal(value)
	if err != nil {
		return "", ErrCleanup
	}
	raw := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body)
	token := raw + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(raw)))
	if len(token) > MaxAbortTokenBytes {
		return "", ErrCleanup
	}
	return token, nil
}
func verifyCleanupClaims(token, kind, keyID string, key ed25519.PublicKey, dst any) error {
	if len(token) == 0 || len(token) > MaxAbortTokenBytes || len(key) != ed25519.PublicKeySize || !cleanupName(keyID) {
		return ErrCleanup
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ErrCleanup
	}
	decoded := make([][]byte, 3)
	for i, part := range parts {
		raw, err := base64.RawURLEncoding.Strict().DecodeString(part)
		if err != nil || len(raw) == 0 || base64.RawURLEncoding.EncodeToString(raw) != part {
			return ErrCleanup
		}
		decoded[i] = raw
	}
	var header cleanupHeader
	if delegation.StrictJSONLimit(decoded[0], &header, 1024) != nil || header != (cleanupHeader{"EdDSA", kind, keyID}) || !ed25519.Verify(key, []byte(parts[0]+"."+parts[1]), decoded[2]) || delegation.StrictJSONLimit(decoded[1], dst, MaxAbortTokenBytes) != nil {
		return ErrCleanup
	}
	return nil
}
