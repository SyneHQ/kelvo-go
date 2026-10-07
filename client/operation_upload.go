// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"strings"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// UploadOperationInput stores exact sealed bytes; it does not execute them.
// Keep its returned reference unchanged once an operation has been submitted.
// The caller must not modify raw until this call returns; the input limit is 1 MiB.
func (c *Client) UploadOperationInput(ctx context.Context, auth Authority, raw []byte) (operations.InputRef, error) {
	expected, ok := operationUploadBinding(auth.InputGrant, raw)
	if !ok {
		return operations.InputRef{}, failure("INVALID_ARGUMENT")
	}
	auth, err := c.authority(auth, "input")
	if err != nil {
		return operations.InputRef{}, err
	}
	ctx, release, err := c.begin(ctx, false)
	if err != nil {
		return operations.InputRef{}, err
	}
	defer release()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+"/v1/operation-inputs", bytes.NewReader(raw))
	if err != nil {
		return operations.InputRef{}, failure("INVALID_ARGUMENT")
	}
	req.GetBody = nil
	if auth.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+auth.BearerToken)
	}
	req.Header.Set("X-Kelvo-Operation-Input-Grant", auth.InputGrant)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	response, err := c.send(req)
	if err != nil {
		if ctx.Err() != nil {
			return operations.InputRef{}, contextFailure(ctx)
		}
		return operations.InputRef{}, failure("UNAVAILABLE")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return operations.InputRef{}, statusFailure(response.StatusCode)
	}
	if !operationContentType(response, "application/json") || response.ContentLength > 4096 {
		return operations.InputRef{}, failure("PROTOCOL_ERROR")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	var ref operations.InputRef
	if ctx.Err() != nil {
		return operations.InputRef{}, contextFailure(ctx)
	}
	if err != nil || len(body) > 4096 || operations.DecodeStrict(body, &ref, 4096) != nil || ref.Validate() != nil || ref.Format != expected.Format || ref.SHA256 != expected.SHA256 || ref.Bytes != expected.Bytes {
		return operations.InputRef{}, failure("PROTOCOL_ERROR")
	}
	return ref, nil
}

// Claims only bind the outgoing bytes and expected response. The server verifies
// the signature and live authority; these fields never choose a transport target.
func operationUploadBinding(grant string, raw []byte) (operations.InputUploadClaims, bool) {
	var claims operations.InputUploadClaims
	if len(raw) < 1 || len(raw) > operations.MaxSealedInputBytes || !validOperationGrant(grant) {
		return claims, false
	}
	parts := strings.Split(grant, ".")
	header, _ := base64.RawURLEncoding.Strict().DecodeString(parts[0])
	var envelope struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
		KeyID     string `json:"kid"`
	}
	payload, _ := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	if operations.DecodeStrict(header, &envelope, 1024) != nil || envelope.Algorithm != "EdDSA" || envelope.Type != "kelvo-operation-input+jwt" || operations.DecodeStrict(payload, &claims, operations.MaxGrantBytes) != nil || claims.Version != operations.InputUploadVersion || envelope.KeyID == "" || envelope.KeyID != claims.Issuer {
		return claims, false
	}
	switch claims.Format {
	case "ingestion_batch_v1", "watch_checkpoint_v1", "migration_plan_v1":
	default:
		return claims, false
	}
	hash := sha256.Sum256(raw)
	return claims, claims.SHA256 == hex.EncodeToString(hash[:]) && claims.Bytes == int64(len(raw))
}
