// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Digest binds the complete validated request, including exact SQL, parameter
// lexical values, input digests, target, idempotency key and approval reference.
// This versioned Go JSON encoding is a protocol contract; do not substitute a
// reordered map or another serializer without matching the golden vectors.
func Digest(r Request) (string, error) {
	if r.Validate() != nil {
		return "", ErrInvalid
	}
	b, err := json.Marshal(r)
	if err != nil {
		return "", ErrInvalid
	}
	h := sha256.New()
	_, _ = h.Write([]byte("kelvo.database.operation.v1\x00"))
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func Clone(r Request) (Request, error) {
	if r.Validate() != nil {
		return Request{}, ErrInvalid
	}
	b, err := json.Marshal(r)
	if err != nil {
		return Request{}, ErrInvalid
	}
	return ParseRequest(b)
}

// Encode is the canonical request encoding used by Digest and sealed inputs.
func Encode(r Request) ([]byte, error) {
	if r.Validate() != nil {
		return nil, ErrInvalid
	}
	return json.Marshal(r)
}

// SealRequest describes canonical bytes; the caller must durably seal them in
// tenant-owned storage before admission and verify ownership on every read.
func SealRequest(r Request, id string) (InputRef, []byte, error) {
	if !ValidID(id) {
		return InputRef{}, nil, ErrInvalid
	}
	b, err := Encode(r)
	if err != nil {
		return InputRef{}, nil, err
	}
	sum := sha256.Sum256(b)
	return InputRef{ID: id, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(b)), Format: "operation_request_v1"}, b, nil
}
