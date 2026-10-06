// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

// LookupRequest recovers an existing retained operation after its initial
// response was lost. A missing result never authorizes a new submission.
type LookupRequest struct {
	Version        int    `json:"version"`
	IdempotencyKey string `json:"idempotency_key"`
	RequestSHA256  string `json:"request_sha256"`
}

func (r LookupRequest) Validate() error {
	if r.Version != Version || r.IdempotencyKey == "" || !optionalText(r.IdempotencyKey, 128) || !ValidDigest(r.RequestSHA256) {
		return ErrInvalid
	}
	return nil
}
