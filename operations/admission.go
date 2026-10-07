// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

const MaxAdmissionRejectionBytes = 1024

// AdmissionRejection is a bounded response to a verified submission whose
// admission did not commit. It is not an operation receipt or evidence about
// any previous attempt using the same idempotency key. Clients must not replay
// an uncertain earlier attempt on the strength of this response.
//
// Gateways emit this contract only for HTTP 429 on POST /v1/operations. Other
// errors, especially lost storage acknowledgements, cannot use this envelope.
type AdmissionRejection struct {
	Version       int    `json:"version"`
	Admission     string `json:"admission"`
	Code          string `json:"code"`
	RequestSHA256 string `json:"request_sha256"`
	GrantSHA256   string `json:"grant_sha256"`
}

// ValidateBinding requires the complete request and exact submitted grant.
// The caller must separately authenticate the responding gateway transport.
func (r AdmissionRejection) ValidateBinding(requestDigest, grantDigest string) error {
	if r.Version != 1 || r.Admission != "not_admitted" || r.Code != "RESOURCE_EXHAUSTED" ||
		!ValidDigest(requestDigest) || !ValidDigest(grantDigest) ||
		r.RequestSHA256 != requestDigest || r.GrantSHA256 != grantDigest {
		return ErrInvalid
	}
	return nil
}
