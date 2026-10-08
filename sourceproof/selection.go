// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sourceproof

import (
	"crypto/ed25519"
	"time"
)

// VerifiedSelection carries a complete source scope after initial admission.
// Scope returns a value copy. Reopens must use Verify with that exact scope.
type VerifiedSelection struct{ scope Scope }

func (s VerifiedSelection) Scope() Scope { return s.scope }

// VerifySelection learns only TokenID and BindingVersion from the signed proof.
// The trusted parent supplies every other expected field independently. These
// include the configured route and the original source authority. expected must
// leave only TokenID and BindingVersion empty. No partial scope is returned.
// The application signer must authorize current metadata before issuing proof.
func VerifySelection(key ed25519.PublicKey, envelope Envelope, expected Scope, now time.Time) (VerifiedSelection, error) {
	if expected.TokenID != "" || expected.BindingVersion != 0 {
		return VerifiedSelection{}, ErrInvalid
	}
	claims, err := verifyEnvelope(key, envelope, now)
	if err != nil {
		return VerifiedSelection{}, ErrInvalid
	}
	fixed := claims.Scope
	fixed.TokenID = ""
	fixed.BindingVersion = 0
	if fixed != expected {
		return VerifiedSelection{}, ErrInvalid
	}
	return VerifiedSelection{scope: claims.Scope}, nil
}
