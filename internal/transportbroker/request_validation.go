// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package transportbroker

import "time"

// ValidateAt checks an open's immutable scope before a parent-side provider uses
// issuer credentials. It does not establish current source or execution custody.
func (r OpenRequest) ValidateAt(now time.Time) error {
	if !validBinding(r.Binding, now) || !hexValue(r.ID, 64) || (r.Purpose != Data && r.Purpose != Cancellation) {
		return ErrInvalid
	}
	return nil
}

// ValidateAuthority checks canonical host:port syntax without performing DNS.
func ValidateAuthority(authority string) error {
	if !validAuthority(authority) {
		return ErrInvalid
	}
	return nil
}
