// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package delegation retains trusted execution context and public contract compatibility.
package delegation

import (
	"crypto/ed25519"
	"time"

	publicdelegation "github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/query"
)

const MaxTokenBytes = publicdelegation.MaxTokenBytes
const Header = publicdelegation.Header

var ErrInvalid = publicdelegation.ErrInvalid

type Subject = publicdelegation.Subject
type Table = publicdelegation.Table
type Source = publicdelegation.Source
type Claims = publicdelegation.Claims
type Trust = publicdelegation.Trust
type ExecutionBinding = publicdelegation.ExecutionBinding

func QueryDigest(r query.Request) (string, error) { return publicdelegation.QueryDigest(r) }

func Digest(token string) string { return publicdelegation.Digest(token) }

func Sign(c Claims, key ed25519.PrivateKey) (string, error) {
	return publicdelegation.Sign(c, key)
}

func VerifyClaims(token string, trust Trust, now time.Time) (Claims, error) {
	return publicdelegation.VerifyClaims(token, trust, now)
}

func Verify(token string, trust Trust, r query.Request, now time.Time) (Claims, error) {
	return publicdelegation.Verify(token, trust, r, now)
}

func StrictJSON(raw []byte, dst any) error { return publicdelegation.StrictJSON(raw, dst) }

func StrictJSONLimit(raw []byte, dst any, limit int) error {
	return publicdelegation.StrictJSONLimit(raw, dst, limit)
}
