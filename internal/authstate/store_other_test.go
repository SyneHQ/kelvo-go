//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authstate

import (
	"context"
	"testing"
)

func TestStoreUnsupportedPlatformNeverReturnsAuthority(t *testing.T) {
	ctx := context.Background()
	if store, err := Open(ctx, "/unused", testScope()); store != nil || err != ErrUnavailable {
		t.Fatal("unsupported platform returned authority", err)
	}
	if err := Initialize(ctx, "/unused", testScope(), testCandidate(1)); err != ErrUnavailable {
		t.Fatal("unsupported platform initialized state", err)
	}
	if err := new(Store).Admit(ctx, testCandidate(1)); err != ErrUnavailable {
		t.Fatal("unsupported platform admitted a candidate", err)
	}
	if err := (*Store)(nil).Close(ctx); err != nil {
		t.Fatal("empty unsupported store failed to close", err)
	}
}
