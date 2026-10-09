//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/childipc"
)

func newPrivatePostgresChannel(ctx context.Context, input adapter.ProcessRequest, session *transportbroker.Session, binding transportbroker.Binding, cleanup *privatePostgresCleanup, release func()) (*privateOperationChannel, error) {
	digest, err := privateInputDigest(input)
	if err != nil || session == nil || cleanup == nil || release == nil || !session.BoundTo(binding) || !privateInputScope(input, binding) {
		return nil, adapter.ErrInvalid
	}
	server, file, err := childipc.NewPairWithPostgresCleanup(ctx, binding.Authority, session.DataDialer(), cleanup)
	if err != nil {
		return nil, err
	}
	return &privateOperationChannel{inputSHA256: digest, server: server, file: file, session: session, binding: binding, release: release, postgresCleanup: cleanup}, nil
}
