//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
)

func newPrivatePostgresChannel(context.Context, adapter.ProcessRequest, *transportbroker.Session, transportbroker.Binding, *privatePostgresCleanup, func()) (*privateOperationChannel, error) {
	return nil, adapter.ErrUnsupported
}
