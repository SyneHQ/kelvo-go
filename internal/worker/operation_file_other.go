//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"os"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func prepareOperationSourceFile(context.Context, *scratchWorkspace, adapter.ProcessRequest, int64) (*os.File, operationBinaryIdentity, int64, error) {
	return nil, operationBinaryIdentity{}, 0, connectionUnavailable()
}
