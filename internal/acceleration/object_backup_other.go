//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"os"
)

func backupObjectToLocal(context.Context, *objectBackend, MigrationRequest, backupCopyFunc, func(*os.File) error) (MigrationResult, error) {
	return MigrationResult{}, ErrBackupUnsupported
}
