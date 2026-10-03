//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"os"
)

func backupLocal(context.Context, *Store, BackupRequest, backupCopyFunc, func(*os.File) error) (Snapshot, error) {
	return Snapshot{}, ErrBackupUnsupported
}
