// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// Backup copies one current local generation to a new private acceleration root.
// Policy always comes from the operator's active catalog. This operation neither
// executes a source query nor changes the active store or the snapshot's age.
func (m *Manager) Backup(ctx context.Context, id, destination string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	dataset, ok := m.config.Dataset(id)
	if !ok {
		return Snapshot{}, query.NewError("INVALID_ARGUMENT", "Unknown accelerated dataset")
	}
	if m.config.Acceleration.ObjectStorage != nil {
		return Snapshot{}, ErrRecoveryUnsupported
	}
	backend, ok := m.store.(BackupBackend)
	if !ok {
		return Snapshot{}, ErrRecoveryUnsupported
	}
	fingerprint, err := m.config.DatasetFingerprint(id)
	if err != nil {
		return Snapshot{}, err
	}
	// The same configured snapshot budget bounds the backup's encoded payloads.
	// Programmatic catalogs must supply it just as YAML catalogs do after Load.
	if dataset.Limits.MaxBytes < 1 || dataset.Limits.MaxBytes > 1<<40 {
		return Snapshot{}, query.NewError("CONFIGURATION_ERROR", "Invalid snapshot backup byte limit")
	}
	return backend.Backup(ctx, BackupRequest{
		Dataset: id, Fingerprint: fingerprint, Destination: destination,
		MaxBytes: dataset.Limits.MaxBytes,
	})
}
