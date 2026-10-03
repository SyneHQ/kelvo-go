// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"os"
	"time"
)

// MigrationRequest is an operator-only remote-to-local storage migration.
// Both fingerprints must be derived from explicitly compared active catalogs;
// the caller must not select arbitrary replacement authorization policies.
type MigrationRequest struct {
	Dataset, SourceFingerprint, TargetFingerprint, Destination string
	MaxBytes, MaxRows                                          int64
}

type MigrationResult struct {
	Snapshot          Snapshot `yaml:"snapshot"`
	SourceFingerprint string   `yaml:"source_fingerprint"`
	SourceSHA256      string   `yaml:"source_generation_sha256"`
}

type MigrationBackend interface {
	MigrateBackup(context.Context, MigrationRequest) (MigrationResult, error)
}

// A local store measures freshness with its host clock; object stores use the
// observed service clock. Refuse material skew instead of quietly making a
// restored generation younger. The original RefreshedAt is never rewritten.
const migrationClockSkew = 5 * time.Second

func migrationAgeCompatible(snapshot Snapshot) bool {
	local := max(time.Duration(0), time.Since(snapshot.RefreshedAt))
	observed := snapshot.Age()
	return local-observed <= migrationClockSkew && observed-local <= migrationClockSkew
}

func (backend *objectBackend) MigrateBackup(ctx context.Context, request MigrationRequest) (MigrationResult, error) {
	return backend.migrateBackup(ctx, request, backupCopy, nil)
}

func (backend *objectBackend) migrateBackup(ctx context.Context, request MigrationRequest, copyFile backupCopyFunc, syncParent func(*os.File) error) (MigrationResult, error) {
	if backend == nil {
		return MigrationResult{}, ErrRecoveryUnsupported
	}
	if err := backend.check(ctx, request.Dataset); err != nil {
		return MigrationResult{}, err
	}
	if !storeDigest.MatchString(request.SourceFingerprint) || !storeDigest.MatchString(request.TargetFingerprint) || request.SourceFingerprint == request.TargetFingerprint || request.MaxBytes < 1 || request.MaxBytes > MaxBackupBytes || request.MaxRows < 1 || request.MaxRows > 100000000 || copyFile == nil {
		return MigrationResult{}, errors.New("migration requires distinct authorized fingerprints and bounded row and byte limits")
	}
	return backupObjectToLocal(ctx, backend, request, copyFile, syncParent)
}

var _ MigrationBackend = (*objectBackend)(nil)
