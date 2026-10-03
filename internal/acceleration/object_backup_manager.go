// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"reflect"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// ValidateMigrationCatalogs performs pure policy validation before credentials or
// storage are opened. Every field except acceleration storage must match exactly.
func ValidateMigrationCatalogs(source, target catalog.Config, id string) (MigrationRequest, error) {
	if source.Acceleration == nil || source.Acceleration.ObjectStorage == nil || target.Acceleration == nil || target.Acceleration.ObjectStorage != nil {
		return MigrationRequest{}, query.NewError("INVALID_ARGUMENT", "Migration requires an object-storage source catalog and a local target catalog")
	}
	sourcePolicy, targetPolicy := source, target
	sourceStorage, targetStorage := *source.Acceleration, *target.Acceleration
	sourceStorage.Directory, targetStorage.Directory = "", ""
	sourceStorage.ObjectStorage, targetStorage.ObjectStorage = nil, nil
	sourcePolicy.Acceleration, targetPolicy.Acceleration = &sourceStorage, &targetStorage
	if !reflect.DeepEqual(sourcePolicy, targetPolicy) {
		return MigrationRequest{}, query.NewError("INVALID_ARGUMENT", "Migration catalogs may differ only in acceleration storage location")
	}
	d, ok := source.Dataset(id)
	if !ok {
		return MigrationRequest{}, query.NewError("INVALID_ARGUMENT", "Unknown accelerated dataset")
	}
	if err := d.Limits.Validate(); err != nil {
		return MigrationRequest{}, query.NewError("CONFIGURATION_ERROR", "Invalid migration resource limits")
	}
	sourceFingerprint, err := source.DatasetFingerprint(id)
	if err != nil {
		return MigrationRequest{}, err
	}
	targetFingerprint, err := target.DatasetFingerprint(id)
	if err != nil {
		return MigrationRequest{}, err
	}
	return MigrationRequest{Dataset: id, SourceFingerprint: sourceFingerprint, TargetFingerprint: targetFingerprint, Destination: target.Acceleration.Directory, MaxBytes: d.Limits.MaxBytes, MaxRows: d.Limits.MaxRows}, nil
}

// MigrateBackup changes storage while preserving source, query, authorization,
// schema, schedule and resource policy. Target fingerprinting remains explicit.
// No executor is created and no source database credentials are resolved.
func (m *Manager) MigrateBackup(ctx context.Context, id string, target catalog.Config) (MigrationResult, error) {
	if err := ctx.Err(); err != nil {
		return MigrationResult{}, err
	}
	if m == nil {
		return MigrationResult{}, ErrRecoveryUnsupported
	}
	request, err := ValidateMigrationCatalogs(m.config, target, id)
	if err != nil {
		return MigrationResult{}, err
	}
	backend, ok := m.store.(MigrationBackend)
	if !ok {
		return MigrationResult{}, ErrRecoveryUnsupported
	}
	dataset, _ := m.config.Dataset(id) // validated above
	ctx, cancel := context.WithTimeout(ctx, dataset.Limits.Timeout)
	defer cancel()
	return backend.MigrateBackup(ctx, request)
}
