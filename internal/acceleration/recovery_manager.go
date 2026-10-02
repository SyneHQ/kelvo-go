// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// Inventory exposes only datasets in the current operator-owned catalog.
func (m *Manager) Inventory(ctx context.Context, id string) ([]Generation, error) {
	if _, ok := m.config.Dataset(id); !ok {
		return nil, query.NewError("INVALID_ARGUMENT", "Unknown accelerated dataset")
	}
	backend, ok := m.store.(RecoveryBackend)
	if !ok {
		return nil, ErrRecoveryUnsupported
	}
	return backend.Inventory(ctx, id)
}

// Restore uses the current catalog fingerprint, never one supplied by a caller.
// The expected generation fences an operator action against a concurrent refresh.
func (m *Manager) Restore(ctx context.Context, id, generation, expected string) (Snapshot, error) {
	if _, ok := m.config.Dataset(id); !ok {
		return Snapshot{}, query.NewError("INVALID_ARGUMENT", "Unknown accelerated dataset")
	}
	fingerprint, err := m.config.DatasetFingerprint(id)
	if err != nil {
		return Snapshot{}, err
	}
	backend, ok := m.store.(RecoveryBackend)
	if !ok {
		return Snapshot{}, ErrRecoveryUnsupported
	}
	return backend.Restore(ctx, RestoreRequest{Dataset: id, Generation: generation, Fingerprint: fingerprint, ExpectedGeneration: expected})
}
