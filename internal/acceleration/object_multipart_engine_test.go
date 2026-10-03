//go:build duckdb_arrow && (linux || darwin)

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration_test

import (
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/engine/duckdb"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestObjectMultipartDatasetLargerThanFourGiB(t *testing.T) {
	// Keep the real engine dependency outside the storage package. The shared
	// fixture retains its internal staging assertions and exact result checks.
	acceleration.RunObjectMultipartLargeFixture(t, func(config catalog.Config, limits query.Limits) (query.Executor, error) {
		return duckdb.New(config, limits)
	})
}
