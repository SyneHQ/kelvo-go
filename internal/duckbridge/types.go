// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package duckbridge connects pinned DuckDB Arrow scan planning to Go readers.
package duckbridge

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/federation"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ScanPlan contains the exact source column order required by DuckDB and every
// required pushed filter. The producer must apply every filter or return an
// error; DuckDB does not reapply pushed filters after consuming Arrow batches.
type ScanPlan = federation.ScanPlan

// Filter is a typed predicate, never source SQL. Kinds are comparison, is_null,
// is_not_null, and, or. Comparisons use eq/ne/lt/le/gt/ge and exact lexical values
// with type int8/int16/int32/int64/uint8/uint16/uint32/uint64/bool.
type Filter = federation.Filter

// Producer returns an owned reader whose schema and columns follow plan.Columns.
// For an empty projection it returns one private constant column per source row;
// DuckDB consumes only the row count. Calls may overlap for independent scans.
type Producer func(context.Context, ScanPlan) (array.RecordReader, error)
