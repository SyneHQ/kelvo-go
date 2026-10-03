//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"os"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestObjectSnapshotReaderCompletesGuardedScanWithIndependentProvenance(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "account_id", Type: arrow.PrimitiveTypes.Int64}, {Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{42, 7, 42}, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 99, 0}, []bool{true, true, false})
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	local := snapshotFixture(t, record)
	data, err := os.ReadFile(local.Path)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, digest, _ := snapshotRangeFixture(t, data, nil)
	read := local.LocalSnapshot
	read.Scan.MaxRows = 3
	source := catalog.Source{ID: local.ID, Type: "parquet", Path: endpoint,
		Range: &catalog.ObjectRange{URL: endpoint, Bytes: int64(len(data))},
		ObjectSnapshot: &catalog.ObjectSnapshotRead{Dataset: read.Dataset, Generation: read.Generation,
			SchemaSHA256: read.SchemaSHA256, Scan: read.Scan,
			Parts: []catalog.ObjectSnapshotPart{{URL: endpoint, Rows: 3, Bytes: int64(len(data)), SHA256: digest}}}}
	ctx := snapshotPolicy(t, []string{"id"}, &access.Predicate{Kind: "comparison", Column: "account_id", Type: "int64", Op: "eq", Value: "42"})
	table := snapshotTableFor(t, ctx, source)
	// Caller reuse cannot change the selected capability or post-scan row check.
	source.ObjectSnapshot.Parts[0].Rows = 999
	source.ObjectSnapshot.Parts[0].URL = "changed"
	source.ObjectSnapshot.Scan.MaxRows = 999
	source.Range.URL = "changed"
	reader, err := table.Scan(ctx, duckbridge.ScanPlan{Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	var rows int
	for reader.Next() {
		batch := reader.RecordBatch()
		if batch.NumCols() != 1 || batch.Schema().Field(0).Name != "id" {
			t.Fatal("hidden source schema escaped")
		}
		ids := batch.Column(0).(*array.Int64)
		for i := range ids.Len() {
			if rows == 0 && (ids.IsNull(i) || ids.Value(i) != 1) || rows == 1 && !ids.IsNull(i) || rows > 1 {
				t.Fatal("guarded object values or NULLs changed")
			}
			rows++
		}
	}
	err = reader.Err()
	reader.Release()
	if err != nil || rows != 2 {
		t.Fatal("object part did not complete with exact row provenance", rows, err)
	}
	if _, err := snapshotScan(t, table, "id"); err == nil {
		t.Fatal("rescan ignored its original cumulative raw budget")
	}
}
