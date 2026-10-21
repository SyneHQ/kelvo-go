//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func coverageOf(t *testing.T, column string, typ arrow.DataType, fill func(array.Builder)) string {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "TS", Type: typ, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	fill(builder.Field(0))
	record := builder.NewRecordBatch()
	defer record.Release()
	c := &coverage{column: column}
	c.observe(record)
	c.observe(record) // a second batch must not change the maximum
	return c.Value()
}

func TestCoverageIsMaxOfTheColumn(t *testing.T) {
	ints := func(b array.Builder) {
		b.(*array.Int64Builder).AppendValues([]int64{3, 9, 4}, nil)
		b.AppendNull()
	}
	if got := coverageOf(t, "ts", arrow.PrimitiveTypes.Int64, ints); got != "9" {
		t.Fatalf("int max %q", got)
	}
	stamps := func(b array.Builder) {
		b.(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1_700_000_000_000_000, 1_800_000_000_000_001}, nil)
	}
	want := time.UnixMicro(1_800_000_000_000_001).UTC().Format(time.RFC3339Nano)
	if got := coverageOf(t, "ts", &arrow.TimestampType{Unit: arrow.Microsecond}, stamps); got != want {
		t.Fatalf("timestamp max %q, want %q", got, want)
	}
	dates := func(b array.Builder) { b.(*array.Date32Builder).AppendValues([]arrow.Date32{19000, 19500}, nil) }
	if got := coverageOf(t, "ts", arrow.FixedWidthTypes.Date32, dates); got != "2023-05-23T00:00:00Z" {
		t.Fatalf("date max %q", got)
	}
	nulls := func(b array.Builder) { b.AppendNull() }
	if got := coverageOf(t, "ts", arrow.PrimitiveTypes.Int64, nulls); got != "" {
		t.Fatalf("all NULL must not cover: %q", got)
	}
	if got := coverageOf(t, "other", arrow.PrimitiveTypes.Int64, ints); got != "" {
		t.Fatalf("missing column must not cover: %q", got)
	}
	text := func(b array.Builder) { b.(*array.StringBuilder).Append("2026-01-01") }
	if got := coverageOf(t, "ts", arrow.BinaryTypes.String, text); got != "" {
		t.Fatalf("unsupported type must not cover: %q", got)
	}
}

func TestRefreshRecordsCoveredUntilOnlyWithRoute(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		c, m, _ := managerFixture(t)
		_ = c
		if multipart {
			m.config.Acceleration.Datasets[0].Multipart = &catalog.MultipartConfig{MaxParts: 2, MaxPartBytes: 1 << 20}
		}
		snapshot, err := m.Refresh(context.Background(), "orders_fast", false)
		if err != nil || snapshot.CoveredUntil != "" {
			t.Fatalf("multipart=%v: no route must record nothing: %+v %v", multipart, snapshot, err)
		}
		m.config.Acceleration.Datasets[0].Route = &catalog.Route{TimeColumn: "id", LiveSource: "source"}
		if snapshot, err = m.Refresh(context.Background(), "orders_fast", false); err != nil || snapshot.CoveredUntil != "7" {
			t.Fatalf("multipart=%v: committed %+v %v", multipart, snapshot, err)
		}
		if status, err := m.Status("orders_fast"); err != nil || status.CoveredUntil != "7" {
			t.Fatalf("multipart=%v: status %+v %v", multipart, status, err)
		}
	}
}
