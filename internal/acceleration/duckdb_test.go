//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	duckdbengine "github.com/SYNEHQ/kelvo-go/internal/engine/duckdb"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestParquetDuckDBSemanticRoundTrip(t *testing.T) {
	input := parquetDuckDBFixture(t, false)
	defer input.Release()
	want := parquetDuckDBFixture(t, true)
	defer want.Release()
	for _, empty := range []bool{false, true} {
		name := "values"
		if empty {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			rows := input.NumRows()
			if empty {
				rows = 0
			}
			source := input.NewSlice(0, rows)
			defer source.Release()
			expected := want.NewSlice(0, rows)
			defer expected.Release()
			limits := query.DefaultLimits()
			limits.MaxRows, limits.MaxBytes, limits.Threads = 32, 1<<20, 1
			path := filepath.Join(t.TempDir(), "accelerated.parquet")
			out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Close()
			writer := acceleration.NewParquetSink(out, limits)
			defer writer.Abort()
			if err := writer.Schema(source.Schema()); err != nil {
				t.Fatal(err)
			}
			// Read through the real engine after separate writes, including a
			// NULL-only data row in the second row group.
			if rows != 0 {
				for _, bounds := range [][2]int64{{0, 1}, {1, rows}} {
					part := source.NewSlice(bounds[0], bounds[1])
					err := writer.Write(part)
					part.Release()
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := writer.Finish(); err != nil {
				t.Fatal(err)
			}
			if err := out.Close(); err != nil {
				t.Fatal(err)
			}
			engine, err := duckdbengine.New(catalog.Config{Sources: []catalog.Source{{ID: "cached", Type: "parquet", Path: path}}}, limits)
			if err != nil {
				t.Fatal(err)
			}
			sink := &parquetDuckDBExpectedSink{want: expected}
			stats, err := engine.Execute(context.Background(), query.Request{SQL: "SELECT * FROM cached ORDER BY row_id", Sources: []string{"cached"}}, sink)
			if err != nil {
				t.Fatalf("DuckDB execution: %v; semantic assertion: %v", err, sink.err)
			}
			if sink.err != nil || !sink.sawSchema || sink.rows != rows || stats.Rows != rows {
				t.Fatalf("DuckDB roundtrip: rows=%d stats=%d schema=%t error=%v", sink.rows, stats.Rows, sink.sawSchema, sink.err)
			}
		})
	}
}

// The expected fixture is fixed independently of the reader's inferred types:
// Parquet seconds/milliseconds become DuckDB microseconds without losing their
// signed instant values. Unzoned nanoseconds retain all nine decimal places.
func parquetDuckDBFixture(t *testing.T, expected bool) arrow.RecordBatch {
	t.Helper()
	secondUnit, millisecondUnit := arrow.Second, arrow.Millisecond
	timestamps := [5][2]arrow.Timestamp{
		{-1, 1_700_000_000},
		{-1, 1_700_000_000_123},
		{-1, 1_700_000_000_123456},
		{-1, 1_700_000_000_123456789},
		{-1, 1_700_000_000_654321},
	}
	if expected {
		secondUnit, millisecondUnit = arrow.Microsecond, arrow.Microsecond
		timestamps = [5][2]arrow.Timestamp{
			{-1_000_000, 1_700_000_000_000000},
			{-1_000, 1_700_000_000_123000},
			{-1, 1_700_000_000_123456},
			{-1, 1_700_000_000_123456789},
			{-1, 1_700_000_000_654321},
		}
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "row_id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "signed64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "unsigned64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 38, Scale: 9}, Nullable: true},
		{Name: "date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "timestamp_s", Type: &arrow.TimestampType{Unit: secondUnit}, Nullable: true},
		{Name: "timestamp_ms", Type: &arrow.TimestampType{Unit: millisecondUnit}, Nullable: true},
		{Name: "timestamp_us", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "timestamp_ns", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
		{Name: "timestamp_utc", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "signed8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "signed16", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "unsigned8", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
		{Name: "unsigned16", Type: arrow.PrimitiveTypes.Uint16, Nullable: true},
		{Name: "unsigned32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for row := 0; row < 3; row++ {
		b.Field(0).(*array.Int32Builder).Append(int32(row))
		if row == 2 {
			for _, field := range b.Fields()[1:] {
				field.AppendNull()
			}
			continue
		}
		b.Field(1).(*array.Int64Builder).Append([2]int64{math.MinInt64, math.MaxInt64}[row])
		b.Field(2).(*array.Uint64Builder).Append([2]uint64{math.MaxUint64, 0}[row])
		decimal, err := decimal128.FromString([2]string{
			"-99999999999999999999999999999.999999999",
			"99999999999999999999999999999.999999999",
		}[row], 38, 9)
		if err != nil {
			t.Fatal(err)
		}
		b.Field(3).(*array.Decimal128Builder).Append(decimal)
		b.Field(4).(*array.Date32Builder).Append([2]arrow.Date32{-10000, 20000}[row])
		for column, values := range timestamps {
			b.Field(5 + column).(*array.TimestampBuilder).Append(values[row])
		}
		b.Field(10).(*array.StringBuilder).Append([2]string{"", "नमस्ते, 世界"}[row])
		b.Field(11).(*array.Int8Builder).Append([2]int8{math.MinInt8, math.MaxInt8}[row])
		b.Field(12).(*array.Int16Builder).Append([2]int16{math.MinInt16, math.MaxInt16}[row])
		b.Field(13).(*array.Uint8Builder).Append([2]uint8{0, math.MaxUint8}[row])
		b.Field(14).(*array.Uint16Builder).Append([2]uint16{0, math.MaxUint16}[row])
		b.Field(15).(*array.Uint32Builder).Append([2]uint32{0, math.MaxUint32}[row])
	}
	return b.NewRecordBatch()
}

type parquetDuckDBExpectedSink struct {
	want      arrow.RecordBatch
	rows      int64
	sawSchema bool
	err       error
}

func (s *parquetDuckDBExpectedSink) fail(format string, args ...any) error {
	s.err = fmt.Errorf(format, args...)
	return s.err
}

func (s *parquetDuckDBExpectedSink) Schema(got *arrow.Schema) error {
	if got.NumFields() != s.want.Schema().NumFields() {
		return s.fail("DuckDB schema width changed: %d", got.NumFields())
	}
	for index, expected := range s.want.Schema().Fields() {
		actual := got.Field(index)
		actualType := actual.Type
		if timestamp, ok := actualType.(*arrow.TimestampType); ok && expected.Name == "timestamp_utc" {
			if timestamp.TimeZone != "UTC" && timestamp.TimeZone != "Etc/UTC" {
				return s.fail("UTC instant annotation changed to %q", timestamp.TimeZone)
			}
			// DuckDB spells its canonical UTC zone Etc/UTC. These annotations
			// have identical semantics; arbitrary named zones are not accepted.
			actualType = &arrow.TimestampType{Unit: timestamp.Unit, TimeZone: "UTC"}
		}
		if actual.Name != expected.Name || actual.Nullable != expected.Nullable || !arrow.TypeEqual(actualType, expected.Type) {
			return s.fail("DuckDB column %d: want %s, got %s", index, expected, actual)
		}
	}
	s.sawSchema = true
	return nil
}

func (s *parquetDuckDBExpectedSink) Write(record arrow.RecordBatch) error {
	if !s.sawSchema || record.NumRows() > s.want.NumRows()-s.rows {
		return s.fail("unexpected DuckDB batch: rows=%d before=%d", record.NumRows(), s.rows)
	}
	for column, actual := range record.Columns() {
		expected := s.want.Column(column)
		if timestamp, ok := actual.(*array.Timestamp); ok && expected.DataType().(*arrow.TimestampType).TimeZone == "UTC" {
			wanted := expected.(*array.Timestamp)
			for row := 0; row < actual.Len(); row++ {
				index := int(s.rows) + row
				if timestamp.IsNull(row) != wanted.IsNull(index) || (!timestamp.IsNull(row) && timestamp.Value(row) != wanted.Value(index)) {
					return s.fail("UTC timestamp value changed at row %d", index)
				}
			}
			continue
		}
		if !array.SliceEqual(actual, 0, record.NumRows(), expected, s.rows, s.rows+record.NumRows()) {
			return s.fail("DuckDB column %q changed exact values or NULLs", record.Schema().Field(column).Name)
		}
	}
	s.rows += record.NumRows()
	return nil
}
