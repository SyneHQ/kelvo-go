// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func parquetReadTable(t *testing.T, data []byte) arrow.Table {
	t.Helper()
	table, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(data), parquet.NewReaderProperties(memory.DefaultAllocator), pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(table.Release)
	return table
}

func parquetAssertSchema(t *testing.T, want, got *arrow.Schema) {
	t.Helper()
	if want.NumFields() != got.NumFields() {
		t.Fatalf("schema field count changed: %d -> %d", want.NumFields(), got.NumFields())
	}
	for index, expected := range want.Fields() {
		actual := got.Field(index)
		if expected.Name != actual.Name || expected.Nullable != actual.Nullable || !arrow.TypeEqual(expected.Type, actual.Type) {
			t.Fatalf("column %d changed: %s -> %s", index, expected, actual)
		}
		// pqarrow adds PARQUET:field_id metadata. User field metadata must
		// survive in addition to that format metadata.
		for key, expectedValue := range expected.Metadata.ToMap() {
			if actualValue, ok := actual.Metadata.GetValue(key); !ok || actualValue != expectedValue {
				t.Fatalf("column %q lost metadata %q", expected.Name, key)
			}
		}
	}
}

func TestParquetSinkScalarRoundTrip(t *testing.T) {
	metadata := arrow.MetadataFrom(map[string]string{"source": "roundtrip"})
	fields := []arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "i16", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
		{Name: "u16", Type: arrow.PrimitiveTypes.Uint16, Nullable: true},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 38, Scale: 12}, Nullable: true, Metadata: metadata},
		{Name: "timestamp_ms", Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: true},
		{Name: "timestamp_us", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "timestamp_ns", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
		{Name: "timestamp_utc", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "binary", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}
	schema := arrow.NewSchema(fields, &metadata)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	// A leading discarded row exercises nonzero Arrow array offsets. Remaining
	// rows contain extremes, a NULL in every column, then negative/zero values.
	for row := 0; row < 4; row++ {
		if row == 2 {
			for _, field := range b.Fields() {
				field.AppendNull()
			}
			continue
		}
		negative := row == 3
		if negative {
			b.Field(0).(*array.Int8Builder).Append(math.MinInt8)
			b.Field(1).(*array.Int16Builder).Append(math.MinInt16)
			b.Field(2).(*array.Int32Builder).Append(math.MinInt32)
			b.Field(3).(*array.Int64Builder).Append(math.MinInt64)
		} else {
			b.Field(0).(*array.Int8Builder).Append(math.MaxInt8)
			b.Field(1).(*array.Int16Builder).Append(math.MaxInt16)
			b.Field(2).(*array.Int32Builder).Append(math.MaxInt32)
			b.Field(3).(*array.Int64Builder).Append(math.MaxInt64)
		}
		b.Field(4).(*array.Uint8Builder).Append(math.MaxUint8)
		b.Field(5).(*array.Uint16Builder).Append(math.MaxUint16)
		b.Field(6).(*array.Uint32Builder).Append(math.MaxUint32)
		b.Field(7).(*array.Uint64Builder).Append(math.MaxUint64)
		decimalText := "99999999999999999999999999.123456789012"
		if negative {
			decimalText = "-99999999999999999999999999.123456789012"
		}
		decimal, err := decimal128.FromString(decimalText, 38, 12)
		if err != nil {
			t.Fatal(err)
		}
		b.Field(8).(*array.Decimal128Builder).Append(decimal)
		for index, value := range []arrow.Timestamp{1_700_000_000_123, 1_700_000_000_123456, 1_700_000_000_123456789, 1_700_000_000_654321} {
			if negative {
				value = -value
			}
			b.Field(9 + index).(*array.TimestampBuilder).Append(value)
		}
		b.Field(13).(*array.StringBuilder).Append("utf8: नमस्ते, 世界\x00\n")
		b.Field(14).(*array.BinaryBuilder).Append([]byte{0, 255, 1, 128})
		b.Field(15).(*array.Date32Builder).Append(-10000)
		b.Field(16).(*array.BooleanBuilder).Append(!negative)
		b.Field(17).(*array.Float32Builder).Append(math.MaxFloat32)
		b.Field(18).(*array.Float64Builder).Append(-math.MaxFloat64)
	}
	record := b.NewRecordBatch()
	defer record.Release()
	sliced := record.NewSlice(1, 4)
	defer sliced.Release()
	expected := array.NewTableFromRecords(schema, []arrow.RecordBatch{sliced})
	defer expected.Release()
	var out bytes.Buffer
	sink := NewParquetSink(&out, query.DefaultLimits())
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(sliced); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	got := parquetReadTable(t, out.Bytes())
	parquetAssertSchema(t, schema, got.Schema())
	if expected.NumRows() != got.NumRows() || sink.Rows() != 3 {
		t.Fatalf("roundtrip values changed or wrong row count: %d", sink.Rows())
	}
	for index := 0; index < int(expected.NumCols()); index++ {
		if !array.ChunkedEqual(expected.Column(index).Data(), got.Column(index).Data()) {
			t.Fatalf("column %s roundtrip values changed", schema.Field(index).Name)
		}
	}
	reader, err := file.NewParquetReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	metadataKV := reader.MetaData().KeyValueMetadata()
	encoded := metadataKV.FindValue("ARROW:schema")
	if encoded == nil {
		t.Fatal("missing stored Arrow schema")
	}
	decoded, err := base64.StdEncoding.DecodeString(*encoded)
	if err != nil {
		t.Fatal(err)
	}
	original, err := flight.DeserializeSchema(decoded, memory.DefaultAllocator)
	if err != nil || !schema.Equal(original) || !schema.Metadata().Equal(original.Metadata()) {
		t.Fatalf("stored original schema changed: %v", err)
	}
	if value := metadataKV.FindValue("source"); value == nil || *value != "roundtrip" {
		t.Fatal("file metadata lost")
	}
}

func TestParquetSinkSecondTimestamps(t *testing.T) {
	for _, zone := range []string{"", "UTC", "Etc/UTC"} {
		t.Run(zone, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "created", Type: &arrow.TimestampType{Unit: arrow.Second, TimeZone: zone}, Nullable: true}}, nil)
			b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			defer b.Release()
			b.Field(0).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{-1, 1_700_000_000, 0}, []bool{true, true, false})
			record := b.NewRecordBatch()
			defer record.Release()
			var out bytes.Buffer
			sink := NewParquetSink(&out, query.DefaultLimits())
			if err := sink.Schema(schema); err != nil {
				t.Fatal(err)
			}
			if err := sink.Write(record); err != nil {
				t.Fatal(err)
			}
			if err := sink.Finish(); err != nil {
				t.Fatal(err)
			}
			got := parquetReadTable(t, out.Bytes())
			values := got.Column(0).Data().Chunk(0).(*array.Timestamp)
			if values.DataType().(*arrow.TimestampType).Unit != arrow.Millisecond || values.Value(0) != -1000 || values.Value(1) != 1_700_000_000_000 || !values.IsNull(2) {
				t.Fatalf("second timestamps changed: %s", values)
			}
			gotZone := values.DataType().(*arrow.TimestampType).TimeZone
			if (zone == "" && gotZone != "") || (zone != "" && gotZone != "UTC") {
				t.Fatalf("timezone semantics changed: %q -> %q", zone, gotZone)
			}
		})
	}
}

func TestParquetSinkEmptyAndBorrowedBatch(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{true: "empty", false: "borrowed"}[empty], func(t *testing.T) {
			out := new(parquetTestOutput)
			sink := NewParquetSink(out, query.DefaultLimits())
			if err := sink.Schema(schema); err != nil {
				t.Fatal(err)
			}
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			b := array.NewRecordBuilder(mem, schema)
			if !empty {
				b.Field(0).(*array.StringBuilder).Append("retained only during Write")
			}
			record := b.NewRecordBatch()
			if err := sink.Write(record); err != nil {
				t.Fatal(err)
			}
			record.Release()
			b.Release()
			mem.AssertSize(t, 0)
			if err := sink.Finish(); err != nil {
				t.Fatal(err)
			}
			before := out.Len()
			if err := sink.Finish(); err != nil || out.Len() != before || out.closed {
				t.Fatalf("Finish not idempotent or closed caller: %v", err)
			}
			got := parquetReadTable(t, out.Bytes())
			parquetAssertSchema(t, schema, got.Schema())
			if got.NumRows() != sink.Rows() || (empty && got.NumRows() != 0) || (!empty && got.NumRows() != 1) {
				t.Fatal("empty/borrowed roundtrip mismatch")
			}
		})
	}
	// Executors may report just the schema for an empty result.
	var out bytes.Buffer
	sink := NewParquetSink(&out, query.DefaultLimits())
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	got := parquetReadTable(t, out.Bytes())
	parquetAssertSchema(t, schema, got.Schema())
	if got.NumRows() != 0 {
		t.Fatal("schema-only empty result mismatch")
	}
}

func parquetIntRecord(values []int64) arrow.RecordBatch {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(values, nil)
	column := b.NewArray()
	defer column.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{column}, int64(len(values)))
}

func TestParquetSinkBoundedRowGroupsAndRows(t *testing.T) {
	values := make([]int64, parquetRowGroupRows*2+3)
	for index := range values {
		values[index] = int64(index)
	}
	record := parquetIntRecord(values)
	defer record.Release()
	var out bytes.Buffer
	sink := NewParquetSink(&out, query.DefaultLimits())
	if err := sink.Schema(record.Schema()); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	reader, err := file.NewParquetReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.NumRowGroups() != 3 || sink.Rows() != int64(len(values)) {
		t.Fatalf("row groups=%d rows=%d", reader.NumRowGroups(), sink.Rows())
	}
	for group := 0; group < reader.NumRowGroups(); group++ {
		if reader.RowGroup(group).NumRows() > parquetRowGroupRows {
			t.Fatal("unbounded row group")
		}
	}
	limits := query.DefaultLimits()
	limits.MaxRows = int64(len(values) - 1)
	var rejected bytes.Buffer
	sink = NewParquetSink(&rejected, limits)
	if err := sink.Schema(record.Schema()); err != nil {
		t.Fatal(err)
	}
	err = sink.Write(record)
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" || sink.Rows() != 0 || !errors.Is(sink.Finish(), err) {
		t.Fatalf("row limit not sticky: %v", err)
	}
	// Rows must also accumulate across separate borrowed batches.
	limits.MaxRows = int64(len(values))
	sink = NewParquetSink(io.Discard, limits)
	if err := sink.Schema(record.Schema()); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err == nil {
		t.Fatal("cumulative row limit not enforced")
	}
}

func TestParquetSinkEncodedByteLimits(t *testing.T) {
	record := parquetIntRecord([]int64{math.MaxInt64})
	defer record.Release()
	var complete bytes.Buffer
	sink := NewParquetSink(&complete, query.DefaultLimits())
	if err := sink.Schema(record.Schema()); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err != nil {
		t.Fatal(err)
	}
	beforeFooter := complete.Len()
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int64{3, 16, int64(complete.Len() - 1), int64(complete.Len())} {
		limits := query.DefaultLimits()
		limits.MaxBytes = limit
		var out bytes.Buffer
		sink := NewParquetSink(&out, limits)
		err := sink.Schema(record.Schema())
		if err == nil {
			err = sink.Write(record)
		}
		if limit > int64(beforeFooter) && err != nil {
			t.Fatalf("byte limit %d should permit data: %v", limit, err)
		}
		if err == nil {
			err = sink.Finish()
		}
		if limit == int64(complete.Len()) {
			if err != nil || !bytes.Equal(out.Bytes(), complete.Bytes()) {
				t.Fatalf("exact encoded byte budget failed: %v", err)
			}
			continue
		}
		if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" || int64(out.Len()) > limit || !errors.Is(sink.Finish(), err) {
			t.Fatalf("byte limit %d: size=%d error=%v", limit, out.Len(), err)
		}
	}
}

type parquetTestOutput struct {
	bytes.Buffer
	err    error
	short  bool
	closed bool
}

func (w *parquetTestOutput) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.short && len(p) > 0 {
		return w.Buffer.Write(p[:len(p)-1])
	}
	return w.Buffer.Write(p)
}
func (w *parquetTestOutput) Close() error { w.closed = true; return nil }

func TestParquetSinkOutputFailures(t *testing.T) {
	record := parquetIntRecord([]int64{42})
	defer record.Release()
	for _, stage := range []string{"header", "data", "footer", "short"} {
		t.Run(stage, func(t *testing.T) {
			failure := errors.New("output failed")
			out := new(parquetTestOutput)
			sink := NewParquetSink(out, query.DefaultLimits())
			if stage == "header" {
				out.err = failure
			}
			err := sink.Schema(record.Schema())
			if stage == "data" {
				out.err = failure
			}
			if stage == "short" {
				out.short = true
				failure = io.ErrShortWrite
			}
			if err == nil {
				err = sink.Write(record)
			}
			if stage == "footer" {
				out.err = failure
			}
			if err == nil {
				err = sink.Finish()
			}
			if !errors.Is(err, failure) || !errors.Is(sink.Finish(), failure) || out.closed {
				t.Fatalf("output failure not preserved or caller closed: %v", err)
			}
			out.err, out.short = nil, false
			before := out.Len()
			if !errors.Is(sink.Write(record), failure) || out.Len() != before {
				t.Fatal("failed sink resumed writing")
			}
		})
	}
}

func TestParquetSinkRejectsLossyTypesAndValues(t *testing.T) {
	types := []arrow.DataType{
		arrow.Null, arrow.FixedWidthTypes.Date64, arrow.FixedWidthTypes.Time64ns,
		arrow.BinaryTypes.LargeString, &arrow.FixedSizeBinaryType{ByteWidth: 8},
		&arrow.Decimal128Type{Precision: 20, Scale: -1},
		&arrow.Decimal256Type{Precision: 40, Scale: 2},
		&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
		&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Kolkata"},
		arrow.ListOf(arrow.PrimitiveTypes.Int64),
	}
	for _, typ := range types {
		t.Run(typ.String(), func(t *testing.T) {
			var out bytes.Buffer
			sink := NewParquetSink(&out, query.DefaultLimits())
			err := sink.Schema(arrow.NewSchema([]arrow.Field{{Name: "unsupported", Type: typ, Nullable: true}}, nil))
			if err == nil || out.Len() != 0 || !errors.Is(sink.Finish(), err) {
				t.Fatalf("lossy schema accepted: %v", err)
			}
		})
	}
	for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
		for _, value := range []arrow.Timestamp{math.MaxInt64, math.MinInt64, -math.MaxInt64} {
			schema := arrow.NewSchema([]arrow.Field{{Name: "timestamp", Type: &arrow.TimestampType{Unit: unit}}}, nil)
			b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			b.Field(0).(*array.TimestampBuilder).Append(value)
			record := b.NewRecordBatch()
			b.Release()
			sink := NewParquetSink(io.Discard, query.DefaultLimits())
			if err := sink.Schema(schema); err != nil {
				t.Fatal(err)
			}
			err := sink.Write(record)
			record.Release()
			if err == nil || !errors.Is(sink.Finish(), err) {
				t.Fatalf("unsafe timestamp accepted: unit=%s value=%d", unit, value)
			}
		}
	}
}

func TestParquetSinkBoundsVariableWidthRowGroups(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for row := 0; row < 20; row++ {
		b.Field(0).(*array.StringBuilder).Append(strings.Repeat("x", 64<<10))
	}
	record := b.NewRecordBatch()
	defer record.Release()
	limits := query.DefaultLimits()
	limits.MemoryMB = 1 // 256 KiB estimated row-group payload.
	var out bytes.Buffer
	sink := NewParquetSink(&out, limits)
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	reader, err := file.NewParquetReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for group := 0; group < reader.NumRowGroups(); group++ {
		if rows := reader.RowGroup(group).NumRows(); rows > 3 {
			t.Fatalf("variable-width row group exceeds memory budget: %d rows", rows)
		}
	}
	b.Field(0).(*array.StringBuilder).Append(strings.Repeat("y", 256<<10))
	large := b.NewRecordBatch()
	defer large.Release()
	sink = NewParquetSink(io.Discard, limits)
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(large); err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("oversized row accepted: %v", err)
	}
}

func TestParquetSinkRequiresStableSchema(t *testing.T) {
	record := parquetIntRecord([]int64{1})
	defer record.Release()
	sink := NewParquetSink(io.Discard, query.DefaultLimits())
	if sink.Write(record) == nil || sink.Finish() == nil {
		t.Fatal("missing schema accepted")
	}
	sink = NewParquetSink(io.Discard, query.DefaultLimits())
	if err := sink.Schema(record.Schema()); err != nil {
		t.Fatal(err)
	}
	other := arrow.NewSchema([]arrow.Field{{Name: "changed", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err := sink.Schema(other); err == nil || !errors.Is(sink.Finish(), err) {
		t.Fatal("changed schema accepted")
	}
	for _, fields := range [][]arrow.Field{
		{{Name: "n", Type: arrow.PrimitiveTypes.Int64}, {Name: "N", Type: arrow.PrimitiveTypes.Int64}},
		{{Name: "", Type: arrow.PrimitiveTypes.Int64}},
	} {
		sink := NewParquetSink(io.Discard, query.DefaultLimits())
		if sink.Schema(arrow.NewSchema(fields, nil)) == nil {
			t.Fatal("ambiguous column name accepted")
		}
	}
}

func TestParquetSinkAbort(t *testing.T) {
	record := parquetIntRecord([]int64{42})
	defer record.Release()
	var out bytes.Buffer
	sink := NewParquetSink(&out, query.DefaultLimits())
	if err := sink.Schema(record.Schema()); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err != nil {
		t.Fatal(err)
	}
	before := out.Len()
	sink.Abort()
	sink.Abort()
	if out.Len() != before || sink.Finish() == nil || sink.Write(record) == nil {
		t.Fatal("aborted sink wrote a footer or accepted more data")
	}
	sink = NewParquetSink(&out, query.DefaultLimits())
	if err := sink.Schema(record.Schema()); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	before = out.Len()
	sink.Abort()
	if out.Len() != before || sink.Finish() != nil {
		t.Fatal("Abort changed successfully finished sink")
	}
}

func TestParquetSinkLimitsRowGroupMetadata(t *testing.T) {
	record := parquetIntRecord([]int64{42})
	defer record.Release()
	limits := query.DefaultLimits()
	limits.MemoryMB = 1
	var out bytes.Buffer
	sink := NewParquetSink(&out, limits)
	if err := sink.Schema(record.Schema()); err != nil {
		t.Fatal(err)
	}
	for group := int64(0); group < sink.maxGroups; group++ {
		if err := sink.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	before := out.Len()
	err := sink.Write(record)
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" || out.Len() != before || !errors.Is(sink.Finish(), err) {
		t.Fatalf("row-group metadata limit not enforced: %v", err)
	}
	fields := make([]arrow.Field, 4097)
	for index := range fields {
		fields[index] = arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64}
	}
	sink = NewParquetSink(io.Discard, limits)
	if err := sink.Schema(arrow.NewSchema(fields, nil)); err == nil {
		t.Fatal("excessively wide schema accepted")
	}
}

func TestParquetSinkRejectsInvalidScalarValues(t *testing.T) {
	for _, kind := range []string{"date", "decimal", "required"} {
		t.Run(kind, func(t *testing.T) {
			var typ arrow.DataType = arrow.PrimitiveTypes.Int64
			switch kind {
			case "date":
				typ = arrow.FixedWidthTypes.Date32
			case "decimal":
				typ = &arrow.Decimal128Type{Precision: 3, Scale: 1}
			}
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: typ}}, nil)
			b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			switch kind {
			case "date":
				b.Field(0).(*array.Date32Builder).Append(math.MaxInt32)
			case "decimal":
				b.Field(0).(*array.Decimal128Builder).Append(decimal128.FromI64(1000))
			case "required":
				b.Field(0).AppendNull()
			}
			record := b.NewRecordBatch()
			defer record.Release()
			b.Release()
			sink := NewParquetSink(io.Discard, query.DefaultLimits())
			if err := sink.Schema(schema); err != nil {
				t.Fatal(err)
			}
			err := sink.Write(record)
			if err == nil || !errors.Is(sink.Finish(), err) {
				t.Fatalf("invalid %s value accepted", kind)
			}
		})
	}
}
