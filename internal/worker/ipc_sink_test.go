// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

var resultEOS = []byte{255, 255, 255, 255, 0, 0, 0, 0}

func resultRecords(allocator memory.Allocator) []arrow.RecordBatch {
	fields := []arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "i16", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 20, Scale: 4}, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "Asia/Kolkata"}, Nullable: true},
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	plainSchema := arrow.NewSchema(fields, nil)
	dictionaryType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	schema := arrow.NewSchema(append(fields, arrow.Field{Name: "category", Type: dictionaryType, Nullable: true}), nil)
	var records []arrow.RecordBatch
	for batch := 0; batch < 3; batch++ {
		builder := array.NewRecordBuilder(allocator, plainSchema)
		indicesBuilder := array.NewInt8Builder(allocator)
		for row := 0; row < 128; row++ {
			if row%17 == 0 {
				for column := 0; column < len(plainSchema.Fields()); column++ {
					builder.Field(column).AppendNull()
				}
				indicesBuilder.AppendNull()
				continue
			}
			offset := int64(batch*128 + row)
			builder.Field(0).(*array.Int8Builder).Append(int8(row%127 - 63))
			builder.Field(1).(*array.Int16Builder).Append(int16(-32000 + offset))
			builder.Field(2).(*array.Int32Builder).Append(int32(math.MinInt32 + offset))
			builder.Field(3).(*array.Int64Builder).Append(math.MaxInt64 - offset)
			builder.Field(4).(*array.Uint64Builder).Append(math.MaxUint64 - uint64(offset))
			builder.Field(5).(*array.Decimal128Builder).Append(decimal128.FromI64(-1234567890123456789 + offset))
			builder.Field(6).(*array.TimestampBuilder).Append(arrow.Timestamp(1700000000123456789 + offset))
			builder.Field(7).(*array.StringBuilder).Append(strings.Repeat("compressible-value-", 64))
			indicesBuilder.Append(int8(row % 2))
		}
		plain := builder.NewRecordBatch()
		builder.Release()
		indices := indicesBuilder.NewArray()
		indicesBuilder.Release()
		labelsBuilder := array.NewStringBuilder(allocator)
		labels := []string{"first-category", "second-category"}
		if batch == 2 {
			labels = []string{"replacement-category", "last-category"}
		}
		labelsBuilder.AppendValues(labels, nil)
		labelsArray := labelsBuilder.NewArray()
		labelsBuilder.Release()
		dictionary := array.NewDictionaryArray(dictionaryType, indices, labelsArray)
		indices.Release()
		labelsArray.Release()
		columns := append(append([]arrow.Array(nil), plain.Columns()...), dictionary)
		records = append(records, array.NewRecordBatch(schema, columns, plain.NumRows()))
		plain.Release()
		dictionary.Release()
	}
	return records
}

func releaseResultRecords(records []arrow.RecordBatch) {
	for _, record := range records {
		record.Release()
	}
}

func encodeResultRecords(t *testing.T, records []arrow.RecordBatch, compression string) []byte {
	t.Helper()
	limits := query.DefaultLimits()
	limits.ResultCompression = compression
	var output bytes.Buffer
	sink := NewIPCSink(&output, limits)
	defer sink.Abort()
	if err := sink.Schema(records[0].Schema()); err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if err := sink.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	if sink.EncodedBytes() != int64(output.Len()) || !bytes.HasSuffix(output.Bytes(), resultEOS) {
		t.Fatal("encoded accounting or successful EOS changed")
	}
	return output.Bytes()
}

func TestIPCSinkCompressionPreservesExactArrowBatches(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	records := resultRecords(allocator)
	defer releaseResultRecords(records)
	for _, compression := range []string{"", "none", "lz4_frame"} {
		t.Run("compression="+compression, func(t *testing.T) {
			encoded := encodeResultRecords(t, records, compression)
			reader, err := ipc.NewReader(bytes.NewReader(encoded), ipc.WithAllocator(allocator))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Release()
			if !reader.Schema().Equal(records[0].Schema()) {
				t.Fatal("integer widths, decimal scale, timezone or dictionary schema changed")
			}
			batch := 0
			for reader.Next() {
				if batch >= len(records) {
					t.Fatal("unexpected batch")
				}
				got, want := reader.RecordBatch(), records[batch]
				if got.NumRows() != want.NumRows() {
					t.Fatal("row count changed")
				}
				for column := 0; column < int(got.NumCols()); column++ {
					if !array.Equal(got.Column(column), want.Column(column)) {
						t.Fatalf("batch %d column %d changed values or NULLs", batch, column)
					}
				}
				batch++
			}
			if reader.Err() != nil || batch != len(records) {
				t.Fatalf("incomplete round trip: batches=%d error=%v", batch, reader.Err())
			}
		})
	}
	uncompressed := encodeResultRecords(t, records, "")
	if !bytes.Equal(uncompressed, encodeResultRecords(t, records, "none")) {
		t.Fatal("default and explicit none encodings differ")
	}
	var legacy bytes.Buffer
	writer := ipc.NewWriter(&legacy, ipc.WithSchema(records[0].Schema()))
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(uncompressed, legacy.Bytes()) {
		t.Fatal("default bytes changed from the original uncompressed Arrow writer")
	}
	compressed := encodeResultRecords(t, records, "lz4_frame")
	if len(compressed) >= len(uncompressed)/2 {
		t.Fatalf("compressible fixture was not compressed: none=%d lz4=%d", len(uncompressed), len(compressed))
	}
}

func TestIPCSinkCompressedResultKeepsDecodedAndRowLimits(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	records := resultRecords(allocator)
	defer releaseResultRecords(records)
	for _, limit := range []string{"decoded bytes", "rows"} {
		t.Run(limit, func(t *testing.T) {
			limits := query.DefaultLimits()
			limits.ResultCompression = "lz4_frame"
			if limit == "decoded bytes" {
				limits.MaxBytes = arrowutil.TotalRecordSize(records[0]) + arrowutil.TotalRecordSize(records[1])/2
			} else {
				limits.MaxRows = records[0].NumRows() + records[1].NumRows()/2
			}
			var output bytes.Buffer
			sink := NewIPCSink(&output, limits)
			if err := sink.Schema(records[0].Schema()); err != nil {
				t.Fatal(err)
			}
			if err := sink.Write(records[0]); err != nil {
				t.Fatal(err)
			}
			before := output.Len()
			err := sink.Write(records[1])
			if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
				t.Fatalf("%s limit bypassed: %v", limit, err)
			}
			if !errors.Is(sink.Finish(), err) || output.Len() != before || sink.EncodedBytes() != int64(before) {
				t.Fatal("failed result emitted or counted cleanup bytes")
			}
			if bytes.HasSuffix(output.Bytes(), resultEOS) {
				t.Fatal("failed result emitted successful EOS")
			}
		})
	}
}

func TestIPCSinkCompressedResultKeepsEncodedLimitIncludingEOS(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: strings.Repeat("column", 256), Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(42)
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 1)
	column.Release()
	defer record.Release()
	complete := encodeResultRecords(t, []arrow.RecordBatch{record}, "lz4_frame")
	for _, maxBytes := range []int64{1024, int64(len(complete) - 1), int64(len(complete))} {
		limits := query.DefaultLimits()
		limits.ResultCompression = "lz4_frame"
		limits.MaxBytes = maxBytes
		var output bytes.Buffer
		sink := NewIPCSink(&output, limits)
		if err := sink.Schema(schema); err != nil {
			t.Fatal(err)
		}
		err := sink.Write(record)
		if err == nil {
			err = sink.Finish()
		}
		if maxBytes == int64(len(complete)) {
			if err != nil || !bytes.Equal(output.Bytes(), complete) {
				t.Fatalf("exact encoded limit failed: %v", err)
			}
		} else if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
			t.Fatalf("encoded limit %d bypassed: %v", maxBytes, err)
		} else if sink.Finish() == nil || bytes.HasSuffix(output.Bytes(), resultEOS) {
			t.Fatal("encoded failure became a successful result")
		}
		if int64(output.Len()) > maxBytes || sink.EncodedBytes() != int64(output.Len()) {
			t.Fatal("encoded bytes exceeded or misreported the cap")
		}
	}
}

type cancelledResultWriter struct {
	bytes.Buffer
	cancelled bool
}

func (w *cancelledResultWriter) Write(data []byte) (int, error) {
	if w.cancelled {
		return 0, context.Canceled
	}
	return w.Buffer.Write(data)
}

func TestIPCSinkCompressionOutputCancellationDoesNotEmitEOS(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	records := resultRecords(allocator)
	defer releaseResultRecords(records)
	limits := query.DefaultLimits()
	limits.ResultCompression = "lz4_frame"
	var output cancelledResultWriter
	sink := NewIPCSink(&output, limits)
	if err := sink.Schema(records[0].Schema()); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(records[0]); err != nil {
		t.Fatal(err)
	}
	before := output.Len()
	output.cancelled = true
	if err := sink.Write(records[1]); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled output: %v", err)
	}
	if err := sink.Finish(); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled result finished: %v", err)
	}
	if output.Len() != before || sink.EncodedBytes() != int64(before) || bytes.HasSuffix(output.Bytes(), resultEOS) {
		t.Fatal("cancellation emitted or counted additional result bytes")
	}
}

func TestIPCSinkAbortAfterSuccessfulBatchesReleasesDictionaries(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	records := resultRecords(allocator)
	defer releaseResultRecords(records)
	limits := query.DefaultLimits()
	limits.ResultCompression = "lz4_frame"
	var output bytes.Buffer
	sink := NewIPCSink(&output, limits)
	defer sink.Abort()
	if err := sink.Schema(records[0].Schema()); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(records[0]); err != nil {
		t.Fatal(err)
	}
	// The executor can fail or be cancelled after a successfully delivered batch,
	// without any sink method returning an error. Abort must release that batch's
	// retained dictionary while keeping the public result visibly incomplete.
	before := output.Len()
	sink.Abort()
	sink.Abort()
	if err := sink.Finish(); err == nil || query.PublicError(err).Code != "CANCELLED" {
		t.Fatalf("aborted stream finished: %v", err)
	}
	if err := sink.Write(records[1]); err == nil {
		t.Fatal("aborted stream accepted a batch")
	}
	if output.Len() != before || sink.EncodedBytes() != int64(before) || bytes.HasSuffix(output.Bytes(), resultEOS) {
		t.Fatal("abort emitted or counted cleanup bytes")
	}
}

func TestIPCSinkRejectsNilInputsAndKeepsFinishedState(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	for _, input := range []string{"schema", "batch"} {
		t.Run("nil "+input, func(t *testing.T) {
			limits := query.DefaultLimits()
			limits.ResultCompression = "lz4_frame"
			var output bytes.Buffer
			sink := NewIPCSink(&output, limits)
			defer sink.Abort()
			var err error
			if input == "schema" {
				err = sink.Schema(nil)
			} else {
				if err = sink.Schema(schema); err != nil {
					t.Fatal(err)
				}
				err = sink.Write(nil)
			}
			if err == nil || query.PublicError(err).Code != "INTERNAL" || sink.Finish() == nil {
				t.Fatalf("nil %s accepted: %v", input, err)
			}
			if output.Len() != 0 || sink.EncodedBytes() != 0 {
				t.Fatal("nil input emitted output")
			}
		})
	}
	var output bytes.Buffer
	sink := NewIPCSink(&output, query.DefaultLimits())
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(output.Bytes())
	sink.Abort()
	sink.Abort()
	if err := sink.Finish(); err != nil || !bytes.Equal(before, output.Bytes()) {
		t.Fatal("repeated finish or deferred abort changed a successful empty result")
	}
	if sink.Write(nil) == nil || sink.Schema(schema) == nil || !bytes.Equal(before, output.Bytes()) {
		t.Fatal("finished stream accepted additional output")
	}
}

func TestIPCSinkRejectsUnsupportedCompressionBeforeOutput(t *testing.T) {
	limits := query.DefaultLimits()
	limits.ResultCompression = "zstd"
	var output bytes.Buffer
	sink := NewIPCSink(&output, limits)
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err := sink.Schema(schema); err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" {
		t.Fatalf("unsupported codec accepted: %v", err)
	}
	if output.Len() != 0 || sink.EncodedBytes() != 0 {
		t.Fatal("invalid codec emitted output")
	}
}

func TestExecutorPublicCompressionKeepsChildIPCUncompressed(t *testing.T) {
	limits := query.DefaultLimits()
	limits.ResultCompression = "lz4_frame"
	executor, err := New(catalog.Config{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	sink := NewIPCSink(&output, limits)
	defer sink.Abort()
	stats, err := executor.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, sink)
	if err != nil {
		t.Fatalf("public codec reached the strict child IPC boundary: %v", err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	reader, err := ipc.NewReader(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() || stats.Rows != 3 || reader.RecordBatch().NumRows() != 3 {
		t.Fatal("subprocess result missing")
	}
	values := reader.RecordBatch().Column(0).(*array.Int64)
	if values.Value(0) != 1 || values.Value(2) != 3 || reader.Next() || reader.Err() != nil {
		t.Fatal("subprocess result changed")
	}
}
