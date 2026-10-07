// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rowarrow

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
)

func TestWriterDefaultBatchDeliveryUnchanged(t *testing.T) {
	sink := &captureSink{}
	defer sink.close()
	l := limits()
	l.MaxRows = 3000
	w, err := NewWriter(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil), l, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := range 2050 {
		if err := w.Write([]any{int64(i)}); err != nil {
			t.Fatal(err)
		}
		if len(sink.records) != (i+1)/1024 {
			t.Fatalf("legacy first-batch delivery changed at row %d", i)
		}
	}
	stats, err := w.Finish()
	if err != nil || stats.Rows != 2050 || stats.Batches != 3 {
		t.Fatalf("finish: %+v, %v", stats, err)
	}
	assertBatchRows(t, sink.records, []int64{1024, 1024, 2})
}

func TestWriterByteTargetBatchBoundaries(t *testing.T) {
	for _, test := range []struct {
		name        string
		target      int64
		value       string
		rows        int
		maxBytes    int64
		wantBatches []int64
	}{
		{name: "exact target", target: 1024, value: strings.Repeat("x", 251), rows: 9, maxBytes: 1 << 20, wantBatches: []int64{4, 4, 1}},
		{name: "flush before crossing target", target: 1024, value: strings.Repeat("x", 300), rows: 7, maxBytes: 1 << 20, wantBatches: []int64{3, 3, 1}},
		{name: "row larger than target", target: 1024, value: strings.Repeat("x", 2048), rows: 2, maxBytes: 1 << 20, wantBatches: []int64{1, 1}},
		{name: "larger than legacy batch", target: 64 << 10, value: "x", rows: 10000, maxBytes: 1 << 20, wantBatches: []int64{10000}},
		{name: "remaining result budget", target: 1024, value: strings.Repeat("x", 251), rows: 6, maxBytes: 1536, wantBatches: []int64{4, 2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sink := &captureSink{}
			defer sink.close()
			l := limits()
			l.MaxRows, l.MaxBytes, l.RowBatchTargetBytes = 10000, test.maxBytes, test.target
			w, err := NewWriter(arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}}, nil), l, sink)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			for range test.rows {
				if err := w.Write([]any{test.value}); err != nil {
					t.Fatal(err)
				}
			}
			if test.name == "remaining result budget" && len(sink.records) != 2 {
				t.Fatal("did not flush at the remaining result budget")
			}
			stats, err := w.Finish()
			if err != nil || stats.Rows != int64(test.rows) || stats.Bytes > l.MaxBytes {
				t.Fatalf("finish: %+v, %v", stats, err)
			}
			assertBatchRows(t, sink.records, test.wantBatches)
		})
	}
}

func TestWriterByteTargetKeepsMemoryAndResultBounds(t *testing.T) {
	t.Run("batch memory and oversized row", func(t *testing.T) {
		sink := &captureSink{}
		defer sink.close()
		l := limits()
		l.RowBatchTargetBytes, l.MaxBytes = 64<<20, 16<<20
		w, err := NewWriter(arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}}, nil), l, sink)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		for range 2 {
			if err := w.Write([]any{strings.Repeat("x", 3<<20)}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Write([]any{strings.Repeat("x", 5<<20)}); err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
			t.Fatalf("accepted row above memory budget: %v", err)
		}
		stats, err := w.Finish()
		if err != nil || stats.Rows != 2 {
			t.Fatalf("finish: %+v, %v", stats, err)
		}
		assertBatchRows(t, sink.records, []int64{1, 1})
	})
	t.Run("null columns keep row cap", func(t *testing.T) {
		sink := &captureSink{}
		defer sink.close()
		l := limits()
		l.RowBatchTargetBytes, l.MaxRows = 64<<20, 65537
		w, err := NewWriter(arrow.NewSchema([]arrow.Field{{Name: "empty", Type: arrow.Null, Nullable: true}}, nil), l, sink)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		for range 65537 {
			if err := w.Write([]any{nil}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		assertBatchRows(t, sink.records, []int64{65536, 1})
	})
	for _, test := range []struct {
		name  string
		rows  int64
		bytes int64
	}{
		{"row limit", 1, 1 << 20},
		{"decoded byte limit", 10, 1024},
	} {
		t.Run(test.name, func(t *testing.T) {
			sink := &captureSink{}
			defer sink.close()
			l := limits()
			l.MaxRows, l.MaxBytes, l.RowBatchTargetBytes = test.rows, test.bytes, 1<<20
			w, err := NewWriter(arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}}, nil), l, sink)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if err := w.Write([]any{strings.Repeat("x", 600)}); err != nil {
				t.Fatal(err)
			}
			if err := w.Write([]any{strings.Repeat("x", 600)}); err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
				t.Fatalf("batch target relaxed resource limit: %v", err)
			}
			stats, err := w.Finish()
			if err != nil || stats.Rows != 1 {
				t.Fatalf("rejected row was partially appended: %+v, %v", stats, err)
			}
		})
	}
}

func TestWriterByteTargetPreservesTypedNullableValues(t *testing.T) {
	decimalType := &arrow.Decimal128Type{Precision: 38, Scale: 8}
	largeDecimalType := &arrow.Decimal256Type{Precision: 76, Scale: 10}
	stampType := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "signed", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "unsigned", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "binary", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "decimal", Type: decimalType, Nullable: true},
		{Name: "large_decimal", Type: largeDecimalType, Nullable: true},
		{Name: "instant", Type: stampType, Nullable: true},
	}, nil)
	d, err := decimal128.FromString("12345678901234567890.12345678", 38, 8)
	if err != nil {
		t.Fatal(err)
	}
	large, err := decimal256.FromString("1234567890123456789012345678901234567890.1234567890", 76, 10)
	if err != nil {
		t.Fatal(err)
	}
	instant := time.Date(2026, 10, 7, 3, 2, 1, 123456789, time.UTC)
	stamp, err := arrow.TimestampFromTime(instant, arrow.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	sink := &captureSink{}
	defer sink.close()
	l := limits()
	l.RowBatchTargetBytes = 1024
	w, err := NewWriter(schema, l, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := range 23 {
		row := make([]any, 7)
		if i%3 != 0 {
			row = []any{int64(math.MinInt64), uint64(math.MaxUint64), strings.Repeat("界", 100+i), bytes.Repeat([]byte{0xff, 0x00}, 20+i), d, large, instant}
		}
		if err := w.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := w.Finish()
	if err != nil || stats.Rows != 23 || stats.Batches < 2 {
		t.Fatalf("finish: %+v, %v", stats, err)
	}
	index := 0
	for _, record := range sink.records {
		if !record.Schema().Equal(schema) {
			t.Fatal("schema fidelity changed")
		}
		for j := 0; j < int(record.NumRows()); j++ {
			for _, column := range record.Columns() {
				if column.IsNull(j) != (index%3 == 0) {
					t.Fatalf("null fidelity changed at row %d", index)
				}
			}
			if index%3 != 0 {
				if record.Column(0).(*array.Int64).Value(j) != math.MinInt64 || record.Column(1).(*array.Uint64).Value(j) != math.MaxUint64 {
					t.Fatalf("integer fidelity changed at row %d", index)
				}
				if record.Column(2).(*array.String).Value(j) != strings.Repeat("界", 100+index) || !bytes.Equal(record.Column(3).(*array.Binary).Value(j), bytes.Repeat([]byte{0xff, 0x00}, 20+index)) {
					t.Fatalf("variable-width fidelity changed at row %d", index)
				}
				if record.Column(4).(*array.Decimal128).Value(j) != d || record.Column(5).(*array.Decimal256).Value(j) != large || record.Column(6).(*array.Timestamp).Value(j) != stamp {
					t.Fatalf("decimal/timestamp fidelity changed at row %d", index)
				}
			}
			index++
		}
	}
	if index != 23 {
		t.Fatalf("delivered %d rows", index)
	}
}

type failingBatchSink struct {
	err    error
	writes int
}

func (*failingBatchSink) Schema(*arrow.Schema) error { return nil }
func (s *failingBatchSink) Write(arrow.RecordBatch) error {
	s.writes++
	return s.err
}

func TestWriterSinkFailureIsTerminal(t *testing.T) {
	for _, test := range []struct {
		name   string
		target int64
		value  string
		writes int
	}{
		{"legacy flush", 0, "x", 1024},
		{"target flush", 1024, strings.Repeat("x", 1019), 1},
		{"flush before next row", 1024, strings.Repeat("x", 600), 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := errors.New("sink unavailable")
			sink := &failingBatchSink{err: failure}
			l := limits()
			l.RowBatchTargetBytes = test.target
			w, err := NewWriter(arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}}, nil), l, sink)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			for i := range test.writes {
				err := w.Write([]any{test.value})
				if (i == test.writes-1 && !errors.Is(err, failure)) || (i < test.writes-1 && err != nil) {
					t.Fatalf("write %d: %v", i, err)
				}
			}
			if err := w.Write([]any{test.value}); !errors.Is(err, failure) {
				t.Fatalf("write lost original failure: %v", err)
			}
			for range 2 {
				if _, err := w.Finish(); !errors.Is(err, failure) {
					t.Fatalf("finish lost original failure: %v", err)
				}
			}
			if sink.writes != 1 {
				t.Fatalf("retried failed batch %d times", sink.writes)
			}
		})
	}
}

type blockedBatchSink struct {
	ctx     context.Context
	entered chan struct{}
}

func (*blockedBatchSink) Schema(*arrow.Schema) error { return nil }
func (s *blockedBatchSink) Write(arrow.RecordBatch) error {
	close(s.entered)
	<-s.ctx.Done()
	return s.ctx.Err()
}

func TestWriterByteTargetBackpressureAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &blockedBatchSink{ctx: ctx, entered: make(chan struct{})}
	l := limits()
	l.RowBatchTargetBytes = 1024
	w, err := NewWriter(arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}}, nil), l, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	done := make(chan error, 1)
	go func() { done <- w.Write([]any{strings.Repeat("x", 1019)}) }()
	select {
	case <-sink.entered:
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("sink was not reached")
	}
	select {
	case err := <-done:
		t.Fatalf("write bypassed sink backpressure: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lost sink cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write did not return after sink cancellation")
	}
	if _, err := w.Finish(); !errors.Is(err, context.Canceled) {
		t.Fatalf("finish masked cancellation: %v", err)
	}
}

func assertBatchRows(t *testing.T, records []arrow.RecordBatch, want []int64) {
	t.Helper()
	var got []int64
	for _, record := range records {
		got = append(got, record.NumRows())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("batch rows = %v, want %v", got, want)
	}
}
