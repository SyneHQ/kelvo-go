package rowarrow

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

type captureSink struct {
	schema  *arrow.Schema
	records []arrow.RecordBatch
}

func (s *captureSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *captureSink) Write(record arrow.RecordBatch) error {
	record.Retain()
	s.records = append(s.records, record)
	return nil
}
func (s *captureSink) close() {
	for _, r := range s.records {
		r.Release()
	}
}
func limits() query.Limits {
	return query.Limits{MaxRows: 2048, MaxBytes: 1 << 20, Timeout: time.Second, MemoryMB: 16, Threads: 1, MaxTempMB: 1}
}

func TestWriterTemporalUnitsAndDateFloor(t *testing.T) {
	sink := &captureSink{}
	defer sink.close()
	schema := arrow.NewSchema([]arrow.Field{{Name: "day", Type: arrow.FixedWidthTypes.Date32, Nullable: true}, {Name: "instant", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true}}, nil)
	w, err := NewWriter(schema, limits(), sink)
	if err != nil {
		t.Fatal(err)
	}
	instant := time.Date(1969, 12, 31, 23, 59, 59, 123456789, time.UTC)
	if err = w.Write([]any{instant, instant}); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Finish(); err != nil {
		t.Fatal(err)
	}
	day := sink.records[0].Column(0).(*array.Date32).Value(0)
	if day != -1 {
		t.Fatalf("date = %d, want -1", day)
	}
	got := sink.records[0].Column(1).(*array.Timestamp).Value(0)
	want, err := arrow.TimestampFromTime(instant, arrow.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("timestamp = %d, want %d", got, want)
	}
}
func TestWriterRejectsNullNonNullableAndNonFinite(t *testing.T) {
	sink := &captureSink{}
	defer sink.close()
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Float64, Nullable: false}}, nil)
	w, err := NewWriter(schema, limits(), sink)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Write([]any{nil}); err == nil {
		t.Fatal("accepted null non-nullable value")
	}
	if err = w.Write([]any{math.Inf(1)}); err == nil {
		t.Fatal("accepted infinite float")
	}
	if _, err = w.Finish(); err != nil {
		t.Fatal(err)
	}
	if len(sink.records) != 0 {
		t.Fatal("invalid rows partially appended")
	}
}
func TestWriterLifecycleAndLimits(t *testing.T) {
	sink := &captureSink{}
	defer sink.close()
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	l := limits()
	l.MaxBytes = 1024
	w, err := NewWriter(schema, l, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Write([]any{string(make([]byte, 1025))}); err == nil {
		t.Fatal("accepted oversized value")
	}
	first, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.Finish()
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatal("Finish is not idempotent")
	}
	if err = w.Write([]any{"x"}); err == nil {
		t.Fatal("accepted row after Finish")
	}
}

func TestWriterPreservesNarrowWidthsAndNegativeDecimalScale(t *testing.T) {
	sink := &captureSink{}
	defer sink.close()
	decimalType := &arrow.Decimal128Type{Precision: 8, Scale: -2}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: false},
		{Name: "i16", Type: arrow.PrimitiveTypes.Int16, Nullable: false},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "rounded", Type: decimalType, Nullable: false},
	}, nil)
	w, err := NewWriter(schema, limits(), sink)
	if err != nil {
		t.Fatal(err)
	}
	rounded, err := decimal128.FromString("1200", 8, -2)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write([]any{int8(-8), int16(-16), int32(-32), uint8(8), float32(1.25), rounded}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	record := sink.records[0]
	if got := record.Column(0).(*array.Int8).Value(0); got != -8 {
		t.Fatalf("int8 = %d", got)
	}
	if got := record.Column(1).(*array.Int16).Value(0); got != -16 {
		t.Fatalf("int16 = %d", got)
	}
	if got := record.Column(2).(*array.Int32).Value(0); got != -32 {
		t.Fatalf("int32 = %d", got)
	}
	if got := record.Column(3).(*array.Uint8).Value(0); got != 8 {
		t.Fatalf("uint8 = %d", got)
	}
	if got := record.Column(4).(*array.Float32).Value(0); got != 1.25 {
		t.Fatalf("float32 = %v", got)
	}
}

func TestWriterFlushesAtBatchMemoryLimit(t *testing.T) {
	sink := &captureSink{}
	defer sink.close()
	l := limits()
	l.MaxBytes = 16 << 20
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String, Nullable: false}}, nil)
	w, err := NewWriter(schema, l, sink)
	if err != nil {
		t.Fatal(err)
	}
	value := string(make([]byte, 3<<20))
	if err = w.Write([]any{value}); err != nil {
		t.Fatal(err)
	}
	if err = w.Write([]any{value}); err != nil {
		t.Fatal(err)
	}
	stats, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Batches != 2 {
		t.Fatalf("batches=%d", stats.Batches)
	}
	w.Close()
	w, err = NewWriter(schema, l, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err = w.Write([]any{string(make([]byte, 5<<20))}); err == nil {
		t.Fatal("accepted row over batch budget")
	}
}

func TestWriterRejectsTimestampRangeAndPrecisionLoss(t *testing.T) {
	for _, test := range []struct {
		unit  arrow.TimeUnit
		value time.Time
	}{{arrow.Nanosecond, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)}, {arrow.Microsecond, time.Unix(0, 1)}} {
		sink := &captureSink{}
		schema := arrow.NewSchema([]arrow.Field{{Name: "t", Type: &arrow.TimestampType{Unit: test.unit}, Nullable: true}}, nil)
		w, err := NewWriter(schema, limits(), sink)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.Write([]any{test.value}); err == nil {
			t.Error("timestamp precision/range loss accepted")
		}
		w.Close()
		sink.close()
	}
}
