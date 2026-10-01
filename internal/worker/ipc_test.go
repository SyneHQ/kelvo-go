package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type workerTestSink struct {
	schema  *arrow.Schema
	rows    int64
	batches int
	write   func(arrow.RecordBatch) error
}

func (s *workerTestSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *workerTestSink) Write(batch arrow.RecordBatch) error {
	if s.write != nil {
		if err := s.write(batch); err != nil {
			return err
		}
	}
	s.rows += batch.NumRows()
	s.batches++
	return nil
}

func ipcFixture(t *testing.T) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	var out bytes.Buffer
	w := ipc.NewWriter(&out, ipc.WithSchema(schema))
	for batch := 0; batch < 3; batch++ {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		for i := 0; i < 256; i++ {
			if i == 7 {
				b.AppendNull()
			} else {
				b.Append(int64(batch*256 + i))
			}
		}
		values := b.NewArray()
		b.Release()
		r := array.NewRecordBatch(schema, []arrow.Array{values}, 256)
		values.Release()
		err := w.Write(r)
		r.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

type fragmentedIPC struct {
	io.Reader
	bytes int
}

func (r *fragmentedIPC) Read(p []byte) (int, error) {
	if len(p) > 13 {
		p = p[:13]
	}
	n, err := r.Reader.Read(p)
	r.bytes += n
	return n, err
}

func TestWorkerIPCValidatesAndDeliversBatchesWithoutPrefetch(t *testing.T) {
	data := ipcFixture(t)
	input := &fragmentedIPC{Reader: bytes.NewReader(data)}
	sink := &workerTestSink{}
	sink.write = func(batch arrow.RecordBatch) error {
		if input.bytes >= len(data) {
			t.Fatal("reader consumed the complete stream before synchronous delivery")
		}
		values := batch.Column(0).(*array.Int64)
		if !values.IsNull(7) || values.Value(8) != int64(sink.batches*256+8) {
			t.Fatal("batch values or nulls changed")
		}
		return nil
	}
	limits := query.DefaultLimits()
	limits.MaxBytes = int64(len(data))
	stats, err := readWorkerIPC(context.Background(), input, limits, sink)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 768 || stats.Batches != 3 || stats.WireBytes != int64(len(data)) {
		t.Fatalf("unexpected observed stats: %+v", stats)
	}
}

func TestWorkerIPCRejectsIncompleteTrailingAndOversizedInput(t *testing.T) {
	data := ipcFixture(t)
	for name, input := range map[string][]byte{
		"empty":              nil,
		"missing EOS":        data[:len(data)-8],
		"truncated batch":    data[:len(data)/2],
		"trailing bytes":     append(bytes.Clone(data), []byte("PRIVATE_DIAGNOSTIC")...),
		"oversized metadata": {255, 255, 255, 255, 255, 255, 255, 127},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := readWorkerIPC(context.Background(), bytes.NewReader(input), query.DefaultLimits(), &workerTestSink{})
			if err == nil {
				t.Fatal("invalid stream accepted")
			}
			if strings.Contains(err.Error(), "PRIVATE_DIAGNOSTIC") {
				t.Fatal("worker diagnostics leaked")
			}
		})
	}
	limits := query.DefaultLimits()
	limits.MaxBytes = int64(len(data) - 1)
	_, err := readWorkerIPC(context.Background(), bytes.NewReader(data), limits, &workerTestSink{})
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("wire limit not enforced: %v", err)
	}
	limits = query.DefaultLimits()
	limits.MaxRows = 100
	_, err = readWorkerIPC(context.Background(), bytes.NewReader(data), limits, &workerTestSink{})
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("row limit not enforced: %v", err)
	}
}

func TestWorkerIPCRejectsAdvertisedBodyBeforeAllocation(t *testing.T) {
	data := bytes.Clone(ipcFixture(t))
	schemaLength := int(binary.LittleEndian.Uint32(data[4:]))
	second := 8 + schemaLength
	metaLength := int(binary.LittleEndian.Uint32(data[second+4:]))
	meta := data[second+8 : second+8+metaLength]
	// Mutate the known-good fixture's Message.bodyLength without exporting
	// the production verifier's private FlatBuffer traversal helpers.
	message := int(binary.LittleEndian.Uint32(meta))
	vtable := message - int(int32(binary.LittleEndian.Uint32(meta[message:])))
	bodyPos := message + int(binary.LittleEndian.Uint16(meta[vtable+4+3*2:]))
	if bodyPos == 0 {
		t.Fatal("fixture body length missing")
	}
	binary.LittleEndian.PutUint64(meta[bodyPos:], 1<<40)
	_, err := readWorkerIPC(context.Background(), bytes.NewReader(data), query.DefaultLimits(), &workerTestSink{})
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("huge advertised body accepted: %v", err)
	}
}

func TestWorkerIPCDictionaryChanges(t *testing.T) {
	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	schema := arrow.NewSchema([]arrow.Field{{Name: "category", Type: dt}}, nil)
	var out bytes.Buffer
	w := ipc.NewWriter(&out, ipc.WithSchema(schema), ipc.WithDictionaryDeltas(true))
	for _, labels := range [][]string{{"one", "two"}, {"one", "two"}, {"three", "four"}} {
		ib := array.NewInt8Builder(memory.DefaultAllocator)
		ib.AppendValues([]int8{0, 1}, nil)
		indices := ib.NewArray()
		ib.Release()
		lb := array.NewStringBuilder(memory.DefaultAllocator)
		lb.AppendValues(labels, nil)
		dictionary := lb.NewArray()
		lb.Release()
		column := array.NewDictionaryArray(dt, indices, dictionary)
		indices.Release()
		dictionary.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{column}, 2)
		column.Release()
		err := w.Write(record)
		record.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	sink := &workerTestSink{}
	sink.write = func(record arrow.RecordBatch) error {
		column := record.Column(0).(*array.Dictionary)
		expected := "one"
		if sink.batches == 2 {
			expected = "three"
		}
		if column.Dictionary().(*array.String).Value(column.GetValueIndex(0)) != expected {
			t.Fatal("dictionary values changed")
		}
		return nil
	}
	stats, err := readWorkerIPC(context.Background(), bytes.NewReader(out.Bytes()), query.DefaultLimits(), sink)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 6 || stats.Batches != 3 {
		t.Fatalf("dictionary batches: %+v", stats)
	}
}

func TestWorkerIPCAllocatorTracksLiveAllocations(t *testing.T) {
	a := &ipcAllocator{base: memory.NewGoAllocator(), limit: 1024}
	b := a.Allocate(768)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("over-budget live allocation accepted")
			}
		}()
		a.Allocate(512)
	}()
	if !a.exceeded.Load() {
		t.Fatal("allocator did not record limit failure")
	}
	a.Free(b)
	if a.used != 0 {
		t.Fatalf("allocation leaked: %d", a.used)
	}
}

func TestWorkerIPCComplexTypesPreserveValues(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 19, Scale: 4}},
		{Name: "occurred_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "identifier", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "optional", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "items", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{Name: "details", Type: arrow.StructOf(arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int32})},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Decimal128Builder).Append(decimal128.FromI64(12345))
	b.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(1700000000123456))
	b.Field(2).(*array.Uint64Builder).Append(math.MaxUint64)
	b.Field(3).(*array.StringBuilder).AppendNull()
	list := b.Field(4).(*array.ListBuilder)
	list.Append(true)
	list.ValueBuilder().(*array.Int64Builder).AppendValues([]int64{3, 4}, nil)
	structure := b.Field(5).(*array.StructBuilder)
	structure.Append(true)
	structure.FieldBuilder(0).(*array.Int32Builder).Append(7)
	record := b.NewRecordBatch()
	defer record.Release()
	var out bytes.Buffer
	w := ipc.NewWriter(&out, ipc.WithSchema(schema))
	if err := w.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	sink := &workerTestSink{write: func(got arrow.RecordBatch) error {
		if got.Column(0).(*array.Decimal128).Value(0).ToString(4) != "1.2345" || got.Column(1).(*array.Timestamp).Value(0) != arrow.Timestamp(1700000000123456) || got.Column(2).(*array.Uint64).Value(0) != math.MaxUint64 || !got.Column(3).IsNull(0) {
			t.Fatal("precision, timestamp, integer width or NULL changed")
		}
		if got.Column(4).(*array.List).ListValues().(*array.Int64).Value(1) != 4 || got.Column(5).(*array.Struct).Field(0).(*array.Int32).Value(0) != 7 {
			t.Fatal("nested values changed")
		}
		return nil
	}}
	stats, err := readWorkerIPC(context.Background(), bytes.NewReader(out.Bytes()), query.DefaultLimits(), sink)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 1 || !schema.Equal(sink.schema) {
		t.Fatal("complex schema or row count changed")
	}
}
