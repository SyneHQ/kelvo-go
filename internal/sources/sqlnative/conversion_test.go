// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestTypedIntegersKeepWidthsAndOverflowChecks(t *testing.T) {
	types := []arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64}
	values := []any{int8(math.MinInt8), int8(math.MaxInt8), int16(math.MinInt16), int16(math.MaxInt16),
		int32(math.MinInt32), int32(math.MaxInt32), int64(math.MinInt64), int64(math.MaxInt64),
		uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
		int64(-1), uint64(0), int64(128), int64(-129), int64(32768), int64(-32769),
		int64(math.MaxInt32) + 1, int64(math.MinInt32) - 1, uint64(math.MaxInt64) + 1,
		int64(256), int64(65536), uint64(math.MaxUint32) + 1, int64(42), uint64(42)}
	for _, typ := range types {
		convert := scalarConverter(typ)
		for _, input := range values {
			want, wantErr := normalizeTextValue(typ, input)
			got, err := convert(input)
			if (err != nil) != (wantErr != nil) || err == nil && !reflect.DeepEqual(got, want) {
				t.Fatalf("%s input=%T(%v): got %T(%v), error=%v, want %T(%v), error=%v", typ, input, input, got, got, err, want, want, wantErr)
			}
		}
	}
}

func TestScalarConversionPreservesTextAndDecimalSemantics(t *testing.T) {
	for _, test := range []struct {
		typ   arrow.DataType
		value any
	}{
		{arrow.PrimitiveTypes.Int32, []byte("2147483647")},
		{arrow.PrimitiveTypes.Int32, []byte("2147483648")},
		{arrow.PrimitiveTypes.Uint64, "18446744073709551615"},
		{arrow.PrimitiveTypes.Uint64, "-1"},
		{arrow.PrimitiveTypes.Int64, float64(1)},
		{arrow.PrimitiveTypes.Int64, float64(1.5)},
		{arrow.PrimitiveTypes.Int64, true},
		{arrow.PrimitiveTypes.Float32, "1.125"},
		{arrow.PrimitiveTypes.Float64, float32(1.2)},
		{arrow.FixedWidthTypes.Boolean, []byte("1")},
		{arrow.FixedWidthTypes.Boolean, "false"},
		{arrow.BinaryTypes.String, []byte("database text")},
		{arrow.BinaryTypes.String, int64(7)},
		{arrow.BinaryTypes.Binary, []byte{0, 255, 1}},
		{&arrow.Decimal128Type{Precision: 10, Scale: 2}, []byte("123.45")},
		{&arrow.Decimal128Type{Precision: 10, Scale: 2}, "1.001"},
		{&arrow.Decimal128Type{Precision: 10, Scale: 2}, float64(1.25)},
		{&arrow.Decimal128Type{Precision: 10, Scale: 2}, float32(1.25)},
		{&arrow.Decimal256Type{Precision: 76, Scale: 4}, "1234567890123456789012345678901234567890.1234"},
		{&arrow.Decimal256Type{Precision: 76, Scale: -2}, "12300"},
		{&arrow.Decimal256Type{Precision: 76, Scale: -2}, "12301"},
	} {
		want, wantErr := normalizeTextValue(test.typ, test.value)
		got, err := scalarConverter(test.typ)(test.value)
		if (err != nil) != (wantErr != nil) || err == nil && !reflect.DeepEqual(got, want) {
			t.Fatalf("conversion changed for %s with %T", test.typ, test.value)
		}
	}
}

func TestTypedFloatConversionPreservesBits(t *testing.T) {
	for _, value := range []float64{math.Copysign(0, -1), 1.25, 1.2, math.SmallestNonzeroFloat64,
		float64(math.SmallestNonzeroFloat32), float64(math.MaxFloat32), math.MaxFloat64, math.Inf(1), math.NaN()} {
		for _, typ := range []arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64} {
			want, wantErr := normalizeTextValue(typ, value)
			got, err := scalarConverter(typ)(value)
			if (err != nil) != (wantErr != nil) {
				t.Fatalf("float error changed for %s", typ)
			}
			if err != nil {
				continue
			}
			if typ.ID() == arrow.FLOAT32 {
				a, b := got.(float32), want.(float32)
				if math.Float32bits(a) != math.Float32bits(b) && !(math.IsNaN(float64(a)) && math.IsNaN(float64(b))) {
					t.Fatalf("float32 bits changed for %v", value)
				}
			} else {
				a, b := got.(float64), want.(float64)
				if math.Float64bits(a) != math.Float64bits(b) && !(math.IsNaN(a) && math.IsNaN(b)) {
					t.Fatalf("float64 bits changed for %v", value)
				}
			}
		}
	}
}

func TestReusedConversionRowsPreserveArrowOwnershipAndNulls(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "bytes", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "number", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	conversion := newRowConversion(schema)
	sink := &execSink{}
	defer sink.close()
	limits := query.Limits{MaxRows: 3, MaxBytes: 1 << 20, Timeout: time.Second, MemoryMB: 16, Threads: 1, MaxTempMB: 1}
	writer, err := rowarrow.NewWriter(schema, limits, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	buffer := []byte("first")
	for _, values := range [][]any{{buffer, buffer, int64(1)}, {nil, nil, nil}, {buffer, buffer, int64(3)}} {
		if values[2] == int64(3) {
			copy(buffer, "third")
		}
		row, err := conversion.convert(values)
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Write(row); err != nil {
			t.Fatal(err)
		}
		copy(buffer, "other")
	}
	if _, err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
	batch := sink.records[0]
	bytes, text, number := batch.Column(0).(*array.Binary), batch.Column(1).(*array.String), batch.Column(2).(*array.Int64)
	if string(bytes.Value(0)) != "first" || text.Value(0) != "first" || number.Value(0) != 1 ||
		!bytes.IsNull(1) || !text.IsNull(1) || !number.IsNull(1) ||
		string(bytes.Value(2)) != "third" || text.Value(2) != "third" || number.Value(2) != 3 {
		t.Fatal("row reuse changed retained Arrow values or nulls")
	}
}
