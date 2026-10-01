package sqlnative

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

func TestNormalizeValue(t *testing.T) {
	decimalType := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	value, err := normalizeValue(decimalType, []byte("12.34"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := value.(decimal128.Num); !ok {
		t.Fatalf("decimal type = %T", value)
	}
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	value, err = normalizeValue(&arrow.TimestampType{Unit: arrow.Microsecond}, when)
	if err != nil || !value.(time.Time).Equal(when) {
		t.Fatalf("time normalization = %v, %v", value, err)
	}
	if _, err = normalizeValue(arrow.PrimitiveTypes.Int64, "not-an-int"); err == nil {
		t.Fatal("accepted invalid integer")
	}
	for _, tc := range []struct {
		typ   arrow.DataType
		input string
		want  any
	}{
		{arrow.PrimitiveTypes.Int8, "-8", int8(-8)},
		{arrow.PrimitiveTypes.Int16, "-16", int16(-16)},
		{arrow.PrimitiveTypes.Int32, "-32", int32(-32)},
		{arrow.PrimitiveTypes.Uint8, "8", uint8(8)},
		{arrow.PrimitiveTypes.Float32, "1.25", float32(1.25)},
	} {
		got, err := normalizeValue(tc.typ, tc.input)
		if err != nil || got != tc.want {
			t.Fatalf("normalize %s: got %#v, err %v", tc.typ, got, err)
		}
	}
}

func TestRejectLossyDecimalsAndPreserveNaiveWallTime(t *testing.T) {
	for _, value := range []any{"1.001", "1e999999999", struct{ Value int }{1}} {
		if _, err := normalizeValue(&arrow.Decimal128Type{Precision: 10, Scale: 2}, value); err == nil {
			t.Fatalf("accepted lossy decimal %v", value)
		}
	}
	zone := time.FixedZone("fixture", 19800)
	input := time.Date(2026, 10, 1, 12, 30, 0, 123456789, zone)
	result, err := normalizeValue(&arrow.TimestampType{Unit: arrow.Nanosecond}, input)
	if err != nil || result.(time.Time).Hour() != 12 || result.(time.Time).Nanosecond() != 123456789 {
		t.Fatalf("naive wall time changed: %v %v", result, err)
	}
}
