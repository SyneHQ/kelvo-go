// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package athena

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

func TestExactScalars(t *testing.T) {
	tests := []struct {
		column athenaColumn
		value  string
		want   any
	}{
		{athenaColumn{Type: "tinyint"}, "-128", int8(-128)},
		{athenaColumn{Type: "smallint"}, "32767", int16(32767)},
		{athenaColumn{Type: "integer"}, "-2147483648", int32(-2147483648)},
		{athenaColumn{Type: "bigint"}, "-9223372036854775808", int64(-9223372036854775808)},
		{athenaColumn{Type: "real"}, "0.125", float32(0.125)},
		{athenaColumn{Type: "double"}, "0.125", float64(0.125)},
		{athenaColumn{Type: "boolean"}, "false", false},
		{athenaColumn{Type: "varchar(12)"}, "null", "null"},
	}
	for _, test := range tests {
		schema, err := makeSchema([]athenaColumn{test.column})
		if err != nil {
			t.Fatal(err)
		}
		got, err := convert(schema.Field(0).Type, test.value)
		if err != nil || got != test.want {
			t.Fatalf("%s got=%v want=%v err=%v", test.column.Type, got, test.want, err)
		}
	}
	schema, err := makeSchema([]athenaColumn{{Type: "decimal", Precision: 38, Scale: 9}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := convert(schema.Field(0).Type, "-12345678901234567890123456789.123456789")
	if err != nil || got.(decimal128.Num).ToString(9) != "-12345678901234567890123456789.123456789" {
		t.Fatalf("decimal=%v err=%v", got, err)
	}
}
func TestPrecisionFailures(t *testing.T) {
	for _, test := range []struct{ kind, value string }{
		{"tinyint", "128"}, {"smallint", "32768"}, {"int", "2147483648"}, {"bigint", "9223372036854775808"},
		{"real", "NaN"}, {"double", "Infinity"}, {"boolean", "1"}, {"decimal(8,2)", "123.456"}, {"decimal(8,2)", "1234567.00"},
		{"timestamp", "2026-10-01 00:00:00.000001"}, {"timestamp(9)", "2026-10-01 00:00:00.1234567891"}, {"timestamp(9)", "1600-01-01 00:00:00"}, {"timestamp", "2026-10-01T00:00:00Z"}, {"date", "2026-02-30"}, {"varbinary", "0g"},
	} {
		t.Run(test.kind+test.value, func(t *testing.T) {
			schema, err := makeSchema([]athenaColumn{{Type: test.kind}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = convert(schema.Field(0).Type, test.value); err == nil {
				t.Fatal("lossy or malformed value accepted")
			}
		})
	}
	for _, column := range []athenaColumn{{Type: "decimal(39,0)"}, {Type: "decimal(3,4)"}, {Type: "decimal", Precision: 3, Scale: -1}, {Type: "timestamp(10)"}, {Type: "array(bigint)"}, {Type: "decimal(5,2)", Precision: 6, Scale: 2}} {
		if _, err := makeSchema([]athenaColumn{column}); err == nil {
			t.Fatalf("accepted unsupported metadata: %+v", column)
		}
	}
}
func TestTimestampsPreservePrecisionAndZoneSemantics(t *testing.T) {
	for _, test := range []struct {
		kind, value string
		unit        arrow.TimeUnit
		tz          string
	}{
		{"timestamp", "1600-01-01 00:00:00.123", arrow.Millisecond, ""},
		{"timestamp(6)", "2026-10-01 00:00:00.123456", arrow.Microsecond, ""},
		{"timestamp(9)", "2026-10-01 00:00:00.123456789", arrow.Nanosecond, ""},
		{"timestamp with time zone", "2026-10-01 05:30:00.123 +05:30", arrow.Millisecond, "UTC"},
		{"timestamp(6) with time zone", "2026-10-01 00:00:00.123456 UTC", arrow.Microsecond, "UTC"},
	} {
		schema, err := makeSchema([]athenaColumn{{Type: test.kind}})
		if err != nil {
			t.Fatal(err)
		}
		dt := schema.Field(0).Type.(*arrow.TimestampType)
		if dt.Unit != test.unit || dt.TimeZone != test.tz {
			t.Fatal("wrong timestamp metadata")
		}
		got, err := convert(dt, test.value)
		if err != nil {
			t.Fatal(err)
		}
		if test.tz != "" && got.(time.Time).UTC().Hour() != 0 {
			t.Fatal("timezone offset lost")
		}
	}
}
