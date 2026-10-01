// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cloudapi

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

func TestExactCloudValues(t *testing.T) {
	columns := []Column{{Name: "id", Type: "BIGINT"}, {Name: "small", Type: "SMALLINT"}, {Name: "decimal", Type: "DECIMAL", Precision: 38, Scale: 9}, {Name: "null", Type: "STRING"}, {Name: "date", Type: "DATE"}, {Name: "timestamp", Type: "TIMESTAMP_NTZ"}, {Name: "binary", Type: "BINARY"}}
	schema, err := Schema(columns)
	if err != nil {
		t.Fatal(err)
	}
	row, err := Row(schema, []any{json.Number("9223372036854775807"), "-32768", "1234567890123456789.123456789", nil, "-1", "-0.000000001", "00ff"}, "snowflake")
	if err != nil {
		t.Fatal(err)
	}
	if row[0] != int64(9223372036854775807) || row[1] != int16(-32768) || row[2].(decimal128.Num).ToString(9) != "1234567890123456789.123456789" || row[3] != nil || row[4].(time.Time).Format("2006-01-02") != "1969-12-31" || row[5].(time.Time).UnixNano() != -1 || !reflect.DeepEqual(row[6], []byte{0, 255}) {
		t.Fatalf("lossy values: %v", row)
	}
	if schema.Field(1).Type.ID() != arrow.INT16 {
		t.Fatal("lost integer width")
	}
}
func TestCloudRejectsLossyTypes(t *testing.T) {
	for _, test := range []struct {
		column Column
		value  any
	}{{Column{Type: "TINYINT"}, "128"}, {Column{Type: "BIGINT"}, "9223372036854775808"}, {Column{Type: "DOUBLE"}, "NaN"}, {Column{Type: "STRING"}, json.Number("1")}, {Column{Type: "DECIMAL", Precision: 10, Scale: 2}, "1.001"}, {Column{Type: "TIMESTAMP_NTZ"}, "0.0000000001"}, {Column{Type: "DATE"}, "2147483648"}} {
		schema, err := Schema([]Column{test.column})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = Row(schema, []any{test.value}, "snowflake"); err == nil {
			t.Fatalf("accepted lossy %s %v", test.column.Type, test.value)
		}
	}
	for _, column := range []Column{{Type: "VARIANT"}, {Type: "TIMESTAMP_TZ"}, {Type: "DECIMAL"}} {
		if _, err := Schema([]Column{column}); err == nil {
			t.Fatalf("accepted unsupported %s", column.Type)
		}
	}
}
func TestDatabricksDateTimestampBinary(t *testing.T) {
	schema, err := Schema([]Column{{Type: "DATE"}, {Type: "TIMESTAMP"}, {Type: "BINARY"}})
	if err != nil {
		t.Fatal(err)
	}
	row, err := Row(schema, []any{"2026-10-01", "2026-10-01T12:30:00.123456789+05:30", "AP8="}, "databricks")
	if err != nil {
		t.Fatal(err)
	}
	if row[1].(time.Time).UTC().Format(time.RFC3339Nano) != "2026-10-01T07:00:00.123456789Z" || !reflect.DeepEqual(row[2], []byte{0, 255}) {
		t.Fatalf("lossy values: %v", row)
	}
}

func TestProviderFloatWidthsAndNaiveTimestamp(t *testing.T) {
	for _, provider := range []string{"databricks", "snowflake"} {
		schema, err := Schema([]Column{{Type: "FLOAT"}}, provider)
		if err != nil {
			t.Fatal(err)
		}
		want := arrow.FLOAT64
		if provider == "databricks" {
			want = arrow.FLOAT32
		}
		if schema.Field(0).Type.ID() != want {
			t.Fatalf("%s float width changed", provider)
		}
	}
	s, err := Schema([]Column{{Type: "TIMESTAMP_NTZ"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Row(s, []any{"2026-10-01T12:30:00+05:30"}, "databricks"); err == nil {
		t.Fatal("offset discarded on naive timestamp")
	}
	for _, value := range []string{"1e999999999", "1/2"} {
		if _, err := epoch(value); err == nil {
			t.Fatal("invalid epoch accepted")
		}
	}
}
