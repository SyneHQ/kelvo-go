// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestOracleDriverNativeTypes(t *testing.T) {
	for _, test := range []struct {
		name string
		want arrow.DataType
	}{
		{"LONG", arrow.BinaryTypes.String},
		{"IBFloat", arrow.PrimitiveTypes.Float32},
		{"IBDouble", arrow.PrimitiveTypes.Float64},
		{"TimeStampDTY", &arrow.TimestampType{Unit: arrow.Nanosecond}},
		{"TimeStampTZ", &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
		{"TimeStampTZ_DTY", &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			schema, err := schemaFor(typesForTest(t, []metadataColumn{{name: test.name}}), Dialect{SourceType: "oracle"})
			if err != nil || !arrow.TypeEqual(schema.Field(0).Type, test.want) {
				t.Fatalf("Oracle native type mismatch: %v", err)
			}
		})
	}
	for _, source := range []string{"mysql", "postgresql", "sqlserver"} {
		if _, err := schemaFor(typesForTest(t, []metadataColumn{{name: "LONG"}}), Dialect{SourceType: source}); err == nil {
			t.Fatalf("Oracle-only LONG mapping leaked to %s", source)
		}
	}
	for _, name := range []string{"TimeStampLTZ_DTY", "TimeStampeLTZ", "NUMBER"} {
		if _, err := schemaFor(typesForTest(t, []metadataColumn{{name: name}}), Dialect{SourceType: "oracle"}); err == nil {
			t.Fatalf("unverified Oracle type %s accepted", name)
		}
	}
}

func TestOracleTimestampUTCInstantAndDST(t *testing.T) {
	for _, name := range []string{"TimeStampTZ", "TimeStampTZ_DTY"} {
		schema, err := schemaFor(typesForTest(t, []metadataColumn{{name: name}}), Dialect{SourceType: "oracle"})
		if err != nil {
			t.Fatal(err)
		}
		// The repeated DST wall time has two distinct instants; neither may be
		// rebuilt as a timezone-less wall clock before Arrow encoding.
		for _, encoded := range []string{"2026-10-06T12:34:56.123456789+05:30", "2026-11-01T01:30:00.123456789-04:00", "2026-11-01T01:30:00.123456789-05:00"} {
			value, err := time.Parse(time.RFC3339Nano, encoded)
			if err != nil {
				t.Fatal(err)
			}
			row, err := normalizeRow(schema, []any{value})
			if err != nil {
				t.Fatal(err)
			}
			got := row[0].(time.Time)
			if got.Location() != time.UTC || got.UnixNano() != value.UnixNano() {
				t.Fatal("Oracle timestamp instant changed")
			}
		}
	}
}
