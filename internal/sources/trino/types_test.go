// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package trino

import (
	"encoding/json"
	"testing"
)

func TestUnsupportedSchemaAndInexactValuesFail(t *testing.T) {
	for _, typ := range []string{"array(bigint)", "map(varchar,integer)", "row(x integer)", "timestamp(6) with time zone", "time(3)", "timestamp(12)", "decimal(39,2)", "decimal(2,3)", "json", "uuid", "variant"} {
		if _, err := resultSchema([]column{{"value", typ}}); err == nil {
			t.Fatalf("accepted type %s", typ)
		}
	}
	for _, test := range []struct {
		typ   string
		value any
	}{
		{"tinyint", json.Number("128")}, {"smallint", json.Number("32768")}, {"integer", json.Number("2147483648")}, {"bigint", json.Number("9223372036854775808")},
		{"decimal(5,2)", "1.234"}, {"decimal(5,2)", "1234.56"}, {"date", "2026-02-30"},
		{"timestamp(3)", "2026-10-01 12:00:00.000001"}, {"timestamp(9)", "2026-10-01 12:00:00.1234567891"}, {"timestamp(6)", "2026-10-01 12:00:00.000001Z"},
		{"boolean", "true"}, {"bigint", "1"}, {"varchar", json.Number("1")}, {"unknown", json.Number("1")}, {"varbinary", "%%%"}, {"real", json.Number("1e100")},
	} {
		schema, err := resultSchema([]column{{"value", test.typ}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := resultRow(schema, []any{test.value}); err == nil {
			t.Fatalf("accepted inexact %s value", test.typ)
		}
	}
}
