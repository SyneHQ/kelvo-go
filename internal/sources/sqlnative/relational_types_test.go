// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

type metadataConnector struct{ columns []metadataColumn }
type metadataColumn struct {
	name    string
	p, s    int64
	decimal bool
}

func (c metadataConnector) Connect(context.Context) (driver.Conn, error) {
	return metadataConn{c.columns}, nil
}
func (c metadataConnector) Driver() driver.Driver { return metadataDriver{} }

type metadataDriver struct{}

func (metadataDriver) Open(string) (driver.Conn, error) { panic("unused") }

type metadataConn struct{ columns []metadataColumn }

func (c metadataConn) Prepare(string) (driver.Stmt, error) { panic("unused") }
func (c metadataConn) Close() error                        { return nil }
func (c metadataConn) Begin() (driver.Tx, error)           { panic("unused") }
func (c metadataConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return metadataRows{c.columns}, nil
}

type metadataRows struct{ columns []metadataColumn }

func (r metadataRows) Columns() []string {
	names := make([]string, len(r.columns))
	for i := range names {
		names[i] = "value"
	}
	return names
}
func (metadataRows) Close() error                              { return nil }
func (metadataRows) Next([]driver.Value) error                 { return io.EOF }
func (r metadataRows) ColumnTypeDatabaseTypeName(i int) string { return r.columns[i].name }
func (r metadataRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	c := r.columns[i]
	return c.p, c.s, c.decimal
}
func typesForTest(t *testing.T, columns []metadataColumn) []*sql.ColumnType {
	t.Helper()
	db := sql.OpenDB(metadataConnector{columns})
	defer db.Close()
	rows, err := db.Query("unused")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	return types
}
func TestRelationalMetadataWidthsZonesAndNativeTypes(t *testing.T) {
	for _, test := range []struct {
		source, name string
		want         arrow.DataType
	}{
		{"mysql", "TINYINT", arrow.PrimitiveTypes.Int8}, {"mysql", "UNSIGNED TINYINT", arrow.PrimitiveTypes.Uint8}, {"mariadb", "SMALLINT", arrow.PrimitiveTypes.Int16}, {"mysql", "UNSIGNED SMALLINT", arrow.PrimitiveTypes.Uint16}, {"mysql", "MEDIUMINT", arrow.PrimitiveTypes.Int32}, {"mysql", "UNSIGNED MEDIUMINT", arrow.PrimitiveTypes.Uint32}, {"mysql", "INT", arrow.PrimitiveTypes.Int32}, {"mysql", "UNSIGNED INT", arrow.PrimitiveTypes.Uint32}, {"mysql", "BIGINT", arrow.PrimitiveTypes.Int64}, {"mysql", "UNSIGNED BIGINT", arrow.PrimitiveTypes.Uint64},
		{"mysql", "BIT", arrow.BinaryTypes.Binary}, {"mysql", "FLOAT", arrow.PrimitiveTypes.Float32}, {"mysql", "TIMESTAMP", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}}, {"mariadb", "DATETIME", &arrow.TimestampType{Unit: arrow.Microsecond}},
		{"postgres", "INT2", arrow.PrimitiveTypes.Int16}, {"postgresql", "INT4", arrow.PrimitiveTypes.Int32}, {"cockroachdb", "INT8", arrow.PrimitiveTypes.Int64}, {"alloydb", "TIMESTAMP", &arrow.TimestampType{Unit: arrow.Microsecond}}, {"redshift", "TIMESTAMPTZ", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}}, {"postgres", "UUID", arrow.BinaryTypes.String}, {"postgres", "JSONB", arrow.BinaryTypes.String}, {"postgres", "BYTEA", arrow.BinaryTypes.Binary},
	} {
		t.Run(test.source+"/"+test.name, func(t *testing.T) {
			schema, err := schemaFor(typesForTest(t, []metadataColumn{{name: test.name}}), Dialect{SourceType: test.source})
			if err != nil {
				t.Fatal(err)
			}
			field := schema.Field(0)
			if !arrow.TypeEqual(field.Type, test.want) {
				t.Fatalf("got %s want %s", field.Type, test.want)
			}
			native, _ := field.Metadata.GetValue("native_type")
			source, _ := field.Metadata.GetValue("source_type")
			if native != test.name || source != test.source {
				t.Fatal("native metadata missing")
			}
		})
	}
}
func TestNumericTypmodIsRequired(t *testing.T) {
	for _, test := range []struct {
		kind, typ string
		p, s      int64
		ok, want  bool
	}{
		{"postgres", "NUMERIC", 65535, 65531, true, false}, {"postgres", "NUMERIC", 0, 0, false, false}, {"postgres", "NUMERIC", 10, 65534, true, false}, {"postgres", "NUMERIC", 77, 0, true, false}, {"mysql", "DECIMAL", 66, 2, true, false}, {"postgres", "NUMERIC", 30, 10, true, true}, {"mysql", "DECIMAL", 65, 30, true, true},
	} {
		_, err := schemaFor(typesForTest(t, []metadataColumn{{test.typ, test.p, test.s, test.ok}}), Dialect{SourceType: test.kind})
		if (err == nil) != test.want {
			t.Fatalf("numeric metadata accepted=%v want %v", err == nil, test.want)
		}
	}
}
func TestNormalizeUTCWallTimeAndZeroDate(t *testing.T) {
	local := time.Date(2026, 10, 1, 12, 30, 0, 123456000, time.FixedZone("source", 19800))
	for _, test := range []struct {
		kind, name string
		wantHour   int
	}{{"postgres", "TIMESTAMP", 12}, {"postgres", "TIMESTAMPTZ", 7}, {"mysql", "DATETIME", 12}, {"mysql", "TIMESTAMP", 7}} {
		schema, err := schemaFor(typesForTest(t, []metadataColumn{{name: test.name}}), Dialect{SourceType: test.kind})
		if err != nil {
			t.Fatal(err)
		}
		row, err := normalizeRow(schema, []any{local})
		if err != nil {
			t.Fatal(err)
		}
		value := row[0].(time.Time)
		if value.Hour() != test.wantHour || value.Nanosecond() != 123456000 || value.Location() != time.UTC {
			t.Fatal("temporal value changed")
		}
	}
	schema, _ := schemaFor(typesForTest(t, []metadataColumn{{name: "DATE"}}), Dialect{SourceType: "mysql"})
	if _, err := normalizeRow(schema, []any{time.Time{}}); err == nil {
		t.Fatal("zero date silently became valid")
	}
	schema, _ = schemaFor(typesForTest(t, []metadataColumn{{name: "TIMESTAMP"}}), Dialect{SourceType: "postgres"})
	if _, err := normalizeRow(schema, []any{time.Time{}}); err != nil {
		t.Fatal("valid PostgreSQL year one rejected")
	}
	if _, err := normalizeValue(&arrow.Decimal128Type{Precision: 30, Scale: 10}, float64(123.45)); err == nil {
		t.Fatal("inexact float accepted as decimal")
	}
	for _, kind := range []string{"postgres", "mysql"} {
		for _, name := range []string{"TIME", "INTERVAL", "GEOMETRY", "INT4[]"} {
			_, err := schemaFor(typesForTest(t, []metadataColumn{{name: name}}), Dialect{SourceType: kind})
			if err == nil {
				t.Fatalf("unsupported %s %s accepted", kind, name)
			}
		}
	}
}
