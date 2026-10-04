// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func date32CompilerTable() *Table {
	metadata := arrow.MetadataFrom(map[string]string{"source-format": "date32-fixture"})
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "event_day", Type: arrow.FixedWidthTypes.Date32, Nullable: true, Metadata: metadata},
	}
	table := &Table{schema: arrow.NewSchema(fields, &metadata), columns: make(map[string]arrow.Field),
		dialect: dialectClickHouse, remoteName: "`reports`.`events`"}
	for _, field := range fields {
		table.columns[field.Name] = field
	}
	return table
}

func TestDate32CompilerExactSignedDaysAndOperators(t *testing.T) {
	// Includes the epoch, pre-epoch, 2000/2024 leap days, values outside common
	// source calendars, finite extrema and both DuckDB infinity encodings.
	values := []string{"0", "-1", "1", "11016", "19782", "-719529", "2932897",
		"-1000000000", "1000000000", "-2147483648", "-2147483647", "-2147483646", "2147483646", "2147483647"}
	operators := map[string]string{"eq": "=", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
	for op, operator := range operators {
		for _, value := range values {
			t.Run(op+"/"+value, func(t *testing.T) {
				table := date32CompilerTable()
				sql, schema, err := table.compileScan(duckbridge.ScanPlan{Columns: []string{"event_day", "id"},
					Filters: []duckbridge.Filter{comparison("event_day", "date32", value, op)}})
				want := "SELECT `event_day`, `id` FROM `reports`.`events` WHERE (toInt32(`event_day`) " + operator + " CAST('" + value + "' AS Int32))"
				if err != nil || sql != want {
					t.Fatalf("query = %q, error = %v; want %q", sql, err, want)
				}
				if schema.NumFields() != 2 || !schema.Field(0).Equal(table.columns["event_day"]) || !schema.Field(1).Equal(table.columns["id"]) || !schema.Metadata().Equal(table.schema.Metadata()) {
					t.Fatal("Date32 projection order, physical type, nullability or metadata changed")
				}
			})
		}
	}
}

func TestDate32CompilerPreservesNullsAndQuotedColumns(t *testing.T) {
	table := date32CompilerTable()
	plan := duckbridge.ScanPlan{Filters: []duckbridge.Filter{{Kind: "or", Children: []duckbridge.Filter{
		{Kind: "is_null", Column: "event_day"},
		{Kind: "and", Children: []duckbridge.Filter{
			{Kind: "is_not_null", Column: "event_day"},
			comparison("event_day", "date32", "-2147483647", "gt"),
			comparison("event_day", "date32", "2147483647", "lt"),
		}},
	}}}}
	sql, schema, err := table.compileScan(plan)
	want := "SELECT toUInt8(1) AS `__kelvo_count` FROM `reports`.`events` WHERE ((`event_day` IS NULL) OR ((`event_day` IS NOT NULL) AND (toInt32(`event_day`) > CAST('-2147483647' AS Int32)) AND (toInt32(`event_day`) < CAST('2147483647' AS Int32))))"
	if err != nil || sql != want || schema.NumFields() != 1 || schema.Field(0).Type.ID() != arrow.UINT8 {
		t.Fatalf("count/NULL plan changed: %q %v", sql, err)
	}
	name := "day`); DROP TABLE other; --\\tail"
	table.columns[name] = arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Date32, Nullable: true}
	sql, _, err = table.compileScan(duckbridge.ScanPlan{Columns: []string{name}, Filters: []duckbridge.Filter{comparison(name, "date32", "0", "eq")}})
	quoted := "`day\\`); DROP TABLE other; --\\\\tail`"
	want = "SELECT " + quoted + " FROM `reports`.`events` WHERE (toInt32(" + quoted + ") = CAST('0' AS Int32))"
	if err != nil || sql != want {
		t.Fatalf("Date32 column escaped identifier quoting: %q %v", sql, err)
	}
}

func TestDate32CompilerRejectsMalformedAndWrongTypes(t *testing.T) {
	badValues := []string{"", "+1", "01", "00", "-0", "-01", " 1", "1 ", "1\n", "1.0", "1e3", "0x1", "--1", "NaN", "NULL", "infinity", "-infinity", "2147483648", "-2147483649", "0' OR 1=1 --", strings.Repeat("9", 100)}
	for _, value := range badValues {
		for _, nested := range []bool{false, true} {
			filter := comparison("event_day", "date32", value, "eq")
			if nested {
				filter = duckbridge.Filter{Kind: "or", Children: []duckbridge.Filter{
					comparison("event_day", "date32", "0", "eq"), {Kind: "and", Children: []duckbridge.Filter{filter}},
				}}
			}
			sql, schema, err := date32CompilerTable().compileScan(duckbridge.ScanPlan{Columns: []string{"id"}, Filters: []duckbridge.Filter{filter}})
			checkCode(t, err, "UNSUPPORTED")
			if sql != "" || schema != nil {
				t.Fatal("a malformed child left a partially usable source query")
			}
		}
	}
	for _, typ := range []arrow.DataType{arrow.FixedWidthTypes.Date64, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64,
		arrow.BinaryTypes.String, &arrow.Decimal128Type{Precision: 10, Scale: 0}, &arrow.TimestampType{Unit: arrow.Second}} {
		table := date32CompilerTable()
		table.columns["event_day"] = arrow.Field{Name: "event_day", Type: typ}
		_, _, err := table.compileScan(duckbridge.ScanPlan{Columns: []string{"id"}, Filters: []duckbridge.Filter{comparison("event_day", "date32", "0", "eq")}})
		checkCode(t, err, "UNSUPPORTED")
	}
	for _, kind := range []string{"int32", "int64", "date64", "timestamp", "date", "DATE32"} {
		_, _, err := date32CompilerTable().compileScan(duckbridge.ScanPlan{Columns: []string{"id"}, Filters: []duckbridge.Filter{comparison("event_day", kind, "0", "eq")}})
		checkCode(t, err, "UNSUPPORTED")
	}
	for _, filter := range []duckbridge.Filter{
		comparison("event_day", "date32", "0", "contains"),
		comparison("missing_day", "date32", "0", "eq"),
		{Kind: "is_null", Column: "event_day", Type: "date32"},
		{Kind: "is_not_null", Column: "event_day", Value: "0"},
		{Kind: "comparison", Column: "event_day", Type: "date32", Value: "0", Op: "eq", Children: []duckbridge.Filter{{Kind: "is_null", Column: "event_day"}}},
	} {
		_, _, err := date32CompilerTable().compileScan(duckbridge.ScanPlan{Columns: []string{"id"}, Filters: []duckbridge.Filter{filter}})
		checkCode(t, err, "UNSUPPORTED")
	}
	for _, dialect := range []scanDialect{dialectPostgres, dialectMySQL, dialectSQLServer, dialectOracle, dialectSnowflake, dialectDatabricks, dialectBigQuery, scanDialect(255)} {
		table := date32CompilerTable()
		table.dialect = dialect
		_, _, err := table.compileScan(duckbridge.ScanPlan{Columns: []string{"id"}, Filters: []duckbridge.Filter{comparison("event_day", "date32", "0", "eq")}})
		checkCode(t, err, "UNSUPPORTED")
	}
}

func TestDate32RequiredFiltersRejectCustomGuardAndSnapshot(t *testing.T) {
	plan := duckbridge.ScanPlan{Columns: []string{"event_day"}, Filters: []duckbridge.Filter{comparison("event_day", "date32", "0", "eq")}}
	driver := &customTestDriver{schemaAt: func(int32) *arrow.Schema { return date32CompilerTable().schema }}
	custom := customFixtureTable(t, driver, query.DefaultLimits())
	_, err := custom.Scan(context.Background(), plan)
	checkCode(t, err, "UNSUPPORTED")
	if driver.opened.Load() != 1 || driver.closed.Load() != 1 {
		t.Fatal("required custom Date32 filter opened a source scan")
	}
	guard, err := access.NewRelation(date32CompilerTable().schema,
		access.TablePolicy{Columns: []string{"id", "event_day"}, AllRows: true},
		func(context.Context, federationapi.ScanPlan) (array.RecordReader, error) {
			t.Fatal("unsupported Date32 filter reached the guarded source")
			return nil, errors.New("unexpected source scan")
		})
	if err != nil {
		t.Fatal(err)
	}
	table := &Table{guard: guard}
	if reader, err := table.Scan(context.Background(), plan); err == nil {
		reader.Release()
		t.Fatal("guard silently discarded a required Date32 filter")
	}
	table = &Table{snapshot: &snapshotTable{}}
	if reader, err := table.Scan(context.Background(), plan); err == nil {
		reader.Release()
		t.Fatal("snapshot silently discarded a required Date32 filter")
	}
}

func TestDate32RequiredSourceFailureNeverRetriesUnfiltered(t *testing.T) {
	var opened, closed atomic.Int32
	failure := errors.New("fixture Date32 producer failed")
	schema := date32CompilerTable().schema
	table, err := newTable(context.Background(), testSource(), registeredTable, query.DefaultLimits(), func(catalog.Config, query.Limits) (execution, error) {
		opened.Add(1)
		return &fakeExecutor{run: func(_ context.Context, request query.Request, sink query.Sink) error {
			if err := sink.Schema(schema); err != nil {
				return err
			}
			if strings.HasSuffix(request.SQL, " LIMIT 0") {
				return nil
			}
			if !strings.Contains(request.SQL, "WHERE (toInt32(`event_day`) >= CAST('-1' AS Int32))") {
				return errors.New("required Date32 filter was changed or removed")
			}
			return failure
		}, close: func() { closed.Add(1) }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	// Malformed constants fail before creating any scan executor.
	_, err = table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id", "event_day"}, Filters: []duckbridge.Filter{comparison("event_day", "date32", "+1", "ge")}})
	checkCode(t, err, "UNSUPPORTED")
	if opened.Load() != 1 {
		t.Fatal("malformed constant opened a source scan")
	}
	reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id", "event_day"}, Filters: []duckbridge.Filter{comparison("event_day", "date32", "-1", "ge")}})
	if err != nil {
		t.Fatal(err)
	}
	if reader.Next() || !errors.Is(reader.Err(), failure) {
		t.Fatalf("producer error was hidden: %v", reader.Err())
	}
	reader.Release()
	_ = table.Close()
	if opened.Load() != 2 || closed.Load() != 2 || table.Stats().Scans != 1 || len(table.active) != 0 {
		t.Fatal("failed Date32 scan retried or leaked its source owner")
	}
}
