// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/apache/arrow-go/v18/arrow"
)

func relationalSource(kind string) (catalog.Source, catalog.FederationTable) {
	selected := catalog.FederationTable{Name: "events", Table: "fact"}
	if kind == "postgres" {
		selected.Schema = "acceptance"
	} else {
		selected.Database = "acceptance"
	}
	return catalog.Source{ID: "warehouse", Type: kind, DSNEnv: "KELVO_SOURCE_FEDERATION_RELATIONAL_DSN", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{selected}}}, selected
}

func TestNativeRelationalFactoriesKeepTheirBackend(t *testing.T) {
	for _, kind := range []string{"postgres", "mysql", "sqlserver", "oracle"} {
		t.Run(kind, func(t *testing.T) {
			source, _ := relationalSource(kind)
			t.Setenv(source.DSNEnv, "")
			dialect, err := dialectFor(kind)
			if err != nil {
				t.Fatal(err)
			}
			executor, err := dialect.executor(catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer executor.Close()
			stats, err := executor.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: source.ID, SQL: "SELECT 1"}, &describeSink{})
			checkCode(t, err, "CONFIGURATION_ERROR")
			if stats.Backend != kind {
				t.Fatalf("selected wrong native executor: %s", stats.Backend)
			}
		})
	}
	for _, unsupported := range []string{"sqlite", "mongodb", "postgresql", "mariadb", "unknown"} {
		if _, err := dialectFor(unsupported); err == nil {
			t.Fatalf("unregistered dialect %q accepted", unsupported)
		}
	}
}

func TestRelationalQualificationAndRegisteredTableBoundary(t *testing.T) {
	for _, kind := range []string{"postgres", "mysql"} {
		source, selected := relationalSource(kind)
		dialect, _ := dialectFor(kind)
		name, err := dialect.tableName(selected)
		want := `"acceptance"."fact"`
		if kind == "mysql" {
			want = "`acceptance`.`fact`"
		}
		if err != nil || name != want {
			t.Fatalf("%s source qualification changed: %s %v", kind, name, err)
		}
		if kind == "postgres" {
			selected.Schema = "secret"
		} else {
			selected.Database = "secret"
		}
		_, err = newTable(context.Background(), source, selected, query.DefaultLimits(), func(catalog.Config, query.Limits) (execution, error) {
			t.Fatal("unregistered namespace reached native source")
			return nil, nil
		})
		checkCode(t, err, "PERMISSION_DENIED")
	}
}

func TestRelationalExactIntegerBoundaries(t *testing.T) {
	for _, test := range []struct {
		dialect     scanDialect
		kind, value string
		typ         arrow.DataType
		cast        string
	}{
		{dialectPostgres, "int16", "-32768", arrow.PrimitiveTypes.Int16, "SMALLINT"},
		{dialectPostgres, "int16", "32767", arrow.PrimitiveTypes.Int16, "SMALLINT"},
		{dialectPostgres, "int32", "-2147483648", arrow.PrimitiveTypes.Int32, "INTEGER"},
		{dialectPostgres, "int32", "2147483647", arrow.PrimitiveTypes.Int32, "INTEGER"},
		{dialectPostgres, "int64", "-9223372036854775808", arrow.PrimitiveTypes.Int64, "BIGINT"},
		{dialectPostgres, "int64", "9223372036854775807", arrow.PrimitiveTypes.Int64, "BIGINT"},
		{dialectPostgres, "uint32", "4294967295", arrow.PrimitiveTypes.Uint32, "OID"},
		{dialectMySQL, "int8", "-128", arrow.PrimitiveTypes.Int8, "SIGNED"},
		{dialectMySQL, "int16", "-32768", arrow.PrimitiveTypes.Int16, "SIGNED"},
		{dialectMySQL, "int32", "-2147483648", arrow.PrimitiveTypes.Int32, "SIGNED"},
		{dialectMySQL, "int64", "-9223372036854775808", arrow.PrimitiveTypes.Int64, "SIGNED"},
		{dialectMySQL, "int64", "9223372036854775807", arrow.PrimitiveTypes.Int64, "SIGNED"},
		{dialectMySQL, "uint8", "255", arrow.PrimitiveTypes.Uint8, "UNSIGNED"},
		{dialectMySQL, "uint16", "65535", arrow.PrimitiveTypes.Uint16, "UNSIGNED"},
		{dialectMySQL, "uint32", "4294967295", arrow.PrimitiveTypes.Uint32, "UNSIGNED"},
		{dialectMySQL, "uint64", "18446744073709551615", arrow.PrimitiveTypes.Uint64, "UNSIGNED"},
	} {
		got, err := test.dialect.exactConstant(test.kind, test.value, test.typ)
		want := "CAST('" + test.value + "' AS " + test.cast + ")"
		if err != nil || got != want {
			t.Fatalf("%v %s %s changed: got %s, error %v", test.dialect, test.kind, test.value, got, err)
		}
	}
	for _, dialect := range []scanDialect{dialectPostgres, dialectMySQL} {
		for _, value := range []string{"-9223372036854775809", "9223372036854775808", "01", "-0", "+1", "1e2", "1.0", "1' OR 1=1 --"} {
			if _, err := dialect.exactConstant("int64", value, arrow.PrimitiveTypes.Int64); err == nil {
				t.Fatalf("inexact or injected integer accepted: %q", value)
			}
		}
		if _, err := dialect.exactConstant("int64", "1", arrow.PrimitiveTypes.Uint64); err == nil {
			t.Fatal("integer signedness mismatch accepted")
		}
	}
	if _, err := dialectMySQL.exactConstant("uint64", "18446744073709551616", arrow.PrimitiveTypes.Uint64); err == nil {
		t.Fatal("MySQL UInt64 overflow accepted")
	}
	for _, kind := range []string{"int8", "uint8", "uint16", "uint64"} {
		typ := map[string]arrow.DataType{"int8": arrow.PrimitiveTypes.Int8, "uint8": arrow.PrimitiveTypes.Uint8, "uint16": arrow.PrimitiveTypes.Uint16, "uint64": arrow.PrimitiveTypes.Uint64}[kind]
		if _, err := dialectPostgres.exactConstant(kind, "1", typ); err == nil {
			t.Fatalf("PostgreSQL accepted unsupported native width %s", kind)
		}
	}
}

func TestRelationalNullLogicAndRequiredUnsupportedPredicates(t *testing.T) {
	for _, dialect := range []scanDialect{dialectPostgres, dialectMySQL} {
		table := compilerTable()
		table.dialect = dialect
		plan := duckbridge.ScanPlan{Columns: []string{"label", "signed"}, Filters: []duckbridge.Filter{{Kind: "or", Children: []duckbridge.Filter{
			{Kind: "is_null", Column: "label"},
			{Kind: "and", Children: []duckbridge.Filter{{Kind: "is_not_null", Column: "label"}, comparison("signed", "int64", "-9223372036854775808", "ge")}},
		}}}}
		sql, _, err := table.compileScan(plan)
		if err != nil {
			t.Fatal(err)
		}
		quoted, _ := dialect.quoteIdentifier("label")
		if !strings.Contains(sql, "(("+quoted+" IS NULL) OR (("+quoted+" IS NOT NULL) AND (") || strings.Contains(sql, " LIMIT ") {
			t.Fatalf("NULL predicate tree or relation changed: %s", sql)
		}
		for _, filter := range []duckbridge.Filter{
			comparison("label", "string", "secret", "eq"), comparison("signed", "float64", "1", "eq"),
			comparison("signed", "date", "2026-01-01", "eq"), {Kind: "in", Column: "signed"},
			{Kind: "is_null", Column: "label", Value: "secret"},
		} {
			if _, _, err := table.compileScan(duckbridge.ScanPlan{Columns: []string{"signed"}, Filters: []duckbridge.Filter{filter}}); err == nil {
				t.Fatalf("unsupported required predicate accepted: %+v", filter)
			}
		}
	}
	if value, err := dialectPostgres.exactConstant("bool", "true", arrow.FixedWidthTypes.Boolean); err != nil || value != "true" {
		t.Fatalf("PostgreSQL boolean changed: %s %v", value, err)
	}
	if _, err := dialectMySQL.exactConstant("bool", "true", arrow.FixedWidthTypes.Boolean); err == nil {
		t.Fatal("MySQL's native integer boolean was silently relabeled as Arrow Bool")
	}
}

func TestRelationalIdentifiersStayWithinOneReadOnlyStatement(t *testing.T) {
	for _, test := range []struct {
		dialect scanDialect
		name    string
		quoted  string
	}{
		{dialectPostgres, `x"); DROP TABLE secret; --`, `"x""); DROP TABLE secret; --"`},
		{dialectMySQL, "x`); DROP TABLE secret; --", "`x``); DROP TABLE secret; --`"},
	} {
		quoted, err := test.dialect.quoteIdentifier(test.name)
		if err != nil || quoted != test.quoted {
			t.Fatalf("identifier changed: %s %v", quoted, err)
		}
		sql := "SELECT " + quoted + " FROM " + quoted
		if checked, err := sqlguard.ReadOnlyWithOptions(sql, test.dialect == dialectPostgres); err != nil || checked != sql {
			t.Fatalf("quoted identifier escaped the native read-only envelope: %s %v", checked, err)
		}
		for _, invalid := range []string{"", "back\\slash", "nul\x00name", "line\nname", string([]byte{0xff})} {
			if _, err := test.dialect.quoteIdentifier(invalid); err == nil {
				t.Fatalf("ambiguous identifier accepted: %q", invalid)
			}
		}
	}
}
