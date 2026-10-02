// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/apache/arrow-go/v18/arrow"
)

func compilerTable() *Table {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64}, {Name: "signed", Type: arrow.PrimitiveTypes.Int64},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean}, {Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "tiny", Type: arrow.PrimitiveTypes.Int8},
	}, nil)
	table := &Table{schema: schema, columns: make(map[string]arrow.Field), remoteName: "`reports`.`events`"}
	for _, field := range schema.Fields() {
		table.columns[field.Name] = field
	}
	return table
}
func comparison(column, typ, value, op string) duckbridge.Filter {
	return duckbridge.Filter{Kind: "comparison", Column: column, Type: typ, Value: value, Op: op}
}
func TestCompileProjectionAndExactPredicates(t *testing.T) {
	table := compilerTable()
	plan := duckbridge.ScanPlan{Columns: []string{"label", "id"}, Filters: []duckbridge.Filter{
		comparison("id", "uint64", "18446744073709551615", "eq"),
		{Kind: "or", Children: []duckbridge.Filter{comparison("signed", "int64", "-9223372036854775808", "ge"), {Kind: "is_null", Column: "label"}}},
		comparison("active", "bool", "true", "ne"),
	}}
	sql, schema, err := table.compileScan(plan)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT `label`, `id` FROM `reports`.`events` WHERE (`id` = CAST('18446744073709551615' AS UInt64)) AND ((`signed` >= CAST('-9223372036854775808' AS Int64)) OR (`label` IS NULL)) AND (`active` != true)"
	if sql != want {
		t.Fatalf("compiled query differs:\n%s\nwant:\n%s", sql, want)
	}
	if schema.NumFields() != 2 || schema.Field(0).Name != "label" || !schema.Field(0).Nullable || schema.Field(1).Type.ID() != arrow.UINT64 {
		t.Fatal("projection order, nullability or exact type changed")
	}
	if strings.Contains(sql, " LIMIT ") {
		t.Fatal("scan silently truncates the relation")
	}
}
func TestEmptyProjectionPreservesSourceRowCount(t *testing.T) {
	sql, schema, err := compilerTable().compileScan(duckbridge.ScanPlan{})
	if err != nil || sql != "SELECT toUInt8(1) AS `__kelvo_count` FROM `reports`.`events`" || schema.NumFields() != 1 || schema.Field(0).Name != countColumn || schema.Field(0).Type.ID() != arrow.UINT8 {
		t.Fatalf("count projection: %s %v", sql, err)
	}
}
func TestRequiredFiltersFailClosed(t *testing.T) {
	cases := []duckbridge.Filter{
		comparison("id", "uint64", "18446744073709551616", "eq"), comparison("signed", "int64", "-9223372036854775809", "eq"),
		comparison("tiny", "int8", "128", "eq"), comparison("id", "uint64", "-1", "eq"), comparison("id", "int64", "1", "eq"),
		comparison("id", "uint64", "+1", "eq"), comparison("id", "uint64", "01", "eq"), comparison("signed", "int64", "-0", "eq"),
		comparison("id", "uint64", "1 OR 1=1", "eq"), comparison("id", "uint64", "1e3", "eq"), comparison("id", "uint64", "1.0", "eq"),
		comparison("id", "float64", "1", "eq"), comparison("label", "string", "text", "eq"), comparison("active", "bool", "TRUE", "eq"),
		comparison("id", "uint64", "1", "drop"), comparison("unregistered", "uint64", "1", "eq"),
		{Kind: "is_null", Column: "label", Value: "ignored"}, {Kind: "is_null", Column: "label", Children: []duckbridge.Filter{{Kind: "is_null", Column: "label"}}},
		{Kind: "between", Column: "id"}, {Kind: "or"},
		{Kind: "and", Column: "ignored", Children: []duckbridge.Filter{{Kind: "is_null", Column: "label"}, {Kind: "is_not_null", Column: "label"}}},
	}
	for _, filter := range cases {
		if _, _, err := compilerTable().compileScan(duckbridge.ScanPlan{Columns: []string{"id"}, Filters: []duckbridge.Filter{filter}}); err == nil {
			t.Errorf("required filter silently accepted: %+v", filter)
		}
	}
	if _, _, err := compilerTable().compileScan(duckbridge.ScanPlan{Columns: []string{"id; DROP TABLE events"}}); err == nil {
		t.Fatal("unregistered projection accepted")
	}
}
func TestIdentifiersRemainSingleQuotedNames(t *testing.T) {
	name := "x`); DROP TABLE other; --"
	table := compilerTable()
	table.columns[name] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Uint64}
	sql, _, err := table.compileScan(duckbridge.ScanPlan{Columns: []string{name}})
	if err != nil || sql != "SELECT `x\\`); DROP TABLE other; --` FROM `reports`.`events`" {
		t.Fatalf("identifier escape changed: %q %v", sql, err)
	}
	quoted, err := dialectClickHouse.quoteIdentifier("back\\slash")
	if err != nil || quoted != "`back\\\\slash`" {
		t.Fatalf("backslash escape: %q %v", quoted, err)
	}
}
func TestAllExactIntegerWidths(t *testing.T) {
	cases := []struct {
		kind, value, want string
		typ               arrow.DataType
	}{
		{"int8", "-128", "CAST('-128' AS Int8)", arrow.PrimitiveTypes.Int8},
		{"int16", "-32768", "CAST('-32768' AS Int16)", arrow.PrimitiveTypes.Int16},
		{"int32", "-2147483648", "CAST('-2147483648' AS Int32)", arrow.PrimitiveTypes.Int32},
		{"int64", "9223372036854775807", "CAST('9223372036854775807' AS Int64)", arrow.PrimitiveTypes.Int64},
		{"uint8", "255", "CAST('255' AS UInt8)", arrow.PrimitiveTypes.Uint8},
		{"uint16", "65535", "CAST('65535' AS UInt16)", arrow.PrimitiveTypes.Uint16},
		{"uint32", "4294967295", "CAST('4294967295' AS UInt32)", arrow.PrimitiveTypes.Uint32},
		{"uint64", "9007199254740993", "CAST('9007199254740993' AS UInt64)", arrow.PrimitiveTypes.Uint64},
	}
	for _, test := range cases {
		got, err := dialectClickHouse.exactConstant(test.kind, test.value, test.typ)
		if err != nil || got != test.want {
			t.Fatalf("%s changed: %s %v", test.kind, got, err)
		}
	}
}

func TestSingleChildConjunctionAndMetadata(t *testing.T) {
	table := compilerTable()
	schemaMetadata := arrow.NewMetadata([]string{"source-format"}, []string{"fixture-v1"})
	fieldMetadata := arrow.NewMetadata([]string{"semantic"}, []string{"identifier"})
	field := table.columns["id"]
	field.Metadata = fieldMetadata
	table.columns["id"] = field
	table.schema = arrow.NewSchema([]arrow.Field{field}, &schemaMetadata)
	sql, projected, err := table.compileScan(duckbridge.ScanPlan{Columns: []string{"id"}, Filters: []duckbridge.Filter{{Kind: "and", Children: []duckbridge.Filter{comparison("id", "uint64", "42", "eq")}}}})
	if err != nil || sql != "SELECT `id` FROM `reports`.`events` WHERE ((`id` = CAST('42' AS UInt64)))" {
		t.Fatalf("single-child predicate changed: %s %v", sql, err)
	}
	if !projected.Metadata().Equal(schemaMetadata) || !projected.Field(0).Metadata.Equal(fieldMetadata) {
		t.Fatal("projection dropped Arrow metadata")
	}
	changed := arrow.NewSchema([]arrow.Field{field}, nil)
	if sameSchema(projected, changed) {
		t.Fatal("schema metadata drift was accepted")
	}
}
