// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"slices"
	"testing"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func predicateSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
		{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "text", Type: arrow.BinaryTypes.String},
		{Name: "large_text", Type: arrow.BinaryTypes.LargeString},
		{Name: "binary", Type: arrow.BinaryTypes.Binary},
		{Name: "float32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "float64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "decimal", Type: &arrow.Decimal128Type{Precision: 20, Scale: 3}},
		{Name: "date", Type: arrow.FixedWidthTypes.Date32},
		{Name: "date64", Type: arrow.FixedWidthTypes.Date64},
		{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "null", Type: arrow.Null},
	}, nil)
}

func TestPredicateCapabilitiesNativeDialects(t *testing.T) {
	allIntegers := []string{"i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64"}
	for _, tc := range []struct {
		name    string
		dialect scanDialect
		want    []string
	}{
		{"clickhouse", dialectClickHouse, append(slices.Clone(allIntegers), "boolean", "date")},
		{"postgres", dialectPostgres, []string{"i16", "i32", "i64", "u32", "boolean"}},
		{"mysql", dialectMySQL, allIntegers},
		{"sqlserver", dialectSQLServer, []string{"i16", "i32", "i64", "u8", "boolean"}},
		{"oracle", dialectOracle, []string{"i8", "i16", "i32", "i64"}},
		{"snowflake", dialectSnowflake, []string{"i8", "i16", "i32", "i64", "boolean"}},
		{"databricks", dialectDatabricks, []string{"i8", "i16", "i32", "i64", "boolean"}},
		{"bigquery", dialectBigQuery, []string{"i64", "boolean"}},
		{"unknown", scanDialect(255), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := &Table{schema: predicateSchema(), dialect: tc.dialect,
				factory: func(catalog.Config, query.Limits) (execution, error) {
					panic("capability lookup must not open an executor")
				}}
			if got := table.PredicateCapabilities().Columns; !slices.Equal(got, tc.want) {
				t.Fatalf("columns = %v, want %v", got, tc.want)
			}
		})
	}
}

type predicateNoIODriver struct{}

func (predicateNoIODriver) Validate(federationapi.Source, federationapi.Table) error {
	panic("capability lookup must not call a custom adapter")
}

func (predicateNoIODriver) Open(context.Context, federationapi.Source, federationapi.Table, federationapi.Limits) (federationapi.Relation, error) {
	panic("capability lookup must not open a custom adapter")
}

type predicateAdvisoryDriver struct {
	predicateNoIODriver
	declaration func() federationapi.Capabilities
}

func (d predicateAdvisoryDriver) FederationCapabilities() federationapi.Capabilities {
	return d.declaration()
}

func TestPredicateCapabilitiesCustomDeclarationsRemainAdvisory(t *testing.T) {
	full := federationapi.Capabilities{Version: federationapi.CapabilityVersion,
		Projection: true, NullPredicates: true, Conjunction: true, Disjunction: true,
		Comparisons: []federationapi.ComparisonCapability{{Type: "int64", Operators: []string{"eq", "ne", "lt", "le", "gt", "ge"}}}}
	declarations := []federationapi.Capabilities{
		{}, {Version: federationapi.CapabilityVersion + 1}, full,
		{Version: federationapi.CapabilityVersion, Comparisons: []federationapi.ComparisonCapability{{Type: "int64", Operators: []string{"eq"}}}},
		{Version: federationapi.CapabilityVersion, Comparisons: []federationapi.ComparisonCapability{{Type: "date32", Operators: []string{"eq", "ne", "lt", "le", "gt", "ge"}}}},
	}
	drivers := []federationapi.Driver{predicateNoIODriver{}, predicateAdvisoryDriver{
		declaration: func() federationapi.Capabilities { panic("advisory declaration must not be called") },
	}}
	calls := 0
	for _, declaration := range declarations {
		drivers = append(drivers, predicateAdvisoryDriver{declaration: func() federationapi.Capabilities {
			calls++
			return declaration
		}})
	}
	for _, driver := range drivers {
		table := &Table{schema: predicateSchema(), dialect: dialectClickHouse, customDriver: driver}
		if got := table.PredicateCapabilities().Columns; len(got) != 0 {
			t.Fatalf("custom adapter advertised mandatory query predicates: %v", got)
		}
	}
	if calls != 0 {
		t.Fatal("capability lookup invoked advisory adapter code")
	}
}

func TestPredicateCapabilitiesGuardUsesOnlyExposedLocalColumns(t *testing.T) {
	fields := append(predicateSchema().Fields(), arrow.Field{Name: "tenant_id", Type: arrow.PrimitiveTypes.Int64})
	raw := arrow.NewSchema(fields, nil)
	visible := []string{"boolean", "u64", "u32", "u16", "u8", "i64", "i32", "i16", "i8", "text", "decimal", "date", "date64", "timestamp"}
	guard, err := access.NewRelation(raw, access.TablePolicy{Columns: visible,
		Rows: &access.Predicate{Kind: "comparison", Column: "tenant_id", Op: "eq", Type: "int64", Value: "7"}},
		func(context.Context, federationapi.ScanPlan) (array.RecordReader, error) {
			panic("capability lookup must not scan a guard")
		})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		table *Table
	}{
		{"native", &Table{dialect: dialectMySQL}},
		{"clickhouse", &Table{dialect: dialectClickHouse}},
		{"unknown", &Table{dialect: scanDialect(255)}},
		{"custom", &Table{customDriver: predicateNoIODriver{}}},
		{"snapshot", &Table{snapshot: &snapshotTable{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.table.schema, tc.table.guard = raw, guard
			got := tc.table.PredicateCapabilities().Columns
			want := visible[:9]
			if !slices.Equal(got, want) {
				t.Fatalf("guard columns = %v, want %v", got, want)
			}
			got[0] = "tenant_id"
			if again := tc.table.PredicateCapabilities().Columns; !slices.Equal(again, want) {
				t.Fatalf("caller mutation changed guard capabilities: %v", again)
			}
			for i, field := range guard.Schema().Fields() {
				if field.Name != visible[i] {
					t.Fatal("capability lookup changed the exposed guard schema")
				}
			}
		})
	}
}

func TestPredicateCapabilitiesEmptyAndDetached(t *testing.T) {
	for _, table := range []*Table{nil, {}, {snapshot: &snapshotTable{}, schema: predicateSchema()}} {
		if got := table.PredicateCapabilities().Columns; len(got) != 0 {
			t.Fatalf("incomplete or unguarded snapshot table advertised predicates: %v", got)
		}
	}
	// Arrow rejects nil field types while constructing the schema. A valid,
	// unsupported NULL field still proves eligibility skips non-scalar fields.
	schema := arrow.NewSchema([]arrow.Field{{Name: "unsupported", Type: arrow.Null}, {Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	table := &Table{schema: schema, dialect: dialectClickHouse}
	first := table.PredicateCapabilities()
	if !slices.Equal(first.Columns, []string{"id"}) {
		t.Fatalf("columns = %v, want [id]", first.Columns)
	}
	first.Columns[0] = "changed"
	if got := table.PredicateCapabilities().Columns; !slices.Equal(got, []string{"id"}) || schema.Field(1).Name != "id" {
		t.Fatal("caller mutation changed subsequent capabilities or the schema")
	}
}
