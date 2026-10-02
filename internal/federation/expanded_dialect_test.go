// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/apache/arrow-go/v18/arrow"
)

func TestExpandedNamespaceAndZeroRowDiscovery(t *testing.T) {
	for _, tc := range []struct {
		kind, database, schema, table, qualified, describe string
	}{
		{"sqlserver", "Sales", "dbo", "Orders", "[Sales].[dbo].[Orders]", "SELECT TOP (0) * FROM [Sales].[dbo].[Orders]"},
		{"oracle", "", "REPORTING", "ORDERS", `"REPORTING"."ORDERS"`, `SELECT * FROM "REPORTING"."ORDERS" WHERE 1 = 0`},
		{"snowflake", "SALES", "PUBLIC", "ORDERS", `"SALES"."PUBLIC"."ORDERS"`, `SELECT * FROM "SALES"."PUBLIC"."ORDERS" LIMIT 0`},
		{"databricks", "main", "reporting", "orders", "`main`.`reporting`.`orders`", "SELECT * FROM `main`.`reporting`.`orders` LIMIT 0"},
		{"bigquery", "sales-project", "reporting", "orders", "`sales-project.reporting.orders`", "SELECT * FROM `sales-project.reporting.orders` LIMIT 0"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			d, err := dialectFor(tc.kind)
			if err != nil {
				t.Fatal(err)
			}
			name, err := d.tableName(catalog.FederationTable{Database: tc.database, Schema: tc.schema, Table: tc.table})
			if err != nil || name != tc.qualified || d.describeSQL(name) != tc.describe {
				t.Fatalf("qualification/discovery changed: %q %q %v", name, d.describeSQL(name), err)
			}
			if _, err := sqlguard.ReadOnly(tc.describe); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExpandedIntegerAndBooleanFidelity(t *testing.T) {
	for _, tc := range []struct {
		d           scanDialect
		kind, value string
		typ         arrow.DataType
		want        string
	}{
		{dialectSQLServer, "uint8", "255", arrow.PrimitiveTypes.Uint8, "CAST('255' AS TINYINT)"},
		{dialectSQLServer, "int64", "-9223372036854775808", arrow.PrimitiveTypes.Int64, "CAST('-9223372036854775808' AS BIGINT)"},
		{dialectSQLServer, "bool", "false", arrow.FixedWidthTypes.Boolean, "CAST(0 AS BIT)"},
		{dialectSQLServer, "bool", "true", arrow.FixedWidthTypes.Boolean, "CAST(1 AS BIT)"},
		{dialectDatabricks, "int8", "-128", arrow.PrimitiveTypes.Int8, "CAST('-128' AS TINYINT)"},
		{dialectDatabricks, "int64", "9223372036854775807", arrow.PrimitiveTypes.Int64, "CAST('9223372036854775807' AS BIGINT)"},
		{dialectBigQuery, "int64", "9223372036854775807", arrow.PrimitiveTypes.Int64, "CAST('9223372036854775807' AS INT64)"},
		{dialectSnowflake, "bool", "true", arrow.FixedWidthTypes.Boolean, "true"},
		{dialectBigQuery, "bool", "false", arrow.FixedWidthTypes.Boolean, "false"},
	} {
		got, err := tc.d.exactConstant(tc.kind, tc.value, tc.typ)
		if err != nil || got != tc.want {
			t.Fatalf("exact value changed: %s %v; want %s", got, err, tc.want)
		}
	}
	for _, d := range []scanDialect{dialectSQLServer, dialectOracle, dialectSnowflake, dialectDatabricks, dialectBigQuery} {
		for _, value := range []string{"9223372036854775808", "01", "-0", "1e2", "1' OR 1=1 --"} {
			if _, err := d.exactConstant("int64", value, arrow.PrimitiveTypes.Int64); err == nil {
				t.Fatalf("inexact integer accepted: %q", value)
			}
		}
		if _, err := d.exactConstant("int64", "1", &arrow.Decimal128Type{Precision: 38, Scale: 0}); err == nil {
			t.Fatal("decimal column silently coerced to integer for pushdown")
		}
		if _, err := d.exactConstant("uint64", "18446744073709551615", arrow.PrimitiveTypes.Uint64); err == nil {
			t.Fatal("unsupported unsigned source type accepted")
		}
	}
	if _, err := dialectOracle.exactConstant("bool", "true", arrow.FixedWidthTypes.Boolean); err == nil {
		t.Fatal("Oracle version-dependent SQL BOOLEAN accepted")
	}
}

func TestExpandedProjectionAndNullFiltersStayReadOnly(t *testing.T) {
	for _, d := range []scanDialect{dialectSQLServer, dialectOracle, dialectSnowflake, dialectDatabricks, dialectBigQuery} {
		table := compilerTable()
		table.dialect = d
		plan := duckbridge.ScanPlan{Columns: []string{"label", "signed"}, Filters: []duckbridge.Filter{
			{Kind: "or", Children: []duckbridge.Filter{{Kind: "is_null", Column: "label"}, {Kind: "is_not_null", Column: "signed"}}},
		}}
		sql, schema, err := table.compileScan(plan)
		if err != nil || schema.Field(0).Name != "label" || schema.Field(1).Name != "signed" {
			t.Fatalf("projection drift: %s %v", sql, err)
		}
		if strings.Contains(sql, " LIMIT ") || strings.Contains(sql, " TOP ") {
			t.Fatal("source relation truncated before join")
		}
		if _, err := sqlguard.ReadOnly(sql); err != nil {
			t.Fatal(err)
		}
		if _, _, err := table.compileScan(duckbridge.ScanPlan{Columns: []string{"secret"}}); err == nil {
			t.Fatal("unknown projection accepted")
		}
	}
}

func TestExpandedQuotedIdentifiersCannotEscapeScan(t *testing.T) {
	for _, d := range []scanDialect{dialectSQLServer, dialectOracle, dialectSnowflake, dialectDatabricks, dialectBigQuery} {
		for _, name := range []string{`x]; DROP TABLE secrets; --`, `x"; DROP TABLE secrets; --`, "emoji_é"} {
			quoted, err := d.quoteIdentifier(name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sqlguard.ReadOnly("SELECT " + quoted + " FROM trusted"); err != nil {
				t.Fatalf("identifier escaped read envelope: %q %v", quoted, err)
			}
		}
		for _, name := range []string{"back\\slash", "line\nfeed", "nul\x00name", string([]byte{0xff})} {
			if _, err := d.quoteIdentifier(name); err == nil {
				t.Fatalf("ambiguous identifier accepted: %q", name)
			}
		}
	}
	if _, err := dialectBigQuery.quoteIdentifier("embedded`quote"); err == nil {
		t.Fatal("unsupported GoogleSQL quoting accepted")
	}
}
