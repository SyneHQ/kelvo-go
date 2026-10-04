// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package access

import (
	"context"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func testPolicy() Policy {
	return Policy{Sources: map[string]SourcePolicy{"warehouse": {Tables: map[string]TablePolicy{"orders": {Columns: []string{"id", "amount"}, Rows: &Predicate{Kind: "comparison", Column: "tenant_id", Type: "uint64", Op: "eq", Value: "18446744073709551615"}}}}}}
}
func testContext(t *testing.T, p Policy) context.Context {
	t.Helper()
	ctx, err := WithPolicy(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestPolicyContextIsDetachedAndMissingTablesDeny(t *testing.T) {
	p := testPolicy()
	ctx := testContext(t, p)
	p.Sources["warehouse"].Tables["orders"].Rows.Value = "0"
	p.Sources["warehouse"].Tables["orders"].Columns[0] = "private"
	copy, ok := PolicyFromContext(ctx)
	if !ok || !Restricted(ctx) {
		t.Fatal("policy missing")
	}
	copy.Sources["warehouse"].Tables["orders"].Rows.Value = "1"
	table, restricted, allowed := Lookup(ctx, "warehouse", "orders")
	if !restricted || !allowed || table.Rows.Value != "18446744073709551615" || table.Columns[0] != "id" {
		t.Fatal("caller mutated authority")
	}
	table.Rows.Value = "2"
	if _, restricted, allowed = Lookup(ctx, "warehouse", "hidden"); !restricted || allowed {
		t.Fatal("missing table allowed")
	}
	if _, restricted, allowed = Lookup(ctx, "other", "orders"); restricted || !allowed {
		t.Fatal("whole-source grant changed")
	}
	if Restricted(context.WithValue(context.Background(), "sources", testPolicy())) {
		t.Fatal("public context key created authority")
	}
}

func TestPolicyRequiresExplicitRowsAndColumns(t *testing.T) {
	cases := []TablePolicy{
		{}, {Columns: []string{"id"}}, {Columns: []string{"id"}, AllRows: true, Rows: &Predicate{Kind: "is_null", Column: "id"}},
		{Columns: []string{"*"}, AllRows: true}, {Columns: []string{"id", "ID"}, AllRows: true},
		{Columns: []string{"id"}, Rows: &Predicate{Kind: "comparison", Column: "id", Type: "uint64", Op: "eq", Value: "01"}},
		{Columns: []string{"id"}, Rows: &Predicate{Kind: "comparison", Column: "id", Type: "uint64", Op: "eq", Value: "18446744073709551616"}},
		{Columns: []string{"id"}, Rows: &Predicate{Kind: "comparison", Column: "id", Type: "float64", Op: "eq", Value: "1"}},
		{Columns: []string{"id"}, Rows: &Predicate{Kind: "and"}},
		{Columns: []string{"id"}, Rows: &Predicate{Kind: "is_null", Column: "id", Value: "ignored"}},
	}
	for i, table := range cases {
		p := testPolicy()
		p.Sources["warehouse"].Tables["orders"] = table
		if Validate(p) == nil {
			t.Fatalf("invalid policy %d accepted", i)
		}
	}
	if Validate(Policy{}) == nil {
		t.Fatal("empty policy accepted")
	}
	p := testPolicy()
	p.Sources["warehouse"].Tables["orders"] = TablePolicy{Columns: []string{"id"}, AllRows: true}
	if err := Validate(p); err != nil {
		t.Fatal(err)
	}
	row := Predicate{Kind: "is_null", Column: "id"}
	for i := 0; i < 34; i++ {
		row = Predicate{Kind: "and", Children: []Predicate{row}}
	}
	p.Sources["warehouse"].Tables["orders"] = TablePolicy{Columns: []string{"id"}, Rows: &row}
	if Validate(p) == nil {
		t.Fatal("deep policy accepted")
	}
	p = testPolicy()
	p.Sources["warehouse"].Tables["orders"].Rows.Value = strings.Repeat("a", 4097)
	p.Sources["warehouse"].Tables["orders"].Rows.Type = "string"
	if Validate(p) == nil {
		t.Fatal("oversized value accepted")
	}
}

func TestPolicyPreflightRejectsDirectAndSnapshotReaders(t *testing.T) {
	ctx := testContext(t, testPolicy())
	callback := catalog.Source{ID: "warehouse", Type: "clickhouse", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "orders", Table: "orders", Database: "reports"}}}}
	req := query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: "SELECT count(*) FROM warehouse.orders"}
	if err := ValidateRequest(ctx, catalog.Config{Sources: []catalog.Source{callback}}, req); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"postgres", "mysql", "csv", "parquet", "duckdb", "sqlite", "accelerated"} {
		bad := catalog.Source{ID: "warehouse", Type: kind, Path: "must-never-open"}
		if ValidateRequest(ctx, catalog.Config{Sources: []catalog.Source{bad}}, req) == nil {
			t.Fatalf("direct %s allowed", kind)
		}
	}
	mixed := req
	mixed.Sources = []string{"warehouse", "raw"}
	if ValidateRequest(ctx, catalog.Config{Sources: []catalog.Source{callback, {ID: "raw", Type: "parquet", Path: "private"}}}, mixed) == nil {
		t.Fatal("mixed raw source allowed")
	}
	native := req
	native.Mode = "native"
	native.ConnectionID = "warehouse"
	native.Sources = nil
	if ValidateRequest(ctx, catalog.Config{Sources: []catalog.Source{callback}}, native) == nil {
		t.Fatal("native bypass allowed")
	}
	diagnostics := req
	diagnostics.ScanDiagnostics = true
	if ValidateRequest(ctx, catalog.Config{Sources: []catalog.Source{callback}}, diagnostics) == nil {
		t.Fatal("raw diagnostics allowed")
	}
	if ValidateRequest(ctx, catalog.Config{Sources: []catalog.Source{callback}}, query.Request{Mode: "federated", SQL: "SELECT 1"}) == nil {
		t.Fatal("unselected policy retained")
	}
	if err := ValidateRequest(context.Background(), catalog.Config{}, native); err != nil {
		t.Fatal("legacy authorization changed")
	}
}
