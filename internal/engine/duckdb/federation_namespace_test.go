//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

func TestFederationCatalogNamesResolveWithoutSQLRewriting(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, source := range []string{"warehouse", "billing"} {
		for _, statement := range []string{"ATTACH ':memory:' AS " + source, "USE " + source + ".main", "CREATE TABLE selected_orders(id bigint)", "INSERT INTO selected_orders VALUES(7)"} {
			if _, err := conn.ExecContext(ctx, statement); err != nil {
				t.Fatal(statement, err)
			}
		}
		if err := conn.Raw(func(raw any) error {
			return exposeFederationNamespace(ctx, raw.(driver.ExecerContext), source, catalog.FederationTable{Name: "selected_orders", Schema: "public", Table: "orders"})
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.ExecContext(ctx, "USE memory.main"); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"SELECT id FROM warehouse.selected_orders",
		"SELECT id FROM warehouse.public.orders",
		"SELECT a.id FROM warehouse.public.orders a JOIN billing.selected_orders b ON a.id=b.id",
		"WITH selected AS (SELECT * FROM warehouse.selected_orders) SELECT a.id FROM selected a JOIN billing.public.orders b ON a.id=b.id",
	} {
		var got int
		if err := conn.QueryRowContext(ctx, statement).Scan(&got); err != nil || got != 7 {
			t.Fatal(statement, got, err)
		}
	}
	for _, statement := range []string{"SELECT * FROM selected_orders", "SELECT * FROM warehouse.public.unselected", "SELECT * FROM billing.private.orders"} {
		if _, err := conn.ExecContext(ctx, statement); err == nil {
			t.Fatal("unselected relation exposed", statement)
		}
	}
}

func TestFederationNamespaceCollisionsRejectBeforeDiscovery(t *testing.T) {
	base := func() []catalog.Source {
		return []catalog.Source{{ID: "warehouse", Type: "postgres", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "selected", Table: "orders", Schema: "public"}}}}}
	}
	for name, mutate := range map[string]func([]catalog.Source) []catalog.Source{
		"reserved catalog":  func(s []catalog.Source) []catalog.Source { s[0].ID = "Memory"; return s },
		"ambiguous main":    func(s []catalog.Source) []catalog.Source { s[0].ID = "MAIN"; return s },
		"duplicate catalog": func(s []catalog.Source) []catalog.Source { return append(s, catalog.Source{ID: "WAREHOUSE"}) },
		"reserved schema": func(s []catalog.Source) []catalog.Source {
			s[0].Federation.Tables[0].Schema = "information_schema"
			return s
		},
		"invalid schema": func(s []catalog.Source) []catalog.Source { s[0].Federation.Tables[0].Schema = "public.other"; return s },
		"casefold modern view": func(s []catalog.Source) []catalog.Source {
			s[0].Federation.Tables = append(s[0].Federation.Tables, catalog.FederationTable{Name: "SELECTED", Table: "other", Schema: "public"})
			return s
		},
		"casefold legacy view": func(s []catalog.Source) []catalog.Source {
			s[0].Federation.Tables = append(s[0].Federation.Tables, catalog.FederationTable{Name: "other", Table: "Orders", Schema: "PUBLIC"})
			return s
		},
		"legacy modern overlap": func(s []catalog.Source) []catalog.Source {
			s[0].Federation.Tables = append(s[0].Federation.Tables, catalog.FederationTable{Name: "other", Table: "selected", Schema: "main"})
			return s
		},
	} {
		t.Run(name, func(t *testing.T) {
			if validateFederationNamespaces(mutate(base())) == nil {
				t.Fatal("ambiguous federation name accepted")
			}
		})
	}
	valid := base()
	valid[0].Federation.Tables = append(valid[0].Federation.Tables, catalog.FederationTable{Name: "main_orders", Table: "main_orders", Schema: "main"})
	if err := validateFederationNamespaces(valid); err != nil {
		t.Fatal(err)
	}
	if got := federationNamespace(catalog.FederationTable{Database: "analytics"}); got != "analytics" {
		t.Fatal(got)
	}
}
