//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

type federationDiscardSink struct{}

func (federationDiscardSink) Schema(*arrow.Schema) error    { return nil }
func (federationDiscardSink) Write(arrow.RecordBatch) error { return nil }

func TestRawArrowEntryPointsRemainPrivate(t *testing.T) {
	for _, sql := range []string{"SELECT * FROM arrow_scan(1,2,3)", "SELECT * FROM ARROW_SCAN_DUMB(1,2,3)"} {
		if !containsDeniedCapability(sql) {
			t.Fatal("raw pointer table function allowed")
		}
	}
}

func TestRelationalCustomFederationKeepsExternalAccessLocked(t *testing.T) {
	for _, kind := range []string{"postgres", "mysql"} {
		t.Run(kind, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "unselected.txt")
			if err := os.WriteFile(file, []byte("must remain private"), 0600); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("duckdb", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			table := catalog.FederationTable{Name: "orders", Table: "orders"}
			if kind == "postgres" {
				table.Schema = "public"
			} else {
				table.Database = "analytics"
			}
			source := catalog.Source{ID: "db", Type: kind, DSNEnv: "KELVO_SOURCE_DB_DSN", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{table}}}
			workspace := t.TempDir()
			if err := conn.Raw(func(raw any) error { return lockSourceAccess(ctx, raw, []catalog.Source{source}, workspace) }); err != nil {
				t.Fatal(err)
			}
			var external bool
			if err := conn.QueryRowContext(ctx, "SELECT current_setting('enable_external_access')").Scan(&external); err != nil || external {
				t.Fatalf("custom source broadened DuckDB access: enabled=%v error=%v", external, err)
			}
			// Bypass Kelvo's SQL guard to exercise DuckDB's own file restriction.
			if _, err := conn.ExecContext(ctx, "SELECT * FROM read_text('"+quoteLiteral(file)+"')"); err == nil {
				t.Fatal("DuckDB opened an unselected file with custom relational source")
			}
		})
	}
}

func TestCustomFederationNeedsExplicitBridgeBuild(t *testing.T) {
	if duckbridge.Available() {
		t.Skip("bridge is present in this build")
	}
	source := catalog.Source{ID: "warehouse", Type: "clickhouse", URLEnv: "KELVO_SOURCE_UNSET_FEDERATION", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "events", Database: "analytics", Table: "events"}}}}
	e, err := New(catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Execute(context.Background(), query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: "SELECT * FROM warehouse.events"}, federationDiscardSink{})
	var qe *query.Error
	if !errors.As(err, &qe) || qe.Code != "UNSUPPORTED" {
		t.Fatal("did not reject missing bridge before reading credentials", err)
	}
}

func TestCustomFederationDiscoveryIsBoundedBeforeSourceAccess(t *testing.T) {
	makeSource := func(id string, n int) catalog.Source {
		s := catalog.Source{ID: id, Type: "clickhouse", URLEnv: "KELVO_SOURCE_UNSET_FEDERATION", Federation: &catalog.FederationConfig{}}
		for i := 0; i < n; i++ {
			s.Federation.Tables = append(s.Federation.Tables, catalog.FederationTable{Name: fmt.Sprintf("table_%d", i), Database: "analytics", Table: fmt.Sprintf("table_%d", i)})
		}
		return s
	}
	e, err := New(catalog.Config{Sources: []catalog.Source{makeSource("one", 32), makeSource("two", 1)}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Execute(context.Background(), query.Request{Mode: "federated", Sources: []string{"one", "two"}, SQL: "SELECT 1"}, federationDiscardSink{})
	var qe *query.Error
	if !errors.As(err, &qe) || qe.Code != "RESOURCE_EXHAUSTED" {
		t.Fatal("discovery was not bounded before reading credentials", err)
	}
}
