//go:build duckbridge && duckdb_arrow && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckbridge

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestFactoryRegistersInsideSelectedCatalog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	var factories []*Factory
	defer func() {
		conn.Close()
		db.Close()
		for _, f := range factories {
			f.Close()
		}
		if activeStreams.Load() != 0 || activePins.Load() != 0 {
			t.Error("catalog registration leaked bridge ownership")
		}
	}()
	for _, alias := range []string{"warehouse", "billing"} {
		for _, statement := range []string{"ATTACH ':memory:' AS " + alias, "USE " + alias + ".main", "CREATE SCHEMA public"} {
			if _, err := conn.ExecContext(ctx, statement); err != nil {
				t.Fatal(statement, err)
			}
		}
		schema := bridgeSchema()
		factory, err := New(ctx, schema, func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
			return fixtureReader(t, schema, plan)
		}, PredicateCapabilities{})
		if err != nil {
			t.Fatal(err)
		}
		factories = append(factories, factory)
		if err := conn.Raw(func(raw any) error { return factory.Register(raw.(driver.Conn), "main", "selected") }); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, "CREATE VIEW "+alias+".public.orders AS SELECT * FROM "+alias+".main.selected"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.ExecContext(ctx, "USE memory.main; SET enable_external_access=false"); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"SELECT COUNT(*) FROM warehouse.selected",
		"SELECT COUNT(*) FROM warehouse.public.orders",
		"SELECT COUNT(*) FROM warehouse.selected a JOIN billing.public.orders b USING(id)",
		"WITH picked AS (SELECT * FROM warehouse.public.orders) SELECT COUNT(*) FROM picked a JOIN billing.selected b USING(id)",
	} {
		var got int
		if err := conn.QueryRowContext(ctx, statement).Scan(&got); err != nil || got != 5 {
			t.Fatal(statement, got, err)
		}
	}
	if _, err := conn.ExecContext(ctx, "SELECT * FROM main.selected"); err == nil {
		t.Fatal("catalog registration leaked into default schema")
	}
}
