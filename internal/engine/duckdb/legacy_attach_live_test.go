//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Live check of the relation allowlist against legacy DuckDB postgres/mysql
// attachments. Run with KELVO_TEST_LEGACY_ATTACH=1 plus
// KELVO_TEST_EXTENSIONS (provisioned directory), KELVO_TEST_PG_DSN,
// KELVO_TEST_PG_ADMIN (pgx URL), KELVO_TEST_MYSQL_DSN, KELVO_TEST_MYSQL_ADMIN
// (go-sql-driver DSN). Not part of CI.
func TestLegacyAttachRelationAllowlistLive(t *testing.T) {
	if os.Getenv("KELVO_TEST_LEGACY_ATTACH") != "1" {
		t.Skip("set KELVO_TEST_LEGACY_ATTACH=1 to run against live postgres/mysql")
	}
	ext := os.Getenv("KELVO_TEST_EXTENSIONS")
	for _, tc := range []struct {
		id, kind, admin, driver string
		setup                   []string
		allowed                 map[string]int64
		late                    string
		lateQuery               string
	}{
		{
			id: "pg", kind: "postgres", admin: os.Getenv("KELVO_TEST_PG_ADMIN"), driver: "pgx",
			setup: []string{
				`DROP SCHEMA IF EXISTS sales CASCADE`, `DROP TABLE IF EXISTS public."Order Items", public.late_table CASCADE`, `DROP VIEW IF EXISTS public.v_orders`,
				`CREATE TABLE public."Order Items" (id int)`, `INSERT INTO public."Order Items" VALUES (1),(2)`,
				`CREATE VIEW public.v_orders AS SELECT * FROM public."Order Items"`,
				`CREATE SCHEMA sales`, `CREATE TABLE sales."MixedCase" (id int)`, `INSERT INTO sales."MixedCase" VALUES (7)`,
			},
			allowed: map[string]int64{
				`SELECT * FROM pg."Order Items"`:          2,
				`SELECT * FROM pg.public."Order Items"`:   2,
				`SELECT * FROM pg.v_orders`:               2,
				`SELECT * FROM pg.sales."MixedCase"`:      1,
				`SELECT * FROM PG.SALES."mixedcase"`:      1,
				`SELECT count(*) FROM pg.public.v_orders`: 1,
			},
			late: `CREATE TABLE public.late_table (id int)`, lateQuery: `SELECT * FROM pg.late_table`,
		},
		{
			id: "my", kind: "mysql", admin: os.Getenv("KELVO_TEST_MYSQL_ADMIN"), driver: "mysql",
			setup: []string{
				"DROP VIEW IF EXISTS v_orders", "DROP TABLE IF EXISTS `Order Items`, late_table",
				"CREATE TABLE `Order Items` (id int)", "INSERT INTO `Order Items` VALUES (1),(2)",
				"CREATE VIEW v_orders AS SELECT * FROM `Order Items`",
			},
			allowed: map[string]int64{
				// DuckDB's mysql catalog has no default schema: two-part names
				// do not bind in DuckDB itself, so they need catalog.database.table.
				`SELECT * FROM my.kelvo."Order Items"`: 2,
				`SELECT * FROM my.kelvo.v_orders`:      2,
				`SELECT * FROM MY.KELVO."order items"`: 2,
			},
			late: "CREATE TABLE late_table (id int)", lateQuery: `SELECT * FROM my.kelvo.late_table`,
		},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			admin, err := sql.Open(tc.driver, tc.admin)
			if err != nil {
				t.Fatal(err)
			}
			defer admin.Close()
			for _, s := range tc.setup {
				if _, err := admin.Exec(s); err != nil {
					t.Fatal(s, err)
				}
			}
			envName := "KELVO_TEST_PG_DSN"
			if tc.kind == "mysql" {
				envName = "KELVO_TEST_MYSQL_DSN"
			}
			e, err := New(catalog.Config{ExtensionDirectory: ext, Sources: []catalog.Source{{ID: tc.id, Type: tc.kind, DSNEnv: envName}}}, liveLimits())
			if err != nil {
				t.Fatal(err)
			}
			run := func(q string) (int64, error) {
				sink := new(captureSink)
				_, err := e.Execute(context.Background(), query.Request{Sources: []string{tc.id}, SQL: q}, sink)
				return sink.rows, err
			}
			for q, want := range tc.allowed {
				if rows, err := run(q); err != nil || rows != want {
					t.Errorf("%s: rows=%d err=%v", q, rows, err)
				}
			}
			for _, q := range []string{
				"SELECT * FROM " + tc.id + ".nosuch", "SELECT * FROM my.v_orders", "SELECT * FROM " + tc.id + ".csv", "SELECT * FROM " + tc.id + `."d/../x.csv"`, `SELECT * FROM "Order Items"`,
			} {
				_, err := run(q)
				if err == nil || !strings.Contains(err.Error(), "not found or not permitted") {
					t.Errorf("unresolved %s: %v", q, err)
				}
			}
			if _, err := admin.Exec(tc.late); err != nil {
				t.Fatal(err)
			}
			if rows, err := run(tc.lateQuery); err != nil || rows != 0 {
				t.Errorf("table created after a previous attach: rows=%d err=%v", rows, err)
			}
		})
	}
}

// Allowlist build time against a large live catalog (500 tables per DB).
func TestLegacyAttachLargeCatalogTimingLive(t *testing.T) {
	if os.Getenv("KELVO_TEST_LEGACY_ATTACH") != "1" {
		t.Skip("set KELVO_TEST_LEGACY_ATTACH=1")
	}
	ext := os.Getenv("KELVO_TEST_EXTENSIONS")
	for _, tc := range []struct{ id, kind, admin, driver, env, table string }{
		{"pg", "postgres", os.Getenv("KELVO_TEST_PG_ADMIN"), "pgx", "KELVO_TEST_PG_DSN", "public.\"bulk_%d\""},
		{"my", "mysql", os.Getenv("KELVO_TEST_MYSQL_ADMIN"), "mysql", "KELVO_TEST_MYSQL_DSN", "bulk_%d"},
	} {
		admin, err := sql.Open(tc.driver, tc.admin)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 500; i++ {
			if _, err := admin.Exec("CREATE TABLE IF NOT EXISTS " + fmt.Sprintf(tc.table, i) + " (id int)"); err != nil {
				t.Fatal(err)
			}
		}
		_ = admin.Close()
		e, err := New(catalog.Config{ExtensionDirectory: ext, Sources: []catalog.Source{{ID: tc.id, Type: tc.kind, DSNEnv: tc.env}}}, liveLimits())
		if err != nil {
			t.Fatal(err)
		}
		target := tc.id + ".bulk_499"
		if tc.kind == "mysql" {
			target = "my.kelvo.bulk_499"
		}
		for _, q := range []string{"SELECT 1", "SELECT count(*) FROM " + target} {
			var best time.Duration
			for i := 0; i < 3; i++ {
				start := time.Now()
				stats, err := e.Execute(context.Background(), query.Request{Sources: []string{tc.id}, SQL: q}, new(captureSink))
				if err != nil {
					t.Fatal(q, err)
				}
				if d := time.Since(start); best == 0 || d < best {
					best = d
				}
				_ = stats
			}
			t.Logf("%s %q: best of 3 = %s (prepare incl. allowlist build)", tc.kind, q, best)
		}
	}
}

func liveLimits() query.Limits {
	l := query.DefaultLimits()
	l.Timeout = 60 * time.Second
	return l
}

// Isolates the allowlist build (loadRelationIndex) on 500-table attachments.
func TestLegacyAttachAllowlistBuildTimeLive(t *testing.T) {
	if os.Getenv("KELVO_TEST_LEGACY_ATTACH") != "1" {
		t.Skip("set KELVO_TEST_LEGACY_ATTACH=1")
	}
	ext := os.Getenv("KELVO_TEST_EXTENSIONS")
	for _, tc := range []struct{ kind, file, dsnEnv string }{
		{"postgres", "postgres_scanner", "KELVO_TEST_PG_DSN"},
		{"mysql", "mysql_scanner", "KELVO_TEST_MYSQL_DSN"},
	} {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{"LOAD '" + ext + "/" + tc.file + ".duckdb_extension'", "ATTACH '" + quoteLiteral(os.Getenv(tc.dsnEnv)) + "' AS src (TYPE " + tc.kind + ", READ_ONLY)"} {
			if _, err := conn.ExecContext(context.Background(), q); err != nil {
				t.Fatal(err)
			}
		}
		_ = conn.Raw(func(raw any) error {
			q := raw.(driver.QueryerContext)
			for i := 0; i < 3; i++ {
				start := time.Now()
				index, err := loadRelationIndex(context.Background(), q)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("%s allowlist build #%d: %s for %d relations", tc.kind, i+1, time.Since(start), len(index.relations))
			}
			return nil
		})
		_ = conn.Close()
		_ = db.Close()
	}
}
