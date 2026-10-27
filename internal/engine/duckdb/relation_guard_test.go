//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// A file path used as a bare table reference reads the file directly, skipping
// the registered source view (and any column projection applied through it).
// No function name is involved, so the capability denylist never sees it.
func TestRejectFilePathAndGlobTableReferences(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(path, []byte("id,secret\n1,alpha\n2,beta\n"), 0600); err != nil {
		t.Fatal(err)
	}
	e := makeEngine(t, path)
	for _, sql := range []string{
		"SELECT * FROM '" + path + "'",
		`SELECT * FROM "` + path + `"`,
		"SELECT * FROM '" + filepath.Join(dir, "*.csv") + "'",
		"SELECT secret FROM read_csv('" + path + "')",
		"WITH x AS (SELECT * FROM '" + path + "') SELECT * FROM x",
		"SELECT * FROM (SELECT * FROM '" + path + "') y",
		"SELECT * FROM 'events' UNION ALL SELECT * FROM '" + path + "'",
	} {
		sink := new(captureSink)
		_, err := e.Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: sql}, sink)
		if err == nil || sink.rows != 0 {
			t.Fatalf("file-path read was not rejected: %q", sql)
		}
		if query.PublicError(err).Code != "PERMISSION_DENIED" {
			t.Fatalf("%q: code %s", sql, query.PublicError(err).Code)
		}
	}
	// The registered relation still works.
	sink := new(captureSink)
	if _, err := e.Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: "SELECT secret FROM events ORDER BY id"}, sink); err != nil || sink.rows != 2 {
		t.Fatalf("registered relation broken: rows=%d err=%v", sink.rows, err)
	}
	// A column literally named like a denied function must not be rejected.
	for _, sql := range []string{"SELECT id AS query FROM events", "SELECT count(*) AS read_count FROM events"} {
		if _, err := e.Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: sql}, new(captureSink)); err != nil {
			t.Fatalf("legitimate column rejected %q: %v", sql, err)
		}
	}
}

// Upstream names on an attached database may need quoting ("Order Items");
// they are allowed when qualified by a request source alias. Path tricks under
// that qualifier never become a file read.
func TestAttachedSourceQuotedNamesAndPathTricks(t *testing.T) {
	dir := t.TempDir()
	csv := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(csv, []byte("id,secret\n1,alpha\n"), 0600); err != nil {
		t.Fatal(err)
	}
	dbFile := filepath.Join(dir, "upstream.duckdb")
	db, err := sql.Open("duckdb", dbFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{`CREATE SCHEMA "public"`, `CREATE TABLE "public"."Order Items" (id INTEGER)`, `INSERT INTO "public"."Order Items" VALUES (1), (2)`,
		`CREATE TABLE "Order-Items" (id INTEGER)`, `INSERT INTO "Order-Items" VALUES (3)`} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(stmt, err)
		}
	}
	_ = db.Close()
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "pg", Type: "duckdb", Path: dbFile}, {ID: "events", Type: "csv", Path: csv}}}, limits())
	if err != nil {
		t.Fatal(err)
	}
	run := func(sql string) (int64, error) {
		sink := new(captureSink)
		_, err := e.Execute(context.Background(), query.Request{Sources: []string{"pg", "events"}, SQL: sql}, sink)
		return sink.rows, err
	}
	for sql, want := range map[string]int64{
		`SELECT * FROM pg."public"."Order Items"`: 2,
		`SELECT * FROM pg.main."Order-Items"`:     1,
		`SELECT * FROM PG."Order-Items"`:          1,
	} {
		if rows, err := run(sql); err != nil || rows != want {
			t.Fatalf("%s: rows=%d err=%v", sql, rows, err)
		}
	}
	for _, sql := range []string{
		`SELECT * FROM '/etc/hosts'`,
		`SELECT * FROM pg."/etc/hosts"`,
		`SELECT * FROM pg.main."` + csv + `"`,
		`SELECT * FROM events."` + csv + `"`,
		`SELECT * FROM "` + csv + `"`,
		`SELECT * FROM unknown_alias."Order Items"`,
	} {
		if rows, err := run(sql); err == nil || rows != 0 {
			t.Fatalf("path trick was not rejected: %s (rows=%d)", sql, rows)
		}
	}
}

func TestDefinitionFunctionsAndPivotFailClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(path, []byte("id,secret\n1,alpha\n"), 0600); err != nil {
		t.Fatal(err)
	}
	e := makeEngine(t, path)
	for _, sql := range []string{
		"SELECT pg_get_viewdef((SELECT oid FROM pg_class WHERE relname='events'))",
		"SELECT pg_catalog.pg_get_viewdef('events')",
		"SELECT pg_get_constraintdef(1)",
		"SELECT * FROM duckdb_table_sample('events')",
		"PIVOT events ON id USING count(*)",
		"SELECT * FROM (PIVOT events ON id USING count(*))",
	} {
		_, err := e.Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: sql}, new(captureSink))
		if err == nil {
			t.Fatalf("not rejected: %s", sql)
		}
	}
}

// Names that do not bind fall back to replacement scans, which DuckDB tries as
// file paths relative to the working directory. Only existing relations pass.
func TestUnresolvedNamesNeverReachReplacementScans(t *testing.T) {
	root := t.TempDir()
	work := filepath.Join(root, "work")
	for _, d := range []string{filepath.Join(work, "pg.d")} {
		if err := os.MkdirAll(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	for path, body := range map[string]string{
		filepath.Join(work, "pg.csv"):            "leak\n1\n",
		filepath.Join(work, "pg.d", "inner.csv"): "leak\n2\n",
		filepath.Join(root, "kp2.csv"):           "leak\n3\n",
		filepath.Join(work, "csv"):               "leak\n4\n",
		filepath.Join(work, "nosuch"):            "leak\n5\n",
	} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	dbFile := filepath.Join(root, "upstream.duckdb")
	db, err := sql.Open("duckdb", dbFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{`CREATE SCHEMA "public"`, `CREATE TABLE "public"."Order Items" (id INTEGER)`, `INSERT INTO "public"."Order Items" VALUES (1), (2)`, `CREATE TABLE things (id INTEGER)`} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(stmt, err)
		}
	}
	_ = db.Close()
	t.Chdir(work)
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "pg", Type: "duckdb", Path: dbFile}}}, limits())
	if err != nil {
		t.Fatal(err)
	}
	run := func(sql string) (int64, error) {
		sink := new(captureSink)
		_, err := e.Execute(context.Background(), query.Request{Sources: []string{"pg"}, SQL: sql}, sink)
		return sink.rows, err
	}
	for _, sql := range []string{
		`SELECT * FROM pg.csv`, `SELECT * FROM pg."csv"`, `SELECT * FROM pg."d/../../kp2.csv"`, `SELECT * FROM pg."*/../../kp2.csv"`,
		`SELECT * FROM csv`, `SELECT * FROM nosuch`, `SELECT * FROM "pg.csv"`, `SELECT * FROM pg.main.csv`,
		`SELECT * FROM (WITH csv AS (SELECT 1 AS a) SELECT * FROM csv) x, csv`,
		`WITH a AS (SELECT * FROM nosuch) SELECT * FROM a`,
	} {
		rows, err := run(sql)
		if err == nil || rows != 0 || !strings.Contains(err.Error(), "relation unavailable") {
			t.Fatalf("unresolved name not denied by the guard: %s (rows=%d err=%v)", sql, rows, err)
		}
	}
	for sql, want := range map[string]int64{
		`SELECT * FROM pg."public"."Order Items"`:                                                 2,
		`SELECT * FROM PG."PUBLIC"."order items"`:                                                 2,
		`SELECT count(*) FROM pg.things`:                                                          1,
		`SELECT count(*) FROM pg.main.THINGS`:                                                     1,
		`WITH csv AS (SELECT 1 AS a) SELECT * FROM csv`:                                           1,
		`WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a) SELECT * FROM b`:                       1,
		`WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) SELECT * FROM r`: 3,
		`SELECT count(*) FROM information_schema.columns`:                                         1,
		`SELECT count(*) FROM pg_catalog.pg_class`:                                                1,
	} {
		if rows, err := run(sql); err != nil || rows != want {
			t.Fatalf("existing relation rejected: %s rows=%d err=%v", sql, rows, err)
		}
	}
}
