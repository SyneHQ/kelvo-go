//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"math/rand/v2"
	"strings"
	"testing"
)

// serialize returns DuckDB's own parse tree for sqlText, or "" on a parse error.
func serialize(t *testing.T, db *sql.DB, sqlText string) string {
	var tree string
	if err := db.QueryRow("SELECT CAST(json_serialize_sql('" + quoteLiteral(sqlText) + "') AS VARCHAR)").Scan(&tree); err != nil {
		return ""
	}
	if strings.Contains(tree, `"error":true`) {
		return ""
	}
	return tree
}

// referencesDeniedByParser reports whether DuckDB's tree contains a denied
// function call or a base table that is not a plain identifier.
func referencesDeniedByParser(tree string) bool {
	var node any
	if json.Unmarshal([]byte(tree), &node) != nil {
		return false
	}
	found := false
	var walk func(any)
	walk = func(v any) {
		switch tv := v.(type) {
		case map[string]any:
			if kind, _ := tv["type"].(string); kind == "BASE_TABLE" {
				if name, _ := tv["table_name"].(string); checkBaseTable(stringOf(tv["schema_name"]), name) != nil {
					found = true
				}
			}
			if name, ok := tv["function_name"].(string); ok && deniedFunction(name) {
				found = true
			}
			for _, c := range tv {
				walk(c)
			}
		case []any:
			for _, c := range tv {
				walk(c)
			}
		}
	}
	walk(node)
	return found
}

func stringOf(v any) string { s, _ := v.(string); return s }

// TestRelationGuardDifferentialFuzz mixes quotes, comments, carriage returns,
// dollar tags, file paths and denied names. Whenever DuckDB's parse tree
// references a denied function or a path/glob base table, walkPlan must reject
// the same input. The parser is the oracle; the guard never depends on a
// hand-written lexer agreeing with it.
func TestRelationGuardDifferentialFuzz(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fragments := []string{"SELECT ", "* ", "FROM ", "--", "\r", "\n", "'", "''", `"`, "$$", "$é$", "$t$", "$日$", "/*", "*/",
		"'/etc/hosts'", "'/tmp/kelvo_1.csv'", `"/tmp/x.csv"`, "'*.csv'", "read_csv('/x')", "glob('*')", "READ_PARQUET('x')",
		`"read_text"('x')`, "range(3)", "1", ",", "(", ")", "AS x ", `E'\\'`, ";", " ", "query('SELECT 1')", "sales",
		"information_schema.views", "duckdb_tables()", "enable_profiling()", "x", "re", "ad_csv('x')", "events"}
	rng := rand.New(rand.NewPCG(7, 11))
	denials := 0
	err = conn.Raw(func(raw any) error {
		q := raw.(driver.QueryerContext)
		for i := 0; i < 5000; i++ {
			var b strings.Builder
			b.WriteString("SELECT ")
			for n := 1 + rng.IntN(10); n > 0; n-- {
				b.WriteString(fragments[rng.IntN(len(fragments))])
			}
			candidate := b.String()
			tree := serialize(t, db, candidate)
			if tree == "" {
				continue
			}
			if !referencesDeniedByParser(tree) {
				continue
			}
			denials++
			// enforceRelationAllowlist must reject exactly these.
			if err := enforceRelationAllowlist(context.Background(), q, candidate); err == nil {
				t.Fatalf("guard accepted %q, whose parse tree references a denied relation/function", candidate)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if denials == 0 {
		t.Fatal("fuzz never produced a denied reference; the oracle is not exercised")
	}
	t.Logf("%d denied references, all rejected", denials)
}
