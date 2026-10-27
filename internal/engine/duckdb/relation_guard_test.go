//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
