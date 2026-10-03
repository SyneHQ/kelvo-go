//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestExecuteMultipartParquetJoinAndAllowlist(t *testing.T) {
	dir := t.TempDir()
	parts := []string{filepath.Join(dir, "part-1.parquet"), filepath.Join(dir, "part'2.parquet")}
	for _, path := range parts {
		createFileSource(t, path, "parquet")
	}
	// A sibling file must never enter the explicit path list or directory grants.
	private := filepath.Join(dir, "private.parquet")
	createFileSource(t, private, "parquet")
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "data", Type: "parquet", ParquetPaths: parts}, {ID: "other", Type: "parquet", Path: parts[0]}}}, limits())
	if err != nil {
		t.Fatal(err)
	}
	sink := new(captureSink)
	stats, err := e.Execute(context.Background(), query.Request{Sources: []string{"data", "other"}, SQL: "WITH joined AS (SELECT d.name,d.id FROM data d JOIN other o USING (id)) SELECT name,CAST(id AS DECIMAL(10,2)),CAST(id AS BIGINT) FROM joined ORDER BY id"}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 4 || sink.nulls != 2 || len(sink.values) != 2 {
		t.Fatalf("multipart join lost values: stats=%+v sink=%+v", stats, sink)
	}
	_, err = e.Execute(context.Background(), query.Request{Sources: []string{"data"}, SQL: "SELECT * FROM read_parquet('" + quoteLiteral(private) + "')"}, new(captureSink))
	if err == nil {
		t.Fatal("query accessed unregistered sibling parquet")
	}
}
