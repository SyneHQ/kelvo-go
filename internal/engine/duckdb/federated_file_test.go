//go:build duckdb_arrow

package duckdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func federatedFileEngine(t *testing.T, kind, content string) *Engine {
	t.Helper()
	dir := t.TempDir()
	orders := filepath.Join(dir, "orders.csv")
	upload := filepath.Join(dir, "upload."+kind)
	if os.WriteFile(orders, []byte("id,name\n1,alpha\n2,beta\n"), 0o600) != nil || os.WriteFile(upload, []byte(content), 0o600) != nil {
		t.Fatal("fixture write failed")
	}
	e, err := New(catalog.Config{Sources: []catalog.Source{
		{ID: "orders", Type: "csv", Path: orders},
		{ID: "files", Type: kind, Path: upload, FederatedFile: true},
	}}, limits())
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func federatedRows(t *testing.T, e *Engine, sql string) []string {
	t.Helper()
	sink := &captureSink{}
	if _, err := e.Execute(context.Background(), query.Request{Mode: "federated", Sources: []string{"orders", "files"}, SQL: sql}, sink); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return sink.values
}

func TestFederatedCSVFileJoinsAnotherSourceWithTypedColumns(t *testing.T) {
	e := federatedFileEngine(t, "csv", "id,amount\n1,10\n2,25\n")
	got := federatedRows(t, e, "SELECT o.name || ':' || CAST(f.amount AS VARCHAR) FROM orders o JOIN files.data f ON o.id = f.id WHERE f.amount = 10")
	if len(got) != 1 || got[0] != "alpha:10" {
		t.Fatalf("join returned %v", got)
	}
	if got := federatedRows(t, e, "SELECT typeof(amount) FROM files.data LIMIT 1"); len(got) != 1 || got[0] != "BIGINT" {
		t.Fatalf("CSV column was not auto-typed: %v", got)
	}
	// The file is only "<alias>.data"; the bare alias is not a relation.
	sink := &captureSink{}
	if _, err := e.Execute(context.Background(), query.Request{Mode: "federated", Sources: []string{"orders", "files"}, SQL: "SELECT * FROM files"}, sink); err == nil {
		t.Fatal("bare file alias resolved")
	}
}

func TestFederatedJSONFilesAttach(t *testing.T) {
	for kind, content := range map[string]string{
		"json":  `[{"id":1,"tag":"x"},{"id":2,"tag":"y"}]`,
		"jsonl": "{\"id\":1,\"tag\":\"x\"}\n{\"id\":2,\"tag\":\"y\"}\n",
	} {
		e := federatedFileEngine(t, kind, content)
		got := federatedRows(t, e, "SELECT o.name || ':' || json_extract_string(f.document, '$.tag') FROM orders o JOIN files.data f ON o.id = CAST(json_extract(f.document, '$.id') AS INTEGER) ORDER BY o.id")
		if len(got) != 2 || got[0] != "alpha:x" || got[1] != "beta:y" {
			t.Fatalf("%s join returned %v", kind, got)
		}
	}
}

func TestFederatedFileRejectsUnsafeShapes(t *testing.T) {
	for name, source := range map[string]catalog.Source{
		"no path":  {ID: "files", Type: "csv", FederatedFile: true},
		"sqlite":   {ID: "files", Type: "sqlite", Path: "/tmp/x", FederatedFile: true},
		"options":  {ID: "files", Type: "parquet", Path: "/tmp/x", FederatedFile: true, Options: map[string]string{"a": "b"}},
		"with dsn": {ID: "files", Type: "csv", Path: "/tmp/x", FederatedFile: true, DSNEnv: "X"},
	} {
		if attachFederatedFile(context.Background(), nil, source) == nil {
			t.Fatal("unsafe federated file accepted:", name)
		}
	}
}
