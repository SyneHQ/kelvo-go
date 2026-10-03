//go:build duckdb_arrow

package duckdb

import (
	"bytes"
	"context"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func multipartObjectSource() catalog.Source {
	base := "http://127.0.0.1:32123/" + strings.Repeat("a", 64) + "/snapshot/part-"
	return catalog.Source{ID: "snapshot", Type: "parquet", Ranges: []catalog.ObjectRange{{URL: base + "0000", Bytes: 100}, {URL: base + "0001", Bytes: 100}}}
}
func TestMultipartObjectExactSetupAndNoNetworkBypass(t *testing.T) {
	source := multipartObjectSource()
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "httpfs.duckdb_extension"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	exec := new(objectRecordingExecutor)
	if err := attachSources(context.Background(), exec, []catalog.Source{source}, directory, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(exec.statements, "\n")
	if strings.Count(joined, "SET force_download = false") != 1 || !strings.Contains(joined, "read_parquet(['"+source.Ranges[0].URL+"','"+source.Ranges[1].URL+"'])") {
		t.Fatalf("multipart setup wrong: %s", joined)
	}
	exec = new(objectRecordingExecutor)
	if err := lockSourceAccess(context.Background(), exec, []catalog.Source{source}, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(exec.statements, "\n")
	for _, r := range source.Ranges {
		if !strings.Contains(joined, "'"+r.URL+"'") {
			t.Fatal("leaf missing from allowlist")
		}
	}
	for _, kind := range []string{"postgres", "mysql"} {
		if err := lockSourceAccess(context.Background(), new(objectRecordingExecutor), []catalog.Source{source, {ID: "db", Type: kind}}, t.TempDir()); err == nil {
			t.Fatal("legacy network extension bypass")
		}
	}
}
func TestPinnedMultipartObjectRangeQuery(t *testing.T) {
	extensions := os.Getenv("KELVO_OBJECT_EXTENSION_DIRECTORY")
	if extensions == "" {
		t.Skip("set KELVO_OBJECT_EXTENSION_DIRECTORY to use VM provisioned signed HTTPFS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "part.parquet")
	createFileSource(t, path, "parquet")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	base := "/" + strings.Repeat("a", 64) + "/snapshot/part-"
	var gets, bad atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.URL.Path != base+"0000" && r.URL.Path != base+"0001") || r.URL.RawQuery != "" {
			bad.Add(1)
			http.Error(w, "not found", 404)
			return
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("X-Amz-Security-Token") != "" {
			bad.Add(1)
			http.Error(w, "credentials forbidden", 403)
			return
		}
		if r.Method == http.MethodGet {
			gets.Add(1)
			if !strings.HasPrefix(r.Header.Get("Range"), "bytes=") || strings.Contains(r.Header.Get("Range"), ",") {
				bad.Add(1)
				http.Error(w, "range required", 416)
				return
			}
		}
		w.Header().Set("ETag", `"fixture-version"`)
		http.ServeContent(w, r, "part", time.Unix(1700000000, 0), bytes.NewReader(data))
	}))
	defer server.Close()
	source := multipartObjectSource()
	for i := range source.Ranges {
		source.Ranges[i].URL = server.URL + base + []string{"0000", "0001"}[i]
		source.Ranges[i].Bytes = int64(len(data))
	}
	resourceLimits := query.DefaultLimits()
	resourceLimits.Threads = 1
	e, err := New(catalog.Config{ExtensionDirectory: extensions, Sources: []catalog.Source{source}}, resourceLimits)
	if err != nil {
		t.Fatal(err)
	}
	sink := new(captureSink)
	stats, err := e.Execute(ctx, query.Request{Sources: []string{"snapshot"}, SQL: "WITH all_parts AS (SELECT * FROM snapshot) SELECT name,CAST(id AS DECIMAL(10,2)),CAST(id AS BIGINT) FROM all_parts ORDER BY id"}, sink)
	if err != nil || stats.Rows != 4 || sink.nulls != 2 || len(sink.values) != 2 || gets.Load() == 0 || bad.Load() != 0 {
		t.Fatalf("multipart range query failed: stats=%+v nulls=%d values=%v gets=%d bad=%d err=%v", stats, sink.nulls, sink.values, gets.Load(), bad.Load(), err)
	}
}
