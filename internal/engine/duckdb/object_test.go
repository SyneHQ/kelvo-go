//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func objectRangeFixture() catalog.Source {
	rangeRead := &catalog.ObjectRange{URL: "http://127.0.0.1:32123/" + strings.Repeat("a", 64) + "/snapshot", Bytes: 2686290}
	return catalog.Source{ID: "snapshot", Type: "parquet", Path: rangeRead.URL, Range: rangeRead}
}

type objectRecordingExecutor struct {
	statements  []string
	failSetting bool
}

func (e *objectRecordingExecutor) ExecContext(_ context.Context, statement string, _ []driver.NamedValue) (driver.Result, error) {
	e.statements = append(e.statements, statement)
	if e.failSetting && strings.HasPrefix(statement, "SET force_download") {
		return nil, errors.New("private-driver-diagnostic")
	}
	return driver.RowsAffected(0), nil
}

func TestObjectRangeSettingsNeverCreateCloudSecrets(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "httpfs.duckdb_extension"), []byte("recording executor does not load this file"), 0600); err != nil {
		t.Fatal(err)
	}
	executor := new(objectRecordingExecutor)
	if err := prepareObjectRange(context.Background(), executor, objectRangeFixture(), directory, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(executor.statements, "\n")
	for _, setting := range []string{"SET force_download = false", "SET force_download_threshold = 0", "SET auto_fallback_to_full_download = false", "SET unsafe_disable_etag_checks = false", "SET enable_global_s3_configuration = false", "SET merge_http_secret_into_s3_request = false", "SET httpfs_enable_credential_refresh = false"} {
		if !strings.Contains(joined, setting) {
			t.Fatalf("missing range restriction %q", setting)
		}
	}
	for _, denied := range []string{"CREATE", "SECRET", "azure", "credential_chain"} {
		if strings.Contains(joined, denied) {
			t.Fatalf("range setup unexpectedly used %q", denied)
		}
	}
	executor.failSetting = true
	if err := prepareObjectRange(context.Background(), executor, objectRangeFixture(), directory, t.TempDir()); err == nil || strings.Contains(err.Error(), "private-driver-diagnostic") {
		t.Fatal("range configuration driver error was not sanitized")
	}
}

func TestObjectRangeLockdownUsesExactPathsAndRejectsMixedNetworkAccess(t *testing.T) {
	source := objectRangeFixture()
	executor := new(objectRecordingExecutor)
	workspace := t.TempDir()
	if err := lockSourceAccess(context.Background(), executor, []catalog.Source{source}, workspace); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(executor.statements, "\n")
	for _, expected := range []string{"SET allowed_paths = ['" + workspace + "', '" + source.Path + "']", "SET allowed_directories = ['" + workspace + "']", "SET enable_external_access = false", "SET lock_configuration = true"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("missing exact range lockdown %q", expected)
		}
	}
	for _, kind := range []string{"postgres", "mysql"} {
		executor := new(objectRecordingExecutor)
		err := lockSourceAccess(context.Background(), executor, []catalog.Source{source, {ID: "db", Type: kind}}, workspace)
		if err == nil || query.PublicError(err).Code != "UNSUPPORTED" || len(executor.statements) != 0 {
			t.Fatalf("range/network combination weakened lockdown: %v", err)
		}
	}
}

func TestObjectRangesRejectCloudCredentialsAndAmbiguousCapabilities(t *testing.T) {
	for _, mutate := range []func(*catalog.Source){
		func(s *catalog.Source) { s.Path += "-other" },
		func(s *catalog.Source) { s.TokenEnv = "KELVO_SOURCE_OBJECT_READ_SECRET" },
		func(s *catalog.Source) { s.Object = &catalog.ObjectRead{Provider: "s3"} },
		func(s *catalog.Source) {
			s.Range.URL = strings.Replace(s.Range.URL, "127.0.0.1", "storage.example.test", 1)
			s.Path = s.Range.URL
		},
		func(s *catalog.Source) { s.Range.URL += "?token=secret"; s.Path = s.Range.URL },
		func(s *catalog.Source) { s.Range.Bytes = 0 },
		func(s *catalog.Source) { s.Options = map[string]string{"secret": "private"} },
	} {
		invalid := objectRangeFixture()
		mutate(&invalid)
		if validateObjectSource(invalid) == nil {
			t.Fatal("unbounded or credential-bearing source accepted")
		}
	}
	if _, err := New(catalog.Config{Sources: []catalog.Source{{ID: "snapshot", Type: "parquet", Object: &catalog.ObjectRead{Provider: "azure"}}}}, query.DefaultLimits()); err == nil {
		t.Fatal("raw cloud object reached the query engine")
	}
}

type objectResponseCounter struct {
	http.ResponseWriter
	bytes *atomic.Int64
}

func (w objectResponseCounter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytes.Add(int64(n))
	return n, err
}

// Uses the same HTTPFS setup and exact lockdown as Execute. The parent-owned
// bridge is represented by a loopback service that refuses whole-object GETs,
// never redirects, and contains no cloud credentials. Ordinary tests download
// nothing; the signed pinned extension must be provisioned explicitly on the VM.
func TestPinnedObjectRangesUseRangesAndExactAllowlist(t *testing.T) {
	extensions := os.Getenv("KELVO_OBJECT_EXTENSION_DIRECTORY")
	if extensions == "" {
		t.Skip("set KELVO_OBJECT_EXTENSION_DIRECTORY to test the pinned signed HTTPFS extension")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	fixture := filepath.Join(t.TempDir(), "fixture.parquet")
	fixtureDB, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixtureDB.ExecContext(ctx, "COPY (SELECT i::BIGINT AS id, repeat(md5(i::VARCHAR), 8) AS payload FROM range(10000) t(i)) TO '"+quoteLiteral(fixture)+"' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION 'UNCOMPRESSED')")
	fixtureDB.Close()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(fixture)
	if err != nil || len(data) < 1<<20 {
		t.Fatalf("large Parquet fixture unavailable: %v", err)
	}
	path := "/" + strings.Repeat("a", 64) + "/snapshot"
	var served, getCount, unrestrictedGets, deniedPaths, credentialHeaders atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path || r.URL.RawQuery != "" {
			deniedPaths.Add(1)
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("X-Amz-Security-Token") != "" {
			credentialHeaders.Add(1)
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "read only", http.StatusMethodNotAllowed)
			return
		}
		if r.Method == http.MethodGet {
			getCount.Add(1)
			if !strings.HasPrefix(r.Header.Get("Range"), "bytes=") || strings.Contains(r.Header.Get("Range"), ",") {
				unrestrictedGets.Add(1)
				http.Error(w, "one range required", http.StatusRequestedRangeNotSatisfiable)
				return
			}
		}
		w.Header().Set("ETag", `"fixture-version"`)
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(objectResponseCounter{w, &served}, r, "snapshot", time.Unix(1_700_000_000, 0), bytes.NewReader(data))
	}))
	defer server.Close()
	rangeRead := &catalog.ObjectRange{URL: server.URL + path, Bytes: int64(len(data))}
	source := catalog.Source{ID: "snapshot", Type: "parquet", Range: rangeRead, Path: rangeRead.URL}
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
	workspace := t.TempDir()
	resourceLimits := query.DefaultLimits()
	resourceLimits.Threads = 1
	err = conn.Raw(func(raw any) error {
		if err := configure(ctx, raw, extensions, workspace, resourceLimits); err != nil {
			return err
		}
		if err := attachSources(ctx, raw, []catalog.Source{source}, extensions, workspace); err != nil {
			return err
		}
		return lockSourceAccess(ctx, raw, []catalog.Source{source}, workspace)
	})
	if err != nil {
		t.Fatalf("pinned range setup failed: %v", err)
	}
	var sum int64
	if err := conn.QueryRowContext(ctx, "SELECT CAST(sum(id) AS BIGINT) FROM snapshot WHERE id < 10").Scan(&sum); err != nil || sum != 45 {
		t.Fatalf("range scan did not preserve selected values: sum=%d error=%v", sum, err)
	}
	if getCount.Load() == 0 || unrestrictedGets.Load() != 0 || served.Load() <= 0 || served.Load() >= int64(len(data)) || credentialHeaders.Load() != 0 {
		t.Fatalf("expected anonymous partial ranged reads: GETs=%d unrestricted=%d credential_headers=%d bytes=%d full=%d", getCount.Load(), unrestrictedGets.Load(), credentialHeaders.Load(), served.Load(), len(data))
	}
	t.Logf("ranged GETs=%d, transferred bytes=%d, full object bytes=%d", getCount.Load(), served.Load(), len(data))
	for _, uri := range []string{source.Path + "_other", server.URL + "/" + strings.Repeat("b", 64) + "/snapshot", server.URL + "/" + strings.Repeat("a", 64) + "/*"} {
		// Bypass Kelvo's lexical preflight to verify DuckDB's own allowlist.
		if _, err := conn.ExecContext(ctx, "SELECT * FROM read_parquet('"+quoteLiteral(uri)+"')"); err == nil {
			t.Fatal("unselected range capability was readable")
		}
	}
	if deniedPaths.Load() != 0 {
		t.Fatal("unselected capability reached the bridge")
	}
	var secrets int64
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM duckdb_secrets()").Scan(&secrets); err != nil || secrets != 0 {
		t.Fatalf("range query unexpectedly created a DuckDB secret: count=%d error=%v", secrets, err)
	}
	if _, err := conn.ExecContext(ctx, "SET enable_external_access=true"); err == nil {
		t.Fatal("object range configuration was not locked")
	}
}
