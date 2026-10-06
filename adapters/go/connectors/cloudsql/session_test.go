// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cloudsql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Protocol fixtures exercise actual TLS/HTTP without claiming vendor-account
// acceptance. The public D1 resolver continues to require api.cloudflare.com.
func fixtureSession(t *testing.T, engine string, handler http.HandlerFunc) *Session {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("missing private token")
			w.WriteHeader(403)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	s := &Session{source: catalog.Source{ID: "operation_source", Type: engine, Options: map[string]string{}}, credentials: cloudapi.Credentials{URL: server.URL, Token: "fixture-token", TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}, database: "analytics", memoryMB: 64}
	if engine == "d1" {
		s.database = "01234567-89ab-cdef-0123-456789abcdef"
		s.source.Options = map[string]string{"account_id": strings.Repeat("a", 32), "database_id": s.database}
	} else {
		s.source.Options = map[string]string{"warehouse_id": "warehouse-1", "catalog": "analytics", "schema": "public"}
		s.schema = "public"
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

type intSink struct {
	schema  *arrow.Schema
	values  []int64
	batches int
}

func (s *intSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *intSink) Write(batch arrow.RecordBatch) error {
	s.batches++
	col, ok := batch.Column(0).(*array.Int64)
	if !ok {
		return adapter.ErrInvalid
	}
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			return adapter.ErrInvalid
		}
		s.values = append(s.values, col.Value(i))
	}
	return nil
}

func TestCloudSQLQueriesPreserveArrowPrecisionAndChunkBounds(t *testing.T) {
	for _, engine := range []string{"d1", "databricks"} {
		t.Run(engine, func(t *testing.T) {
			var submits atomic.Int32
			s := fixtureSession(t, engine, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if engine == "d1" {
					if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/raw") {
						t.Error("unexpected D1 path")
						w.WriteHeader(400)
						return
					}
					submits.Add(1)
					fmt.Fprint(w, `{"success":true,"result":[{"success":true,"results":{"columns":["id"],"rows":[[1],[9007199254740993]]}}]}`)
					return
				}
				switch r.URL.Path {
				case "/api/2.0/sql/statements":
					submits.Add(1)
					var body map[string]any
					if json.NewDecoder(r.Body).Decode(&body) != nil || body["warehouse_id"] != "warehouse-1" || body["catalog"] != "analytics" || body["schema"] != "public" {
						t.Error("namespace lost")
					}
					fmt.Fprint(w, `{"statement_id":"query-1","status":{"state":"RUNNING"}}`)
				case "/api/2.0/sql/statements/query-1":
					fmt.Fprint(w, `{"statement_id":"query-1","status":{"state":"SUCCEEDED"},"manifest":{"format":"JSON_ARRAY","schema":{"columns":[{"name":"id","type_name":"BIGINT"}]},"total_row_count":2,"total_chunk_count":2},"result":{"chunk_index":0,"row_offset":0,"row_count":1,"data_array":[["1"]],"next_chunk_internal_link":"/api/2.0/sql/statements/query-1/result/chunks/1"}}`)
				case "/api/2.0/sql/statements/query-1/result/chunks/1":
					fmt.Fprint(w, `{"chunk_index":1,"row_offset":1,"row_count":1,"data_array":[["9007199254740993"]]}`)
				default:
					t.Error("unexpected Databricks path")
					w.WriteHeader(400)
				}
			})
			sink := &intSink{}
			stats, err := s.Query(context.Background(), adapter.Query{Statement: "SELECT id FROM events", MaxRows: 10, MaxBytes: 64 << 10, BatchRows: 1}, sink)
			if err != nil || stats.Rows != 2 || len(sink.values) != 2 || sink.values[1] != 9007199254740993 || sink.batches != 2 || submits.Load() != 1 {
				t.Fatal(stats, sink.values, sink.batches, submits.Load(), err)
			}
		})
	}
}

func TestCloudSQLChangesStopOnUnknownWithoutReplay(t *testing.T) {
	for _, engine := range []string{"d1", "databricks"} {
		t.Run(engine, func(t *testing.T) {
			var calls atomic.Int32
			s := fixtureSession(t, engine, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" {
					t.Error("unexpected source request")
					w.WriteHeader(400)
					return
				}
				var body map[string]any
				if json.NewDecoder(r.Body).Decode(&body) != nil {
					t.Error("invalid body")
				}
				count := calls.Add(1)
				if count == 2 {
					fmt.Fprint(w, `{"broken":`)
					return
				}
				if engine == "d1" {
					fmt.Fprint(w, `{"success":true,"result":[{"success":true,"meta":{"changes":2}}]}`)
				} else {
					fmt.Fprint(w, `{"statement_id":"write-1","status":{"state":"SUCCEEDED"}}`)
				}
			})
			change := adapter.Change{Statements: []string{"UPDATE events SET n=1", "UPDATE events SET n=2", "UPDATE events SET n=3"}, Parameters: make([][]operations.Parameter, 3)}
			result, err := s.Execute(context.Background(), change)
			if err == nil || result.Outcome != "unknown" || result.Completed != 1 || result.Attempted != 2 || calls.Load() != 2 {
				t.Fatal(result, calls.Load(), err)
			}
			before := calls.Load()
			change.Transaction = true
			result, err = s.Execute(context.Background(), change)
			if err == nil || result.Attempted != 0 || calls.Load() != before {
				t.Fatal("transaction sent despite unsupported capability")
			}
		})
	}
}

func TestCloudSQLMetadataUsesSelectedNamespace(t *testing.T) {
	for _, engine := range []string{"d1", "databricks"} {
		s := fixtureSession(t, engine, func(http.ResponseWriter, *http.Request) { t.Error("metadata validation sent request") })
		for _, object := range []string{"catalogs", "schemas", "tables", "columns"} {
			statement, err := s.metadataQuery(operations.MetadataSpec{Object: object, Limit: 10})
			if err != nil || !strings.Contains(statement, "LIMIT 10 OFFSET 0") {
				t.Fatal(engine, object, statement, err)
			}
		}
		if _, err := s.metadataQuery(operations.MetadataSpec{Object: "tables", Limit: 10, Target: operations.ObjectRef{Catalog: "other"}}); err == nil {
			t.Fatal("cross-catalog metadata accepted")
		}
		if _, err := s.metadataQuery(operations.MetadataSpec{Object: "columns", Limit: 10, Cursor: "0 UNION SELECT 1"}); err == nil {
			t.Fatal("cursor SQL accepted")
		}
		if err := Capabilities(engine).Validate(); err != nil {
			t.Fatal(err)
		}
	}
}
