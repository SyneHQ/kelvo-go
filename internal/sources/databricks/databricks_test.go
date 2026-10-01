// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package databricks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type capture struct {
	schema *arrow.Schema
	values []int64
	nulls  int
	fail   bool
}

func (s *capture) Schema(v *arrow.Schema) error { s.schema = v; return nil }
func (s *capture) Write(r arrow.RecordBatch) error {
	if s.fail {
		return errors.New("sink disconnected")
	}
	c := r.Column(0).(*array.Int64)
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			s.nulls++
		} else {
			s.values = append(s.values, c.Value(i))
		}
	}
	return nil
}
func setup(t *testing.T, h http.HandlerFunc) *Engine {
	t.Helper()
	server := httptest.NewTLSServer(h)
	t.Cleanup(server.Close)
	t.Setenv("DB_TEST_URL", server.URL)
	t.Setenv("DB_TEST_TOKEN", "secret")
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "db", Type: "databricks", URLEnv: "DB_TEST_URL", TokenEnv: "DB_TEST_TOKEN", Options: map[string]string{"warehouse_id": "warehouse-1"}}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e.client.HTTP.Transport = server.Client().Transport
	t.Cleanup(func() { _ = e.Close() })
	return e
}
func successful(rows, chunks int, truncated bool, result string) string {
	return fmt.Sprintf(`{"statement_id":"test-id","status":{"state":"SUCCEEDED"},"manifest":{"format":"JSON_ARRAY","schema":{"columns":[{"name":"id","type_name":"BIGINT"}]},"total_row_count":%d,"total_chunk_count":%d,"truncated":%t},"result":%s}`, rows, chunks, truncated, result)
}
func TestStatementPollingAndChunks(t *testing.T) {
	var requests atomic.Int32
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case endpoint:
			var body map[string]any
			if json.NewDecoder(r.Body).Decode(&body) != nil || body["format"] != "JSON_ARRAY" || body["disposition"] != "INLINE" || r.Method != "POST" {
				t.Error("wrong submission")
			}
			fmt.Fprint(w, `{"statement_id":"test-id","status":{"state":"PENDING"}}`)
		case endpoint + "/test-id":
			fmt.Fprint(w, successful(3, 2, false, `{"chunk_index":0,"row_offset":0,"row_count":2,"data_array":[["9223372036854775807"],[null]],"next_chunk_internal_link":"/api/2.0/sql/statements/test-id/result/chunks/1"}`))
		case endpoint + "/test-id/result/chunks/1":
			fmt.Fprint(w, `{"chunk_index":1,"row_offset":2,"row_count":1,"data_array":[["-1"]]}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(404)
		}
	})
	sink := &capture{}
	stats, err := e.Execute(context.Background(), query.Request{SQL: "SELECT id FROM example", Mode: "native", ConnectionID: "db"}, sink)
	if err != nil || stats.Rows != 3 || sink.nulls != 1 || len(sink.values) != 2 || sink.values[0] != 9223372036854775807 || sink.values[1] != -1 || requests.Load() != 3 {
		t.Fatalf("stats=%+v values=%v nulls=%d requests=%d err=%v", stats, sink.values, sink.nulls, requests.Load(), err)
	}
}
func TestStatementRejectsTruncationPaginationAndFailure(t *testing.T) {
	for _, test := range []struct{ name, response string }{{"truncated", successful(1, 1, true, `{"row_count":1,"data_array":[["1"]]}`)}, {"missing", successful(2, 1, false, `{"row_count":1,"data_array":[["1"]]}`)}, {"external", successful(2, 2, false, `{"row_count":1,"data_array":[["1"]],"next_chunk_internal_link":"https://evil.invalid/"}`)}, {"failed", `{"statement_id":"test-id","status":{"state":"FAILED","error":{"message":"secret"}}}`}, {"unsupported", strings.Replace(successful(1, 1, false, `{"row_count":1,"data_array":[["1"]]}`), "BIGINT", "MAP", 1)}} {
		t.Run(test.name, func(t *testing.T) {
			var cancelled atomic.Bool
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/cancel") {
					cancelled.Store(true)
					fmt.Fprint(w, `{}`)
					return
				}
				if r.URL.Path != endpoint {
					t.Error("invalid followup")
				}
				fmt.Fprint(w, test.response)
			})
			_, err := e.Execute(context.Background(), query.Request{SQL: "SELECT 1", Mode: "native", ConnectionID: "db"}, &capture{})
			if err == nil || strings.Contains(err.Error(), "secret") || !cancelled.Load() {
				t.Fatalf("unsafe result err=%v cancelled=%v", err, cancelled.Load())
			}
		})
	}
}
func TestStatementCancellationAndReadOnly(t *testing.T) {
	var cancelCalls atomic.Int32
	var calls atomic.Int32
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			cancelCalls.Add(1)
			fmt.Fprint(w, `{}`)
			return
		}
		fmt.Fprint(w, `{"statement_id":"test-id","status":{"state":"RUNNING"}}`)
	})
	e.limits.Timeout = 30 * time.Millisecond
	_, err := e.Execute(context.Background(), query.Request{SQL: "SELECT 1", Mode: "native", ConnectionID: "db"}, &capture{})
	if !errors.Is(err, context.DeadlineExceeded) || cancelCalls.Load() != 1 {
		t.Fatalf("missing cancellation: %v", err)
	}
	before := calls.Load()
	for _, sql := range []string{"DELETE FROM t", "SELECT 1; SELECT 2"} {
		_, err = e.Execute(context.Background(), query.Request{SQL: sql, Mode: "native", ConnectionID: "db"}, &capture{})
		if err == nil {
			t.Fatal("unsafe SQL accepted")
		}
	}
	if calls.Load() != before {
		t.Fatal("unsafe SQL reached source")
	}
}
