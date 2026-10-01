// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package bigquery

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
	schema  *arrow.Schema
	records []arrow.RecordBatch
}

func (s *capture) Schema(v *arrow.Schema) error { s.schema = v; return nil }
func (s *capture) Write(r arrow.RecordBatch) error {
	r.Retain()
	s.records = append(s.records, r)
	return nil
}
func (s *capture) close() {
	for _, r := range s.records {
		r.Release()
	}
}

func setup(t *testing.T, h http.HandlerFunc) *Engine {
	t.Helper()
	server := httptest.NewTLSServer(h)
	t.Cleanup(server.Close)
	t.Setenv("BQ_TEST_URL", server.URL)
	t.Setenv("BQ_TEST_TOKEN", "test-only")
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "bq", Type: "bigquery", URLEnv: "BQ_TEST_URL", TokenEnv: "BQ_TEST_TOKEN", Options: map[string]string{"project": "test-project", "location": "us", "maximum_bytes_billed": "1000000"}}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e.client.HTTP.Transport = server.Client().Transport
	t.Cleanup(func() { e.Close() })
	return e
}
func request() query.Request {
	return query.Request{Mode: "native", ConnectionID: "bq", SQL: "SELECT * FROM t"}
}

func TestJobPaginationExactTypes(t *testing.T) {
	var ref jobRef
	var gets int
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-only" {
			t.Error("missing source credential")
		}
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/jobs") {
			var body struct {
				Ref    jobRef `json:"jobReference"`
				Config struct {
					Query struct {
						SQL    string `json:"query"`
						Legacy bool   `json:"useLegacySql"`
						Billed string `json:"maximumBytesBilled"`
					} `json:"query"`
				} `json:"configuration"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.Config.Query.Legacy || body.Config.Query.Billed != "1000000" || body.Config.Query.SQL != request().SQL {
				t.Error("invalid job submission")
			}
			ref = body.Ref
			json.NewEncoder(w).Encode(map[string]any{"jobReference": ref})
			return
		}
		if r.Method != "GET" || r.URL.Query().Get("formatOptions.useInt64Timestamp") != "true" || r.URL.Query().Get("location") != "us" {
			t.Error("invalid result request")
		}
		gets++
		b := map[string]any{"jobReference": ref, "jobComplete": true, "totalRows": "2"}
		if gets == 1 {
			b["jobComplete"] = false
			delete(b, "totalRows")
		}
		if gets == 2 {
			b["schema"] = map[string]any{"fields": []map[string]string{{"name": "id", "type": "INTEGER"}, {"name": "n", "type": "NUMERIC"}, {"name": "ts", "type": "TIMESTAMP"}}}
			b["rows"] = []any{map[string]any{"f": []any{map[string]any{"v": "9223372036854775807"}, map[string]any{"v": "12345678901234567890.123456789"}, map[string]any{"v": "1735689600123456"}}}}
			b["pageToken"] = "next+/="
		}
		if gets == 3 {
			if r.URL.Query().Get("pageToken") != "next+/=" {
				t.Error("page token changed")
			}
			b["rows"] = []any{map[string]any{"f": []any{map[string]any{"v": nil}, map[string]any{"v": nil}, map[string]any{"v": nil}}}}
		}
		json.NewEncoder(w).Encode(b)
	})
	sink := &capture{}
	defer sink.close()
	stats, err := e.Execute(context.Background(), request(), sink)
	if err != nil || stats.Rows != 2 || gets != 3 || len(sink.records) != 1 {
		t.Fatalf("stats=%+v gets=%d err=%v", stats, gets, err)
	}
	r := sink.records[0]
	if r.Column(0).(*array.Int64).Value(0) != 9223372036854775807 || !r.Column(0).IsNull(1) {
		t.Fatal("int64/null loss")
	}
	if r.Column(1).(*array.Decimal128).Value(0).ToString(9) != "12345678901234567890.123456789" {
		t.Fatal("decimal loss")
	}
	if r.Column(2).(*array.Timestamp).Value(0) != 1735689600123456 || sink.schema.Field(2).Type.(*arrow.TimestampType).Unit != arrow.Microsecond {
		t.Fatal("timestamp loss")
	}
}

func TestFailedIncompleteAndChangedResultsCancel(t *testing.T) {
	for _, scenario := range []string{"error", "count", "incomplete", "repeated", "schema", "rowwidth", "oversized", "reference"} {
		t.Run(scenario, func(t *testing.T) {
			var ref jobRef
			var canceled atomic.Bool
			gets := 0
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/cancel") {
					canceled.Store(true)
					fmt.Fprint(w, `{}`)
					return
				}
				if r.Method == "POST" {
					var b struct {
						Ref jobRef `json:"jobReference"`
					}
					json.NewDecoder(r.Body).Decode(&b)
					ref = b.Ref
					json.NewEncoder(w).Encode(map[string]any{"jobReference": ref})
					return
				}
				gets++
				b := map[string]any{"jobReference": ref, "jobComplete": true, "totalRows": "1", "schema": map[string]any{"fields": []map[string]string{{"name": "id", "type": "INTEGER"}}}, "rows": []any{map[string]any{"f": []any{map[string]any{"v": "1"}}}}}
				switch scenario {
				case "error":
					b["errors"] = []any{map[string]string{"message": "private-secret"}}
				case "count":
					b["totalRows"] = "-1"
				case "incomplete":
					b["totalRows"] = "2"
				case "repeated":
					b["schema"] = map[string]any{"fields": []map[string]string{{"name": "x", "type": "INTEGER", "mode": "REPEATED"}}}
				case "schema":
					b["totalRows"] = "2"
					if gets == 1 {
						b["pageToken"] = "next"
					} else {
						b["schema"] = map[string]any{"fields": []map[string]string{{"name": "id", "type": "STRING"}}}
					}
				case "rowwidth":
					b["rows"] = []any{map[string]any{"f": []any{}}}
				case "oversized":
					b["totalRows"] = "1000001"
				case "reference":
					b["jobReference"] = jobRef{Project: ref.Project, ID: "other", Location: ref.Location}
				}
				json.NewEncoder(w).Encode(b)
			})
			sink := &capture{}
			defer sink.close()
			_, err := e.Execute(context.Background(), request(), sink)
			if err == nil || strings.Contains(err.Error(), "private-secret") || !canceled.Load() {
				t.Fatalf("err=%v cancel=%v", err, canceled.Load())
			}
		})
	}
}
func TestCancellationAndInvalidSQL(t *testing.T) {
	var ref jobRef
	var calls, cancels atomic.Int32
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			cancels.Add(1)
			fmt.Fprint(w, `{}`)
			return
		}
		if r.Method == "POST" {
			var b struct {
				Ref jobRef `json:"jobReference"`
			}
			json.NewDecoder(r.Body).Decode(&b)
			ref = b.Ref
			json.NewEncoder(w).Encode(map[string]any{"jobReference": ref})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"jobReference": ref, "jobComplete": false})
	})
	e.limits.Timeout = 50 * time.Millisecond
	_, err := e.Execute(context.Background(), request(), &capture{})
	if !errors.Is(err, context.DeadlineExceeded) || cancels.Load() != 1 {
		t.Fatalf("err=%v cancels=%d", err, cancels.Load())
	}
	before := calls.Load()
	r := request()
	r.SQL = "DELETE FROM t"
	if _, err = e.Execute(context.Background(), r, &capture{}); err == nil || calls.Load() != before {
		t.Fatal("write reached provider")
	}
}
