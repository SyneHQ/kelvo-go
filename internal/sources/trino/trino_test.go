// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package trino

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	cancel  context.CancelFunc
	fail    bool
}

func (s *capture) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *capture) Write(record arrow.RecordBatch) error {
	if s.fail {
		return query.NewError("RESOURCE_EXHAUSTED", "sink rejected batch")
	}
	record.Retain()
	s.records = append(s.records, record)
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}
func (s *capture) Close() {
	for _, record := range s.records {
		record.Release()
	}
}

func setup(t *testing.T, kind string, handler http.HandlerFunc) *Engine {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	t.Setenv("KELVO_SOURCE_TRINO_URL", server.URL)
	t.Setenv("KELVO_SOURCE_TRINO_TOKEN", "private-token")
	t.Setenv("KELVO_SOURCE_TRINO_USER", "reader")
	source := catalog.Source{ID: "engine", Type: kind, URLEnv: "KELVO_SOURCE_TRINO_URL", TokenEnv: "KELVO_SOURCE_TRINO_TOKEN", UsernameEnv: "KELVO_SOURCE_TRINO_USER", Options: map[string]string{"catalog": "lake", "schema": "analytics"}}
	e, err := New(catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e.client.HTTP.Transport = server.Client().Transport
	t.Cleanup(func() { _ = e.Close() })
	return e
}
func request() query.Request {
	return query.Request{Mode: "native", ConnectionID: "engine", SQL: "SELECT * FROM events"}
}
func checkCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || query.PublicError(err).Code != code {
		t.Fatalf("got %v, want %s", err, code)
	}
}
func nextURL(r *http.Request, kind string, token int) string {
	if kind == "presto" {
		return fmt.Sprintf("https://%s/v1/statement/executing/query_1/%d?slug=slug_1", r.Host, token)
	}
	return fmt.Sprintf("https://%s/v1/statement/executing/query_1/slug_1/%d", r.Host, token)
}
func page(w http.ResponseWriter, data any) { _ = json.NewEncoder(w).Encode(data) }

func TestProtocolHeadersPaginationAndExactTypes(t *testing.T) {
	for _, kind := range []string{"trino", "presto"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int32
			e := setup(t, kind, func(w http.ResponseWriter, r *http.Request) {
				index := calls.Add(1)
				prefix := "X-Trino-"
				other := "X-Presto-"
				if kind == "presto" {
					prefix, other = other, prefix
				}
				if r.Header.Get("Authorization") != "Bearer private-token" || r.Header.Get(prefix+"User") != "reader" || r.Header.Get(prefix+"Catalog") != "lake" || r.Header.Get(prefix+"Schema") != "analytics" || r.Header.Get(other+"User") != "" {
					t.Error("wrong protocol or credentials")
				}
				switch index {
				case 1:
					body, _ := io.ReadAll(r.Body)
					if r.Method != "POST" || r.URL.Path != "/v1/statement" || string(body) != request().SQL || r.Header.Get("Content-Type") != "text/plain; charset=utf-8" {
						t.Error("wrong statement submission")
					}
					page(w, map[string]any{"id": "query_1", "nextUri": nextURL(r, kind, 1)})
				case 2:
					if r.Method != "GET" {
						t.Error("wrong page method")
					}
					page(w, map[string]any{"id": "query_1", "nextUri": nextURL(r, kind, 2), "columns": []column{{"large", "bigint"}, {"small", "tinyint"}, {"amount", "decimal(30,10)"}, {"day", "date"}, {"at", "timestamp(6)"}, {"bytes", "varbinary"}, {"yes", "boolean"}, {"real_value", "real"}, {"empty", "unknown"}}, "data": [][]any{{json.Number("9007199254740993"), json.Number("-128"), "12345678901234567890.0123456789", "1969-12-31", "2026-10-01 12:30:00.123456", "AP8=", true, json.Number("1.5"), nil}}})
				case 3:
					page(w, map[string]any{"id": "query_1", "data": [][]any{{nil, nil, nil, nil, nil, nil, nil, nil, nil}}})
				default:
					t.Error("unexpected extra request")
					w.WriteHeader(500)
				}
			})
			sink := new(capture)
			defer sink.Close()
			stats, err := e.Execute(context.Background(), request(), sink)
			if err != nil || stats.Rows != 2 || stats.Batches != 1 || stats.Backend != kind || !stats.EngineStreaming || stats.WireBytes == 0 || calls.Load() != 3 {
				t.Fatalf("stats=%+v calls=%d err=%v", stats, calls.Load(), err)
			}
			r := sink.records[0]
			if r.Column(0).(*array.Int64).Value(0) != 9007199254740993 || r.Column(1).(*array.Int8).Value(0) != -128 {
				t.Fatal("integer precision/width changed")
			}
			if r.Column(2).(*array.Decimal128).Value(0).ToString(10) != "12345678901234567890.0123456789" {
				t.Fatal("decimal changed")
			}
			if r.Column(3).(*array.Date32).Value(0) != -1 {
				t.Fatal("pre-epoch date changed")
			}
			stamp := r.Column(4).(*array.Timestamp)
			if stamp.DataType().(*arrow.TimestampType).TimeZone != "" || stamp.Value(0).ToTime(arrow.Microsecond).Format("2006-01-02 15:04:05.000000") != "2026-10-01 12:30:00.123456" {
				t.Fatal("timestamp changed")
			}
			if string(r.Column(5).(*array.Binary).Value(0)) != string([]byte{0, 255}) || !r.Column(6).(*array.Boolean).Value(0) || r.Column(7).(*array.Float32).Value(0) != 1.5 || r.Column(8).NullN() != r.Column(8).Len() {
				t.Fatal("typed values changed")
			}
			for i := 0; i < int(r.NumCols())-1; i++ {
				if !r.Column(i).IsNull(1) {
					t.Fatal("null changed")
				}
			}
		})
	}
}

func TestEmptyResultRetainsSchema(t *testing.T) {
	e := setup(t, "trino", func(w http.ResponseWriter, r *http.Request) {
		page(w, map[string]any{"id": "query_1", "columns": []column{{"value", "integer"}}, "data": [][]any{}})
	})
	sink := new(capture)
	defer sink.Close()
	stats, err := e.Execute(context.Background(), request(), sink)
	if err != nil || stats.Rows != 0 || sink.schema == nil || len(sink.records) != 0 {
		t.Fatalf("empty result=%+v %v", stats, err)
	}
}

func TestRejectsUnsafePagination(t *testing.T) {
	for _, location := range []string{
		"https://other.invalid/v1/statement/executing/query_1/slug/1", "http://HOST/v1/statement/executing/query_1/slug/1", "https://user@HOST/v1/statement/executing/query_1/slug/1",
		"https://HOST/v1/statement/executing/other_query/slug/1", "https://HOST/v1/query/query_1", "https://HOST/v1/statement/executing/query_1/%2e%2e/1",
		"https://HOST/v1/statement/executing/query_1/slug/1?token=private-token", "https://HOST/v1/statement/executing/query_1/slug/1#fragment", "https://HOST/v1/statement/executing/query_1/slug/-1",
		"/v1/statement/executing/query_1/slug/1", "https://HOST/v1/statement/executing/query_1/slug/9223372036854775808",
	} {
		t.Run(location, func(t *testing.T) {
			var calls atomic.Int32
			e := setup(t, "trino", func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				page(w, map[string]any{"id": "query_1", "nextUri": strings.ReplaceAll(location, "HOST", r.Host)})
			})
			_, err := e.Execute(context.Background(), request(), new(capture))
			checkCode(t, err, "QUERY_FAILED")
			if calls.Load() != 1 || strings.Contains(err.Error(), "private-token") {
				t.Fatal("unsafe location used or exposed")
			}
		})
	}
}

func TestErrorsSchemaChangesAndPaginationCleanup(t *testing.T) {
	for _, test := range []struct {
		name   string
		second map[string]any
	}{
		{"query error", map[string]any{"id": "query_1", "error": map[string]any{"message": "private-token"}}},
		{"mutation", map[string]any{"id": "query_1", "updateType": "INSERT", "updateCount": 1}},
		{"wrong id", map[string]any{"id": "other_query"}},
		{"changed schema", map[string]any{"id": "query_1", "columns": []column{{"value", "varchar"}}}},
		{"wrong width", map[string]any{"id": "query_1", "data": [][]any{{1, 2}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var gets, deletes atomic.Int32
			e := setup(t, "trino", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "DELETE" {
					deletes.Add(1)
					w.WriteHeader(204)
					return
				}
				if r.Method == "POST" {
					page(w, map[string]any{"id": "query_1", "nextUri": nextURL(r, "trino", 1), "columns": []column{{"value", "bigint"}}, "data": [][]any{{1}}})
					return
				}
				gets.Add(1)
				page(w, test.second)
			})
			sink := new(capture)
			defer sink.Close()
			_, err := e.Execute(context.Background(), request(), sink)
			checkCode(t, err, "QUERY_FAILED")
			if gets.Load() != 1 || deletes.Load() != 1 || len(sink.records) != 0 || strings.Contains(err.Error(), "private-token") {
				t.Fatal("incomplete result succeeded or cleanup missing")
			}
		})
	}
	t.Run("repeated pagination", func(t *testing.T) {
		var gets, deletes atomic.Int32
		e := setup(t, "trino", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "DELETE" {
				deletes.Add(1)
				w.WriteHeader(204)
				return
			}
			if r.Method == "GET" {
				gets.Add(1)
			}
			page(w, map[string]any{"id": "query_1", "nextUri": nextURL(r, "trino", 1), "columns": []column{{"value", "bigint"}}})
		})
		_, err := e.Execute(context.Background(), request(), new(capture))
		checkCode(t, err, "QUERY_FAILED")
		if gets.Load() != 1 || deletes.Load() != 1 {
			t.Fatal("loop not bounded or cancelled")
		}
	})
}

func TestCancellationRowLimitAndPageLimit(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel after batch", "sink failure", "row limit", "page bytes"} {
		t.Run(mode, func(t *testing.T) {
			var deletes, gets atomic.Int32
			e := setup(t, "trino", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "DELETE" {
					deletes.Add(1)
					w.WriteHeader(204)
					return
				}
				if r.Method == "GET" {
					gets.Add(1)
					t.Error("unneeded page request")
				}
				data := [][]any{}
				if mode != "deadline" {
					for i := 0; i < 1024; i++ {
						data = append(data, []any{i})
					}
				}
				page(w, map[string]any{"id": "query_1", "nextUri": nextURL(r, "trino", 1), "columns": []column{{"value", "bigint"}}, "data": data})
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sink := new(capture)
			defer sink.Close()
			want := "RESOURCE_EXHAUSTED"
			switch mode {
			case "deadline":
				e.limits.Timeout = 25 * time.Millisecond
				want = "DEADLINE_EXCEEDED"
			case "cancel after batch":
				sink.cancel = cancel
				want = "CANCELLED"
			case "sink failure":
				sink.fail = true
			case "row limit":
				e.limits.MaxRows = 1
			case "page bytes":
				e.client.Limit = 64
			}
			_, err := e.Execute(ctx, request(), sink)
			checkCode(t, err, want)
			// If the first body exceeds its cap there is no trusted query handle.
			wantDelete := int32(1)
			if mode == "page bytes" {
				wantDelete = 0
			}
			if deletes.Load() != wantDelete || gets.Load() != 0 {
				t.Fatalf("cleanup=%d gets=%d", deletes.Load(), gets.Load())
			}
		})
	}
}

func TestSpoolingRedirectsAndPartialBodiesAreRejected(t *testing.T) {
	for _, test := range []struct {
		name, body string
		status     int
	}{
		{"spooling", `{"id":"query_1","data":{"encoding":"json","segments":[{"uri":"https://other.invalid/private-token"}]}}`, 200},
		{"partial", `{"id":"query_1","data":[`, 200},
		{"trailing", `{"id":"query_1"} {}`, 200},
		{"redirect", "", 307},
		{"forbidden", "private-token", 403},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			e := setup(t, "trino", func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "https://other.invalid/private-token")
				w.WriteHeader(test.status)
				fmt.Fprint(w, test.body)
			})
			_, err := e.Execute(context.Background(), request(), new(capture))
			checkCode(t, err, "QUERY_FAILED")
			if calls.Load() != 1 || strings.Contains(err.Error(), "private-token") {
				t.Fatal("unsafe response followed or exposed")
			}
		})
	}
}

func TestReadOnlyAndBoundSource(t *testing.T) {
	var calls atomic.Int32
	e := setup(t, "trino", func(w http.ResponseWriter, r *http.Request) { calls.Add(1); t.Error("rejected query reached server") })
	for _, r := range []query.Request{
		{Mode: "native", ConnectionID: "engine", SQL: "DELETE FROM events"},
		{Mode: "native", ConnectionID: "engine", SQL: "SELECT 1; SELECT 2"},
		{Mode: "native", ConnectionID: "other", SQL: "SELECT 1"},
		{Mode: "native", ConnectionID: "engine", SQL: "SELECT ?", Parameters: []query.Parameter{{Type: "null"}}},
		{Mode: "native", ConnectionID: "engine", Mongo: &query.MongoRequest{Collection: "events"}},
	} {
		if _, err := e.Execute(context.Background(), r, new(capture)); err == nil {
			t.Fatal("invalid request accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("boundary bypassed")
	}
	for _, username := range []string{"", "reader\r\nAuthorization: evil", " reader"} {
		t.Setenv("KELVO_SOURCE_TRINO_USER", username)
		if _, err := New(catalog.Config{Sources: []catalog.Source{e.source}}, e.limits); err == nil {
			t.Fatal("invalid username accepted")
		}
	}
}

func TestPrestoBinaryPagesAreNotSilentlyDropped(t *testing.T) {
	e := setup(t, "presto", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id":"query_1","columns":[{"name":"value","type":"bigint"}],"binaryData":["AQID"]}`)
	})
	_, err := e.Execute(context.Background(), request(), new(capture))
	checkCode(t, err, "UNSUPPORTED")
}
