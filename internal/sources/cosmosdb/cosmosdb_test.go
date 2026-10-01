// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cosmosdb

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	fail    bool
}

func (s *capture) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *capture) Write(record arrow.RecordBatch) error {
	if s.fail {
		return errors.New("test sink failure")
	}
	record.Retain()
	s.records = append(s.records, record)
	return nil
}
func (s *capture) close() {
	for _, record := range s.records {
		record.Release()
	}
}

func config(auth string) catalog.Config {
	return catalog.Config{Sources: []catalog.Source{{ID: "cosmos", Type: "cosmosdb", URLEnv: "KELVO_COSMOS_TEST_URL", TokenEnv: "KELVO_COSMOS_TEST_TOKEN", Options: map[string]string{"database": "DbCase", "container": "Container", "auth": auth}}}}
}
func setup(t *testing.T, auth string, handler http.HandlerFunc) *Engine {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	t.Setenv("KELVO_COSMOS_TEST_URL", server.URL)
	token := "fixture-aad-token"
	if auth == "master_key" {
		token = base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 64)))
	}
	t.Setenv("KELVO_COSMOS_TEST_TOKEN", token)
	engine, err := New(config(auth), query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	engine.client.HTTP.Transport = server.Client().Transport
	engine.now = func() time.Time { return time.Date(2026, 10, 1, 17, 0, 0, 0, time.UTC) }
	t.Cleanup(func() { engine.Close() })
	return engine
}
func request() query.Request {
	return query.Request{Mode: "native", ConnectionID: "cosmos", SQL: "SELECT * FROM c WHERE c.active = true"}
}
func response(w http.ResponseWriter, documents string, count int, next, session, charge string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-ms-request-charge", charge)
	if next != "" {
		w.Header().Set("x-ms-continuation", next)
	}
	if session != "" {
		w.Header().Set("x-ms-session-token", session)
	}
	fmt.Fprintf(w, `{"_rid":"collection-id","_count":%d,"Documents":%s}`, count, documents)
}

func TestOfficialMasterKeyAuthorizationVector(t *testing.T) {
	// Fixed vector from Microsoft's access-control documentation, independent of
	// the connector fixture's generated key and query request.
	key, err := base64.StdEncoding.DecodeString("dsZQi3KtZmCv1ljt3VNWNm7sQUF1y5rJfC6kv5JiwvW0EndXdDku/dkKBp8/ufDToSxLzR4y+O/0H/t4bQtVNw==")
	if err != nil {
		t.Fatal(err)
	}
	actual, err := url.QueryUnescape(masterAuthorization("GET", "dbs", "dbs/ToDoList", "Thu, 27 Apr 2017 00:51:12 GMT", key))
	if err != nil || actual != "type=master&ver=1.0&sig=c09PEVJrgp2uQRkr934kFbTqhByc7TVr3OHyqlu+c+c=" {
		t.Fatalf("official signature mismatch: %q, %v", actual, err)
	}
}

func TestDistributedQueryShapesRefusedBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	engine := setup(t, "aad", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		response(w, "[]", 0, "", "", "1")
	})
	for _, sql := range []string{
		"SELECT * FROM c ORDER BY c.id", "SELECT DISTINCT c.id FROM c",
		"SELECT VALUE COUNT(1) FROM c", "SELECT AVG(c.n) FROM c",
		"SELECT MIN(c.n), MAX(c.n), SUM(c.n) FROM c",
		"SELECT c.id FROM c GROUP BY c.id", "SELECT TOP 10 * FROM c",
		"SELECT * FROM c OFFSET 0 LIMIT 10", "SELECT * FROM c JOIN a IN c.items",
		"SELECT * FROM c WHERE EXISTS (SELECT VALUE t FROM t IN c.items)",
		"WITH t AS (SELECT * FROM c) SELECT * FROM t",
		"SELECT VALUE [COUNT(1)] FROM c",
		"SELECT VALUE {\"n\": CoUnT /* comment */ (1)} FROM c",
		"SELECT * FROM c ORDER /* comment */ BY RANK FullTextScore(c.text, 'term')",
	} {
		r := request()
		r.SQL = sql
		sink := &capture{}
		_, err := engine.Execute(context.Background(), r, sink)
		sink.close()
		if err == nil || query.PublicError(err).Code != "UNSUPPORTED" || sink.schema != nil {
			t.Fatalf("distributed query admitted: %q, err=%v", sql, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("unsupported queries issued %d HTTP calls", calls.Load())
	}
	for _, sql := range []string{
		"SELECT * FROM c WHERE c.active = true",
		"SELECT VALUE c[\"count\"] FROM c",
		"SELECT VALUE [c.id, c.n] FROM c WHERE c.tag = 'ORDER BY COUNT SUM'",
		"SELECT {\"sum\": c.n, \"count\": c.id} FROM c /* ORDER BY */",
		"-- COUNT(1)\nSELECT VALUE ARRAY_LENGTH(c.items) FROM c;",
	} {
		if _, err := simpleQuery(sql); err != nil {
			t.Fatalf("simple query refused: %q, err=%v", sql, err)
		}
	}
}

func TestBoundAuthenticationPaginationAndExactJSON(t *testing.T) {
	for _, auth := range []string{"aad", "master_key"} {
		t.Run(auth, func(t *testing.T) {
			calls := 0
			want := `{ "large":9007199254740993, "decimal":123456789012345678901.23456789, "nested":[true,null,{"s":"v"}] }`
			engine := setup(t, auth, func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "POST" || r.URL.Path != "/dbs/DbCase/colls/Container/docs" || r.URL.RawQuery != "" {
					t.Error("query escaped configured resource")
				}
				for key, value := range map[string]string{"Content-Type": "application/query+json", "x-ms-documentdb-isquery": "True", "x-ms-documentdb-query-enablecrosspartition": "True", "x-ms-max-item-count": "1000", "x-ms-version": "2018-12-31", "x-ms-date": "Thu, 01 Oct 2026 17:00:00 GMT"} {
					if r.Header.Get(key) != value {
						t.Errorf("header %s=%q", key, r.Header.Get(key))
					}
				}
				expected := url.QueryEscape("type=aad&ver=1.0&sig=fixture-aad-token")
				if auth == "master_key" {
					expected = masterAuthorization("POST", "docs", "dbs/DbCase/colls/Container", r.Header.Get("x-ms-date"), []byte(strings.Repeat("k", 64)))
				}
				if r.Header.Get("Authorization") != expected {
					t.Error("incorrect bound authorization")
				}
				var body struct {
					Query      string `json:"query"`
					Parameters []any  `json:"parameters"`
				}
				if json.NewDecoder(r.Body).Decode(&body) != nil || body.Query != request().SQL || body.Parameters == nil || len(body.Parameters) != 0 {
					t.Error("invalid query request body")
				}
				switch calls {
				case 1:
					if r.Header.Get("x-ms-continuation") != "" || r.Header.Get("x-ms-session-token") != "" {
						t.Error("unexpected initial continuation")
					}
					response(w, "["+want+"]", 1, "first+/=", "0:7", "2.125")
				case 2:
					if r.Header.Get("x-ms-continuation") != "first+/=" || r.Header.Get("x-ms-session-token") != "0:7" {
						t.Error("continuation/session changed")
					}
					response(w, "[]", 0, "second+/=", "0:8", "0.1")
				case 3:
					if r.Header.Get("x-ms-continuation") != "second+/=" || r.Header.Get("x-ms-session-token") != "0:8" {
						t.Error("empty page broke continuation")
					}
					response(w, `[null,9223372036854775807]`, 2, "", "", "1.25")
				default:
					t.Error("unexpected execution replay")
					w.WriteHeader(500)
				}
			})
			sink := &capture{}
			defer sink.close()
			stats, err := engine.Execute(context.Background(), request(), sink)
			if err != nil || stats.Rows != 3 || calls != 3 || len(sink.records) != 1 {
				t.Fatalf("stats=%+v calls=%d err=%v", stats, calls, err)
			}
			column := sink.records[0].Column(0).(*array.Binary)
			for i, value := range []string{want, "null", "9223372036854775807"} {
				if string(column.Value(i)) != value || column.IsNull(i) {
					t.Fatalf("JSON value changed at %d: %s", i, column.Value(i))
				}
			}
			if sink.schema.Field(0).Nullable {
				t.Fatal("JSON null must not become Arrow null")
			}
		})
	}
}

func TestMalformedResponsesAndBudgets(t *testing.T) {
	for _, scenario := range []string{"http", "throttled", "redirect", "content_type", "invalid_json", "trailing_json", "invalid_utf8", "incomplete", "missing_count", "wrong_count", "null_documents", "missing_charge", "negative_charge", "nonfinite_charge", "duplicate_charge", "duplicate_next", "long_next", "row_limit", "byte_limit", "response_limit", "page_limit", "ru_limit", "repeated_next", "cycle", "sink"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			engine := setup(t, "aad", func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("x-ms-request-charge", "1")
				switch scenario {
				case "http":
					w.WriteHeader(503)
					fmt.Fprint(w, "private provider message")
					return
				case "throttled":
					w.WriteHeader(429)
					return
				case "redirect":
					w.Header().Set("Location", "https://unapproved.invalid/leak")
					w.WriteHeader(307)
					return
				case "content_type":
					w.Header().Set("Content-Type", "text/plain")
					fmt.Fprint(w, `{"_count":0,"Documents":[]}`)
					return
				case "invalid_json":
					fmt.Fprint(w, `{invalid`)
					return
				case "trailing_json":
					fmt.Fprint(w, `{"_count":0,"Documents":[]} {}`)
					return
				case "invalid_utf8":
					w.Write([]byte{'{', '"', 'D', 'o', 'c', 'u', 'm', 'e', 'n', 't', 's', '"', ':', '[', '"', 0xff, '"', ']', '}'})
					return
				case "incomplete":
					w.Header().Set("Content-Length", "10000")
					fmt.Fprint(w, `{}`)
					return
				case "missing_count":
					fmt.Fprint(w, `{"Documents":[]}`)
					return
				case "wrong_count":
					fmt.Fprint(w, `{"_count":2,"Documents":[1]}`)
					return
				case "null_documents":
					fmt.Fprint(w, `{"_count":0,"Documents":null}`)
					return
				case "missing_charge":
					w.Header().Del("x-ms-request-charge")
				case "negative_charge":
					w.Header().Set("x-ms-request-charge", "-1")
				case "nonfinite_charge":
					w.Header().Set("x-ms-request-charge", "NaN")
				case "duplicate_charge":
					w.Header().Add("x-ms-request-charge", "2")
				case "duplicate_next":
					w.Header().Add("x-ms-continuation", "a")
					w.Header().Add("x-ms-continuation", "b")
				case "long_next":
					w.Header().Set("x-ms-continuation", strings.Repeat("x", 16385))
				case "row_limit":
					fmt.Fprint(w, `{"_count":2,"Documents":[1,2]}`)
					return
				case "byte_limit", "response_limit":
					fmt.Fprintf(w, `{"_count":1,"Documents":[{"large":%q}]}`, strings.Repeat("x", 2048))
					return
				case "page_limit":
					w.Header().Set("x-ms-continuation", "another-page")
				case "ru_limit":
					w.Header().Set("x-ms-request-charge", "1.000001")
				case "repeated_next":
					w.Header().Set("x-ms-continuation", "same")
				case "cycle":
					if call%2 == 1 {
						w.Header().Set("x-ms-continuation", "a")
					} else {
						w.Header().Set("x-ms-continuation", "b")
					}
				}
				fmt.Fprint(w, `{"_count":1,"Documents":[{"id":"one"}]}`)
			})
			if scenario == "row_limit" {
				engine.limits.MaxRows = 1
			}
			if scenario == "byte_limit" {
				engine.limits.MaxBytes = 1024
			}
			if scenario == "response_limit" {
				engine.client.Limit = 1024
			}
			if scenario == "page_limit" {
				engine.maxPages = 1
			}
			if scenario == "ru_limit" {
				engine.maxRequestUnits = 1
			}
			sink := &capture{fail: scenario == "sink"}
			defer sink.close()
			_, err := engine.Execute(context.Background(), request(), sink)
			expectedCalls := int32(1)
			if scenario == "repeated_next" {
				expectedCalls = 2
			}
			if scenario == "cycle" {
				expectedCalls = 3
			}
			if err == nil || calls.Load() != expectedCalls {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("provider error leaked")
			}
			if strings.HasSuffix(scenario, "limit") || scenario == "throttled" {
				if query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
					t.Fatalf("wrong budget error: %v", err)
				}
			}
		})
	}
}

func TestExactRequestUnitBoundary(t *testing.T) {
	calls := 0
	engine := setup(t, "aad", func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			response(w, "[]", 0, "next", "", "0.100000")
		} else {
			response(w, "[]", 0, "", "", "0.900000")
		}
	})
	engine.maxRequestUnits = 1
	stats, err := engine.Execute(context.Background(), request(), &capture{})
	if err != nil || stats.Rows != 0 || calls != 2 {
		t.Fatalf("exact RU sum failed: %+v %v", stats, err)
	}
}

func TestCompressedPageBounds(t *testing.T) {
	for _, scenario := range []string{"valid", "oversized", "truncated"} {
		t.Run(scenario, func(t *testing.T) {
			engine := setup(t, "aad", func(w http.ResponseWriter, r *http.Request) {
				data := `{"_count":1,"Documents":[{"n":9007199254740993}]}`
				if scenario == "oversized" {
					data = `{"_count":1,"Documents":["` + strings.Repeat("x", 2048) + `"]}`
				}
				var compressed bytes.Buffer
				z := gzip.NewWriter(&compressed)
				z.Write([]byte(data))
				z.Close()
				output := compressed.Bytes()
				if scenario == "truncated" {
					output = output[:len(output)-4]
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("x-ms-request-charge", "1")
				w.Write(output)
			})
			engine.client.Limit = 1024
			sink := &capture{}
			defer sink.close()
			stats, err := engine.Execute(context.Background(), request(), sink)
			if scenario == "valid" {
				if err != nil || stats.Rows != 1 || string(sink.records[0].Column(0).(*array.Binary).Value(0)) != `{"n":9007199254740993}` {
					t.Fatalf("valid gzip failed: %+v %v", stats, err)
				}
			} else if err == nil {
				t.Fatal("invalid compressed response accepted")
			} else if scenario == "oversized" && query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
				t.Fatalf("wrong decompression limit error: %v", err)
			}
		})
	}
}

func TestCancellationAndUnsafeRequests(t *testing.T) {
	var calls atomic.Int32
	engine := setup(t, "aad", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	})
	for _, req := range []query.Request{
		{Mode: "native", ConnectionID: "other", SQL: "SELECT 1"},
		{Mode: "native", ConnectionID: "cosmos", SQL: "DELETE FROM c"},
		{Mode: "native", ConnectionID: "cosmos", SQL: "SELECT 1; SELECT 2"},
		{Mode: "native", ConnectionID: "cosmos", SQL: "SELECT @x", Parameters: []query.Parameter{{Type: "int64", Value: json.RawMessage(`"1"`)}}},
		{Mode: "native", ConnectionID: "cosmos", SQL: "SELECT 1", Sources: []string{"cosmos"}},
	} {
		if _, err := engine.Execute(context.Background(), req, &capture{}); err == nil {
			t.Fatal("unsafe request accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unsafe request reached provider")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := engine.Execute(ctx, request(), &capture{})
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatalf("cancellation/replay failure: calls=%d err=%v", calls.Load(), err)
	}
}

func TestConfigurationIsExplicit(t *testing.T) {
	t.Setenv("KELVO_COSMOS_TEST_URL", "https://account.documents.azure.com")
	t.Setenv("KELVO_COSMOS_TEST_TOKEN", "fixture-token")
	for _, setting := range []struct{ key, value string }{{"database", "../other"}, {"container", "Container/docs"}, {"auth", ""}, {"auth", "automatic"}, {"unknown", "x"}, {"max_pages", "0"}, {"max_pages", "10001"}, {"max_request_units", "1000001"}, {"max_request_units", "0.5"}} {
		c := config("aad")
		c.Sources[0].Options[setting.key] = setting.value
		if e, err := New(c, query.DefaultLimits()); err == nil {
			e.Close()
			t.Fatalf("invalid option accepted: %s", setting.key)
		}
	}
	for _, token := range []string{"not-base64", base64.StdEncoding.EncodeToString([]byte("short"))} {
		t.Setenv("KELVO_COSMOS_TEST_TOKEN", token)
		if e, err := New(config("master_key"), query.DefaultLimits()); err == nil {
			e.Close()
			t.Fatal("invalid master key accepted")
		}
	}
	t.Setenv("KELVO_COSMOS_TEST_TOKEN", "fixture-token")
	for _, origin := range []string{"http://account.documents.azure.com", "https://user:pass@account.documents.azure.com", "https://account.documents.azure.com/dbs/other", "https://account.documents.azure.com?token=x"} {
		t.Setenv("KELVO_COSMOS_TEST_URL", origin)
		if e, err := New(config("aad"), query.DefaultLimits()); err == nil {
			e.Close()
			t.Fatal("invalid origin accepted")
		}
	}
}
