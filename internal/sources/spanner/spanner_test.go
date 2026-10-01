// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package spanner

import (
	"context"
	"encoding/json"
	"errors"
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

const database = "projects/test-project/instances/instance/databases/db"
const session = database + "/sessions/session_1"

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

func config() catalog.Config {
	return catalog.Config{Sources: []catalog.Source{{ID: "sp", Type: "spanner", URLEnv: "KELVO_SPANNER_TEST_URL", TokenEnv: "KELVO_SPANNER_TEST_TOKEN", Options: map[string]string{"project": "test-project", "instance": "instance", "database": "db"}}}}
}
func setup(t *testing.T, handler http.HandlerFunc) *Engine {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	t.Setenv("KELVO_SPANNER_TEST_URL", server.URL)
	t.Setenv("KELVO_SPANNER_TEST_TOKEN", "fixture-token")
	engine, err := New(config(), query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	engine.client.HTTP.Transport = server.Client().Transport
	t.Cleanup(func() { engine.Close() })
	return engine
}
func request() query.Request {
	return query.Request{Mode: "native", ConnectionID: "sp", SQL: "SELECT * FROM samples"}
}
func writeResult(w http.ResponseWriter, fields []field, rows any) {
	json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"rowType": map[string]any{"fields": fields}}, "rows": rows})
}
func oneField(code string) []field { return []field{{Name: "value", Type: valueType{Code: code}}} }

func TestReadOnlySessionAndExactTypes(t *testing.T) {
	var created, executed, deleted atomic.Int32
	fields := []field{}
	for _, code := range []string{"INT64", "NUMERIC", "TIMESTAMP", "DATE", "BYTES", "BOOL", "FLOAT32", "FLOAT64", "JSON", "UUID"} {
		fields = append(fields, field{Name: strings.ToLower(code), Type: valueType{Code: code}})
	}
	engine := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("source bearer credential missing")
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/"+database+"/sessions":
			created.Add(1)
			var body map[string]json.RawMessage
			if json.NewDecoder(r.Body).Decode(&body) != nil || string(body["session"]) != "{}" || len(body) != 1 {
				t.Error("invalid session creation")
			}
			fmt.Fprintf(w, `{"name":%q}`, session)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/"+session+":executeSql":
			executed.Add(1)
			var body struct {
				SQL         string `json:"sql"`
				Mode        string `json:"queryMode"`
				Transaction struct {
					SingleUse struct {
						ReadOnly struct {
							Strong bool `json:"strong"`
						} `json:"readOnly"`
					} `json:"singleUse"`
				} `json:"transaction"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.SQL != request().SQL || body.Mode != "NORMAL" || !body.Transaction.SingleUse.ReadOnly.Strong {
				t.Error("missing single-use strong read-only contract")
			}
			writeResult(w, fields, [][]any{{"9223372036854775807", "1.2345678901234567890123456789e20", "2026-01-02T03:04:05.123456789Z", "0001-01-01", "AP8=", true, 1.25, 1.5, `{"large":9007199254740993}`, "123e4567-e89b-12d3-a456-426614174000"}, {nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/"+session:
			deleted.Add(1)
			fmt.Fprint(w, `{}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(400)
		}
	})
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/unavailable/must-not-be-read.json")
	sink := &capture{}
	defer sink.close()
	stats, err := engine.Execute(context.Background(), request(), sink)
	if err != nil || stats.Rows != 2 || stats.EngineStreaming || created.Load() != 1 || executed.Load() != 1 || deleted.Load() != 1 {
		t.Fatalf("stats=%+v err=%v lifecycle=%d/%d/%d", stats, err, created.Load(), executed.Load(), deleted.Load())
	}
	r := sink.records[0]
	if r.Column(0).(*array.Int64).Value(0) != 9223372036854775807 || r.Column(1).(*array.Decimal128).Value(0).ToString(9) != "123456789012345678901.234567890" {
		t.Fatal("integer or numeric precision loss")
	}
	if r.Column(2).(*array.Timestamp).Value(0) != 1767323045123456789 || string(r.Column(4).(*array.Binary).Value(0)) != "\x00\xff" {
		t.Fatal("timestamp or bytes loss")
	}
	if sink.schema.Field(6).Type.ID() != arrow.FLOAT32 || r.Column(8).(*array.String).Value(0) != `{"large":9007199254740993}` {
		t.Fatal("type or JSON loss")
	}
	for _, column := range r.Columns() {
		if !column.IsNull(1) {
			t.Fatal("NULL lost")
		}
	}
	if value, _ := sink.schema.Field(0).Metadata.GetValue("source_type"); value != "spanner" {
		t.Fatal("missing source metadata")
	}
}

func TestFailuresCleanupWithoutReplay(t *testing.T) {
	for _, scenario := range []string{"http", "invalid_json", "incomplete", "missing_schema", "row_width", "null_rows", "numeric_rounding", "numeric_overflow", "wrong_encoding", "timestamp_precision", "timestamp_range", "nested", "pg_numeric", "row_limit", "byte_limit", "response_limit", "sink"} {
		t.Run(scenario, func(t *testing.T) {
			var executed, deleted atomic.Int32
			engine := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					deleted.Add(1)
					fmt.Fprint(w, `{}`)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/sessions") {
					fmt.Fprintf(w, `{"name":%q}`, session)
					return
				}
				executed.Add(1)
				fields, rows := oneField("INT64"), [][]any{{"1"}}
				switch scenario {
				case "http":
					w.WriteHeader(503)
					fmt.Fprint(w, "private provider error")
					return
				case "invalid_json":
					fmt.Fprint(w, `{bad`)
					return
				case "incomplete":
					w.Header().Set("Content-Length", "10000")
					fmt.Fprint(w, `{}`)
					return
				case "missing_schema":
					fmt.Fprint(w, `{"rows":[]}`)
					return
				case "null_rows":
					fmt.Fprint(w, `{"metadata":{"rowType":{"fields":[{"name":"v","type":{"code":"INT64"}}]}},"rows":null}`)
					return
				case "row_width":
					rows = [][]any{{"1", "2"}}
				case "numeric_rounding":
					fields, rows = oneField("NUMERIC"), [][]any{{"0.0000000001"}}
				case "numeric_overflow":
					fields, rows = oneField("NUMERIC"), [][]any{{"1e30"}}
				case "wrong_encoding":
					rows = [][]any{{9007199254740993}}
				case "timestamp_precision":
					fields, rows = oneField("TIMESTAMP"), [][]any{{"2026-01-02T03:04:05.1234567891Z"}}
				case "timestamp_range":
					fields, rows = oneField("TIMESTAMP"), [][]any{{"0001-01-01T00:00:00Z"}}
				case "nested":
					fields = oneField("ARRAY")
				case "pg_numeric":
					fields = oneField("NUMERIC")
					fields[0].Type.Annotation = "PG_NUMERIC"
				case "row_limit":
					rows = [][]any{{"1"}, {"2"}}
				case "byte_limit", "response_limit":
					fields, rows = oneField("STRING"), [][]any{{strings.Repeat("x", 2048)}}
				}
				writeResult(w, fields, rows)
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
			sink := &capture{fail: scenario == "sink"}
			defer sink.close()
			_, err := engine.Execute(context.Background(), request(), sink)
			if err == nil || executed.Load() != 1 || deleted.Load() != 1 {
				t.Fatalf("err=%v executed=%d deleted=%d", err, executed.Load(), deleted.Load())
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("provider error leaked")
			}
			if strings.HasSuffix(scenario, "limit") && query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
				t.Fatalf("wrong limit error: %v", err)
			}
		})
	}
}

func TestSessionIdentityAndCleanupFailure(t *testing.T) {
	for _, value := range []string{"", "projects/foreign/instances/instance/databases/db/sessions/session_1", session + "/other", session + "?token=secret"} {
		t.Run(value, func(t *testing.T) {
			var calls atomic.Int32
			engine := setup(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprintf(w, `{"name":%q}`, value) })
			_, err := engine.Execute(context.Background(), request(), &capture{})
			if err == nil || calls.Load() != 1 {
				t.Fatalf("invalid session used: calls=%d err=%v", calls.Load(), err)
			}
		})
	}
	for _, status := range []int{404, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			engine := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					w.WriteHeader(status)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/sessions") {
					fmt.Fprintf(w, `{"name":%q}`, session)
					return
				}
				writeResult(w, oneField("INT64"), [][]any{})
			})
			_, err := engine.Execute(context.Background(), request(), &capture{})
			if (status == 404) != (err == nil) {
				t.Fatalf("cleanup status %d err=%v", status, err)
			}
		})
	}
}

func TestCancellationStillDeletesSession(t *testing.T) {
	var deleted, executed atomic.Int32
	engine := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted.Add(1)
			fmt.Fprint(w, `{}`)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/sessions") {
			fmt.Fprintf(w, `{"name":%q}`, session)
			return
		}
		executed.Add(1)
		io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := engine.Execute(ctx, request(), &capture{})
	if !errors.Is(err, context.DeadlineExceeded) || executed.Load() != 1 || deleted.Load() != 1 {
		t.Fatalf("err=%v executed=%d deleted=%d", err, executed.Load(), deleted.Load())
	}
}

func TestCleanupDeadlineIsBounded(t *testing.T) {
	engine := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-time.After(4 * time.Second):
			}
			return
		}
		if strings.HasSuffix(r.URL.Path, "/sessions") {
			fmt.Fprintf(w, `{"name":%q}`, session)
			return
		}
		writeResult(w, oneField("INT64"), [][]any{})
	})
	start := time.Now()
	_, err := engine.Execute(context.Background(), request(), &capture{})
	if err == nil || time.Since(start) > 3500*time.Millisecond {
		t.Fatalf("cleanup deadline failed: %v after %s", err, time.Since(start))
	}
}

func TestUnsafeRequestsDoNotReachProvider(t *testing.T) {
	var calls atomic.Int32
	engine := setup(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) })
	requests := []query.Request{
		{Mode: "native", ConnectionID: "other", SQL: "SELECT 1"},
		{Mode: "native", ConnectionID: "sp", SQL: "DELETE FROM t"},
		{Mode: "native", ConnectionID: "sp", SQL: "SELECT 1; SELECT 2"},
		{Mode: "native", ConnectionID: "sp", SQL: "SELECT @p1", Parameters: []query.Parameter{{Type: "int64", Value: json.RawMessage(`"1"`)}}},
		{Mode: "native", ConnectionID: "sp", SQL: "SELECT 1", Sources: []string{"sp"}},
	}
	for _, req := range requests {
		if _, err := engine.Execute(context.Background(), req, &capture{}); err == nil {
			t.Fatal("unsafe request accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unsafe request reached provider")
	}
}

func TestConfigurationIsExplicit(t *testing.T) {
	t.Setenv("KELVO_SPANNER_TEST_URL", "https://spanner.googleapis.com")
	t.Setenv("KELVO_SPANNER_TEST_TOKEN", "fixture-token")
	for _, key := range []string{"project", "instance", "database", "unknown"} {
		c := config()
		if key == "unknown" {
			c.Sources[0].Options[key] = "value"
		} else {
			c.Sources[0].Options[key] = "../foreign"
		}
		if e, err := New(c, query.DefaultLimits()); err == nil {
			e.Close()
			t.Fatal("invalid option accepted")
		}
	}
	for _, origin := range []string{"http://spanner.googleapis.com", "https://user:pass@spanner.googleapis.com", "https://spanner.googleapis.com/foreign", "https://spanner.googleapis.com?token=x"} {
		t.Setenv("KELVO_SPANNER_TEST_URL", origin)
		if e, err := New(config(), query.DefaultLimits()); err == nil {
			e.Close()
			t.Fatal("invalid origin accepted")
		}
	}
}
