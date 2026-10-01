// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package athena

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
	schema                *arrow.Schema
	records               []arrow.RecordBatch
	failSchema, failWrite bool
}

func (s *capture) Schema(schema *arrow.Schema) error {
	s.schema = schema
	if s.failSchema {
		return errors.New("sink schema failure")
	}
	return nil
}
func (s *capture) Write(record arrow.RecordBatch) error {
	if s.failWrite {
		return errors.New("sink write failure")
	}
	record.Retain()
	s.records = append(s.records, record)
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
	t.Setenv("KELVO_SOURCE_ATHENA_TEST_URL", server.URL)
	t.Setenv("KELVO_SOURCE_ATHENA_TEST_ID", "test-id")
	t.Setenv("KELVO_SOURCE_ATHENA_TEST_SECRET", "private-credential")
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "warehouse", Type: "athena", URLEnv: "KELVO_SOURCE_ATHENA_TEST_URL", UsernameEnv: "KELVO_SOURCE_ATHENA_TEST_ID", PasswordEnv: "KELVO_SOURCE_ATHENA_TEST_SECRET", Options: map[string]string{"region": "us-east-1", "workgroup": "analytics", "database": "sales", "output_location": "s3://test-query-results/prefix/"}}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e.c.HTTP.Transport = server.Client().Transport
	t.Cleanup(func() { _ = e.Close() })
	return e
}
func req() query.Request {
	return query.Request{SQL: "SELECT * FROM facts", Mode: "native", ConnectionID: "warehouse"}
}
func results(columns []athenaColumn, rows [][]*string, next string) string {
	r := result{ResultSet: &resultSet{}, NextToken: next}
	r.ResultSet.Metadata.Columns = columns
	for _, values := range rows {
		row := athenaRow{}
		for _, value := range values {
			row.Data = append(row.Data, cell{value})
		}
		r.ResultSet.Rows = append(r.ResultSet.Rows, row)
	}
	b, _ := json.Marshal(r)
	return string(b)
}
func str(s string) *string { return &s }
func onePage(rows ...string) string {
	values := [][]*string{{str("id")}}
	for _, row := range rows {
		values = append(values, []*string{str(row)})
	}
	return results([]athenaColumn{{Name: "id", Type: "bigint"}}, values, "")
}
func TestLifecyclePaginationTypesAndNulls(t *testing.T) {
	var starts, polls, pages, stops atomic.Int32
	columns := []athenaColumn{{Name: "id", Type: "bigint"}, {Name: "amount", Type: "decimal(38,9)", Precision: 38, Scale: 9}, {Name: "at", Type: "timestamp(9)"}, {Name: "ratio", Type: "real"}, {Name: "name", Type: "varchar"}, {Name: "ok", Type: "boolean"}, {Name: "day", Type: "date"}, {Name: "bytes", Type: "varbinary"}}
	header := []*string{}
	for _, c := range columns {
		header = append(header, str(c.Name))
	}
	first := []*string{str("9223372036854775807"), str("12345678901234567890123456789.123456789"), str("2026-10-01 12:34:56.123456789"), str("1.25"), str(""), str("true"), str("2026-10-01"), str("00 ff")}
	nulls := make([]*string, len(columns))
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			t.Error("invalid request JSON")
		}
		switch r.Header.Get("X-Amz-Target") {
		case "AmazonAthena.StartQueryExecution":
			starts.Add(1)
			token, _ := body["ClientRequestToken"].(string)
			if len(token) != 64 || body["QueryString"] != req().SQL || body["WorkGroup"] != "analytics" || body["QueryExecutionContext"].(map[string]any)["Database"] != "sales" || body["ResultConfiguration"].(map[string]any)["OutputLocation"] != "s3://test-query-results/prefix/" {
				t.Error("incorrect execution scope")
			}
			fmt.Fprint(w, `{"QueryExecutionId":"execution-1"}`)
		case "AmazonAthena.GetQueryExecution":
			state := "SUCCEEDED"
			if polls.Add(1) == 1 {
				state = "QUEUED"
			}
			fmt.Fprintf(w, `{"QueryExecution":{"QueryExecutionId":"execution-1","Status":{"State":%q}}}`, state)
		case "AmazonAthena.GetQueryResults":
			if body["QueryExecutionId"] != "execution-1" || body["QueryResultType"] != "DATA_ROWS" {
				t.Error("invalid fetch")
			}
			if pages.Add(1) == 1 {
				fmt.Fprint(w, results(columns, [][]*string{header, first, nulls}, "opaque-page-2"))
			} else {
				if body["NextToken"] != "opaque-page-2" {
					t.Error("missing next token")
				}
				fmt.Fprint(w, results(columns, [][]*string{first}, ""))
			}
		case "AmazonAthena.StopQueryExecution":
			stops.Add(1)
		default:
			t.Error("unexpected operation")
		}
	})
	sink := &capture{}
	defer sink.close()
	stats, err := e.Execute(context.Background(), req(), sink)
	if err != nil || stats.Rows != 3 || stats.Batches != 1 || stats.WireBytes == 0 || stats.PrepareNS == 0 || stats.DurationNS == 0 || stats.Backend != "athena" || stats.EngineStreaming || starts.Load() != 1 || polls.Load() != 2 || pages.Load() != 2 || stops.Load() != 0 {
		t.Fatalf("stats=%+v err=%v starts=%d polls=%d pages=%d stops=%d", stats, err, starts.Load(), polls.Load(), pages.Load(), stops.Load())
	}
	record := sink.records[0]
	if record.Column(0).(*array.Int64).Value(0) != 9223372036854775807 || !record.Column(0).IsNull(1) || record.Column(1).(*array.Decimal128).Value(0).ToString(9) != "12345678901234567890123456789.123456789" || record.Column(2).(*array.Timestamp).Value(0).ToTime(arrow.Nanosecond).Nanosecond() != 123456789 || record.Column(3).(*array.Float32).Value(0) != 1.25 || record.Column(4).(*array.String).Value(0) != "" || !record.Column(5).(*array.Boolean).Value(0) || string(record.Column(7).(*array.Binary).Value(0)) != "\x00\xff" {
		t.Fatal("result conversion lost data")
	}
	if sink.schema.Field(2).Type.(*arrow.TimestampType).TimeZone != "" {
		t.Fatal("naive timestamp acquired a timezone")
	}
}
func TestFirstPageHeaderOnlyAndEmptyPages(t *testing.T) {
	for _, emptyFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(emptyFirst), func(t *testing.T) {
			pages := 0
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Header.Get("X-Amz-Target") {
				case "AmazonAthena.StartQueryExecution":
					fmt.Fprint(w, `{"QueryExecutionId":"q"}`)
				case "AmazonAthena.GetQueryExecution":
					fmt.Fprint(w, `{"QueryExecution":{"Status":{"State":"SUCCEEDED"}}}`)
				case "AmazonAthena.GetQueryResults":
					pages++
					rows := [][]*string{{str("id")}}
					if emptyFirst {
						rows = nil
					}
					if pages == 1 {
						fmt.Fprint(w, results([]athenaColumn{{Name: "id", Type: "varchar"}}, rows, "n"))
					} else {
						fmt.Fprint(w, results(nil, [][]*string{{str("id")}}, ""))
					}
				}
			})
			sink := &capture{}
			defer sink.close()
			stats, err := e.Execute(context.Background(), req(), sink)
			if err != nil || stats.Rows != 1 || sink.records[0].Column(0).(*array.String).Value(0) != "id" {
				t.Fatalf("discarded data on continuation: %+v %v", stats, err)
			}
		})
	}
}
func TestFailureCleanupAndNoReplay(t *testing.T) {
	for _, test := range []struct{ name, state, page string }{
		{"failed", "FAILED", ""}, {"cancelled", "CANCELLED", ""}, {"invalid state", "UNKNOWN", ""}, {"missing set", "SUCCEEDED", `{}`},
		{"bad header", "SUCCEEDED", results([]athenaColumn{{Name: "id", Type: "bigint"}}, [][]*string{{str("999")}}, "")},
		{"bad width", "SUCCEEDED", results([]athenaColumn{{Name: "id", Type: "bigint"}}, [][]*string{{str("id")}, {str("1"), str("2")}}, "")},
		{"bad scalar", "SUCCEEDED", onePage("9223372036854775808")},
		{"unsupported", "SUCCEEDED", results([]athenaColumn{{Name: "id", Type: "array(bigint)"}}, [][]*string{{str("id")}}, "")},
	} {
		t.Run(test.name, func(t *testing.T) {
			var starts, stops atomic.Int32
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Header.Get("X-Amz-Target") {
				case "AmazonAthena.StartQueryExecution":
					starts.Add(1)
					fmt.Fprint(w, `{"QueryExecutionId":"q"}`)
				case "AmazonAthena.GetQueryExecution":
					fmt.Fprintf(w, `{"QueryExecution":{"Status":{"State":%q,"StateChangeReason":"private-credential"}}}`, test.state)
				case "AmazonAthena.GetQueryResults":
					fmt.Fprint(w, test.page)
				case "AmazonAthena.StopQueryExecution":
					stops.Add(1)
					w.WriteHeader(200)
				}
			})
			sink := &capture{}
			defer sink.close()
			_, err := e.Execute(context.Background(), req(), sink)
			if err == nil || strings.Contains(err.Error(), "private-credential") || stops.Load() != 1 || starts.Load() != 1 {
				t.Fatalf("err=%v starts=%d stops=%d", err, starts.Load(), stops.Load())
			}
		})
	}
}
func TestChangedSchemaAndRepeatedToken(t *testing.T) {
	for _, change := range []bool{false, true} {
		t.Run(fmt.Sprint(change), func(t *testing.T) {
			var pages atomic.Int32
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Header.Get("X-Amz-Target") {
				case "AmazonAthena.StartQueryExecution":
					fmt.Fprint(w, `{"QueryExecutionId":"q"}`)
				case "AmazonAthena.GetQueryExecution":
					fmt.Fprint(w, `{"QueryExecution":{"Status":{"State":"SUCCEEDED"}}}`)
				case "AmazonAthena.GetQueryResults":
					p := pages.Add(1)
					kind := "bigint"
					rows := [][]*string{{str("id")}}
					if p > 1 {
						rows = nil
						if change {
							kind = "varchar"
						}
					}
					fmt.Fprint(w, results([]athenaColumn{{Name: "id", Type: kind}}, rows, "same"))
				}
			})
			sink := &capture{}
			defer sink.close()
			_, err := e.Execute(context.Background(), req(), sink)
			if err == nil || pages.Load() != 2 {
				t.Fatalf("err=%v pages=%d", err, pages.Load())
			}
		})
	}
}
func TestDeadlineAndSinkFailureStopIndependently(t *testing.T) {
	for _, mode := range []string{"deadline", "schema", "write"} {
		t.Run(mode, func(t *testing.T) {
			var stopped atomic.Bool
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Header.Get("X-Amz-Target") {
				case "AmazonAthena.StartQueryExecution":
					fmt.Fprint(w, `{"QueryExecutionId":"q"}`)
				case "AmazonAthena.GetQueryExecution":
					state := "SUCCEEDED"
					if mode == "deadline" {
						state = "RUNNING"
					}
					fmt.Fprintf(w, `{"QueryExecution":{"Status":{"State":%q}}}`, state)
				case "AmazonAthena.GetQueryResults":
					fmt.Fprint(w, onePage("1"))
				case "AmazonAthena.StopQueryExecution":
					if r.Context().Err() != nil {
						t.Error("cleanup inherited canceled context")
					}
					stopped.Store(true)
				}
			})
			e.l.Timeout = 35 * time.Millisecond
			sink := &capture{failSchema: mode == "schema", failWrite: mode == "write"}
			defer sink.close()
			_, err := e.Execute(context.Background(), req(), sink)
			if err == nil || !stopped.Load() {
				t.Fatalf("err=%v stopped=%t", err, stopped.Load())
			}
			if mode == "deadline" && query.PublicError(err).Code != "DEADLINE_EXCEEDED" {
				t.Fatal(err)
			}
		})
	}
}
func TestRequestRestrictionsAndLimits(t *testing.T) {
	var calls atomic.Int32
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.Header.Get("X-Amz-Target") {
		case "AmazonAthena.StartQueryExecution":
			fmt.Fprint(w, `{"QueryExecutionId":"q"}`)
		case "AmazonAthena.GetQueryExecution":
			fmt.Fprint(w, `{"QueryExecution":{"Status":{"State":"SUCCEEDED"}}}`)
		case "AmazonAthena.GetQueryResults":
			fmt.Fprint(w, onePage("1", "2"))
		}
	})
	for _, r := range []query.Request{{SQL: "DELETE FROM facts", Mode: "native", ConnectionID: "warehouse"}, {SQL: "SELECT 1; SELECT 2", Mode: "native", ConnectionID: "warehouse"}, {SQL: "SELECT 1", Mode: "native", ConnectionID: "other"}, {SQL: "SELECT ?", Mode: "native", ConnectionID: "warehouse", Parameters: []query.Parameter{{Type: "int64", Value: json.RawMessage(`1`)}}}} {
		sink := &capture{}
		_, err := e.Execute(context.Background(), r, sink)
		sink.close()
		if err == nil {
			t.Fatal("invalid request accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request sent")
	}
	e.l.MaxRows = 1
	sink := &capture{}
	defer sink.close()
	if _, err := e.Execute(context.Background(), req(), sink); err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("row limit ignored: %v", err)
	}
}
