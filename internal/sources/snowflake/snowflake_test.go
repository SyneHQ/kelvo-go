// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package snowflake

import (
	"compress/gzip"
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
	values []string
	nulls  int
}

func (s *capture) Schema(*arrow.Schema) error { return nil }
func (s *capture) Write(r arrow.RecordBatch) error {
	c := r.Column(0).(*array.Decimal128)
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			s.nulls++
		} else {
			s.values = append(s.values, c.Value(i).ToString(2))
		}
	}
	return nil
}
func setup(t *testing.T, h http.HandlerFunc) *Engine {
	t.Helper()
	server := httptest.NewTLSServer(h)
	t.Cleanup(server.Close)
	t.Setenv("SF_TEST_URL", server.URL)
	t.Setenv("SF_TEST_TOKEN", "secret")
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "sf", Type: "snowflake", URLEnv: "SF_TEST_URL", TokenEnv: "SF_TEST_TOKEN"}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e.client.HTTP.Transport = server.Client().Transport
	t.Cleanup(func() { _ = e.Close() })
	return e
}

const first = `{"statementHandle":"test-id","code":"090001","resultSetMetaData":{"format":"jsonv2","numRows":3,"rowType":[{"name":"v","type":"fixed","precision":38,"scale":2}],"partitionInfo":[{"rowCount":2},{"rowCount":1}]},"data":[["12345678901234567890.12"],[null]]}`

func TestPollingAndGzipPartitions(t *testing.T) {
	var calls atomic.Int32
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-Snowflake-Authorization-Token-Type") != "OAUTH" {
			t.Error("missing token type")
		}
		if r.Method == "POST" {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["parameters"].(map[string]any)["MULTI_STATEMENT_COUNT"] != "1" || r.URL.Query().Get("async") != "true" || r.URL.Query().Get("requestId") == "" {
				t.Error("invalid submission")
			}
			w.WriteHeader(202)
			fmt.Fprint(w, `{"statementHandle":"test-id","code":"333334"}`)
			return
		}
		if r.URL.Query().Get("partition") == "1" {
			w.Header().Set("Content-Encoding", "gzip")
			z := gzip.NewWriter(w)
			fmt.Fprint(z, `{"data":[["-1.23"]]}`)
			z.Close()
			return
		}
		fmt.Fprint(w, first)
	})
	sink := &capture{}
	stats, err := e.Execute(context.Background(), query.Request{SQL: "SELECT v FROM example", Mode: "native", ConnectionID: "sf"}, sink)
	if err != nil || stats.Rows != 3 || len(sink.values) != 2 || sink.values[0] != "12345678901234567890.12" || sink.values[1] != "-1.23" || sink.nulls != 1 || calls.Load() != 3 {
		t.Fatalf("stats=%+v sink=%+v calls=%d err=%v", stats, sink, calls.Load(), err)
	}
}
func TestRejectsIncompleteAndErrorResults(t *testing.T) {
	for _, test := range []struct{ name, body string }{{"rows", strings.Replace(first, `"numRows":3`, `"numRows":4`, 1)}, {"code", strings.Replace(first, "090001", "BAD", 1)}, {"unknown", strings.Replace(first, "fixed", "variant", 1)}, {"loss", strings.Replace(first, "12345678901234567890.12", "1.001", 1)}, {"multiple", strings.Replace(first, `"code":"090001"`, `"code":"090001","statementHandles":["other"]`, 1)}} {
		t.Run(test.name, func(t *testing.T) {
			var cancels atomic.Int32
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/cancel") {
					cancels.Add(1)
					fmt.Fprint(w, `{}`)
					return
				}
				fmt.Fprint(w, test.body)
			})
			_, err := e.Execute(context.Background(), query.Request{SQL: "SELECT 1", Mode: "native", ConnectionID: "sf"}, &capture{})
			if err == nil || cancels.Load() != 1 {
				t.Fatalf("unexpected success: %v cancels=%d", err, cancels.Load())
			}
		})
	}
}
func TestCancellationAndParameterRejection(t *testing.T) {
	var calls, cancels atomic.Int32
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			cancels.Add(1)
			fmt.Fprint(w, `{}`)
			return
		}
		w.WriteHeader(202)
		fmt.Fprint(w, `{"statementHandle":"test-id"}`)
	})
	e.limits.Timeout = 30 * time.Millisecond
	req := query.Request{SQL: "SELECT 1", Mode: "native", ConnectionID: "sf"}
	_, err := e.Execute(context.Background(), req, &capture{})
	if !errors.Is(err, context.DeadlineExceeded) || cancels.Load() != 1 {
		t.Fatalf("cancellation: %v", err)
	}
	n := calls.Load()
	req.Parameters = []query.Parameter{{Type: "null"}}
	if _, err = e.Execute(context.Background(), req, &capture{}); err == nil || calls.Load() != n {
		t.Fatal("parameters silently ignored")
	}
}
