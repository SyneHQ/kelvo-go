// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package d1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

const account = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const database = "11111111-1111-4111-8111-111111111111"
const validResult = `{"success":true,"errors":[],"result":[{"success":true,"results":{"columns":["value"],"rows":[[1]]}}]}`

func fixture(t *testing.T, handler http.HandlerFunc, limits query.Limits) *Engine {
	t.Helper()
	s := httptest.NewTLSServer(handler)
	t.Cleanup(s.Close)
	t.Setenv("KELVO_SOURCE_D1_URL", s.URL)
	t.Setenv("KELVO_SOURCE_D1_TOKEN", "fixture-d1-token")
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "d1", Type: "d1", URLEnv: "KELVO_SOURCE_D1_URL", TokenEnv: "KELVO_SOURCE_D1_TOKEN", Options: map[string]string{"account_id": account, "database_id": database}}}}, limits)
	if err != nil {
		t.Fatal(err)
	}
	e.client.HTTP = s.Client()
	e.client.HTTP.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	t.Cleanup(func() { _ = e.Close() })
	return e
}

type capture struct {
	schema  *arrow.Schema
	records []arrow.RecordBatch
}

func (c *capture) Schema(schema *arrow.Schema) error { c.schema = schema; return nil }
func (c *capture) Write(record arrow.RecordBatch) error {
	record.Retain()
	c.records = append(c.records, record)
	return nil
}
func (c *capture) release() {
	for _, r := range c.records {
		r.Release()
	}
}
func request() query.Request {
	return query.Request{Mode: "native", ConnectionID: "d1", SQL: "SELECT * FROM events -- bounded comment"}
}
func code(t *testing.T, err error, expected string) {
	t.Helper()
	var public *query.Error
	if !errors.As(err, &public) || public.Code != expected {
		t.Fatalf("got %v, expected %s", err, expected)
	}
}

func TestD1RawPreservesColumnOrderNullsAndNumericTypes(t *testing.T) {
	e := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/client/v4/accounts/"+account+"/d1/database/"+database+"/raw" {
			t.Error("wrong raw API route")
		}
		if r.Header.Get("Authorization") != "Bearer fixture-d1-token" {
			t.Error("missing authentication")
		}
		var input map[string]string
		if json.NewDecoder(r.Body).Decode(&input) != nil || !strings.Contains(input["sql"], "\n) AS kelvo_result LIMIT 1000001") {
			t.Error("query not bounded or trailing comment swallowed suffix")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"errors":[],"result":[{"success":true,"results":{"columns":["integer","real","text","bool","blob","null"],"rows":[[null,null,null,null,null,null],[9223372036854775807,1.25,"hello",true,[0,255],null],[2,2,"there",false,[],null]]}}]}`))
	}, query.DefaultLimits())
	sink := new(capture)
	defer sink.release()
	stats, err := e.Execute(context.Background(), request(), sink)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Backend != "d1" || stats.EngineStreaming || stats.Rows != 3 || stats.Batches != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	for i, id := range []arrow.Type{arrow.INT64, arrow.FLOAT64, arrow.STRING, arrow.BOOL, arrow.BINARY, arrow.NULL} {
		if sink.schema.Field(i).Type.ID() != id {
			t.Fatalf("column %d type=%s", i, sink.schema.Field(i).Type)
		}
	}
	r := sink.records[0]
	if !r.Column(0).IsNull(0) || r.Column(0).(*array.Int64).Value(1) != 9223372036854775807 {
		t.Fatal("int64 or initial NULL changed")
	}
	if r.Column(1).(*array.Float64).Value(1) != 1.25 || r.Column(1).(*array.Float64).Value(2) != 2 {
		t.Fatal("numeric promotion failed")
	}
	if !bytes.Equal(r.Column(4).(*array.Binary).Value(1), []byte{0, 255}) || r.Column(4).IsNull(2) || len(r.Column(4).(*array.Binary).Value(2)) != 0 {
		t.Fatal("BLOB value/empty BLOB changed")
	}
	if r.Column(5).NullN() != 3 {
		t.Fatal("all-null column changed")
	}
}

func TestD1EmptyResponseRetainsNullSchema(t *testing.T) {
	e := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"result":[{"success":true,"results":{"columns":["a","b"],"rows":[]}}]}`))
	}, query.DefaultLimits())
	sink := new(capture)
	defer sink.release()
	stats, err := e.Execute(context.Background(), request(), sink)
	if err != nil || stats.Rows != 0 || sink.schema == nil || len(sink.schema.Fields()) != 2 || sink.schema.Field(0).Type.ID() != arrow.NULL || len(sink.records) != 0 {
		t.Fatalf("empty result: %+v %v", stats, err)
	}
}

func TestD1RejectsUnsuccessfulMalformedAndLossyResults(t *testing.T) {
	for _, test := range []struct{ name, result, want string }{
		{"top failure", `{"success":false,"result":[]}`, "QUERY_FAILED"},
		{"result failure", `{"success":true,"result":[{"success":false,"results":{"columns":["x"],"rows":[[1]]}}]}`, "QUERY_FAILED"},
		{"contradictory errors", `{"success":true,"errors":[{"message":"secret backend diagnostic"}],"result":[]}`, "QUERY_FAILED"},
		{"trailing data", validResult + `{}`, "QUERY_FAILED"},
		{"row width", `{"success":true,"result":[{"success":true,"results":{"columns":["x"],"rows":[[1,2]]}}]}`, "QUERY_FAILED"},
		{"heterogeneous", `{"success":true,"result":[{"success":true,"results":{"columns":["x"],"rows":[[1],["changed"]]}}]}`, "UNSUPPORTED"},
		{"lossy promotion", `{"success":true,"result":[{"success":true,"results":{"columns":["x"],"rows":[[9223372036854775807],[1.25]]}}]}`, "UNSUPPORTED"},
		{"int overflow", `{"success":true,"result":[{"success":true,"results":{"columns":["x"],"rows":[[9223372036854775808]]}}]}`, "UNSUPPORTED"},
		{"float overflow", `{"success":true,"result":[{"success":true,"results":{"columns":["x"],"rows":[[1e1000]]}}]}`, "QUERY_FAILED"},
		{"invalid blob", `{"success":true,"result":[{"success":true,"results":{"columns":["x"],"rows":[[[256]]]}}]}`, "QUERY_FAILED"},
		{"unexpected object", `{"success":true,"result":[{"success":true,"results":{"columns":["x"],"rows":[[{"a":1}]]}}]}`, "UNSUPPORTED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := fixture(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(test.result)) }, query.DefaultLimits())
			sink := new(capture)
			defer sink.release()
			_, err := e.Execute(context.Background(), request(), sink)
			code(t, err, test.want)
			if sink.schema != nil || len(sink.records) != 0 {
				t.Fatal("invalid result emitted data")
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatal("provider diagnostic leaked")
			}
		})
	}
}

func TestD1ResponseLimitIncludesTrailingWhitespace(t *testing.T) {
	e := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(validResult + strings.Repeat(" ", 100)))
	}, query.DefaultLimits())
	e.client.Limit = int64(len(validResult) + 10)
	sink := new(capture)
	defer sink.release()
	_, err := e.Execute(context.Background(), request(), sink)
	code(t, err, "RESOURCE_EXHAUSTED")
	if sink.schema != nil {
		t.Fatal("oversized response emitted schema")
	}
}

func TestD1RowAndOutputByteLimits(t *testing.T) {
	for _, test := range []struct {
		name   string
		limits query.Limits
		result string
	}{
		{"rows", func() query.Limits { l := query.DefaultLimits(); l.MaxRows = 1; return l }(), `{"success":true,"result":[{"success":true,"results":{"columns":["x"],"rows":[[1],[2]]}}]}`},
		{"bytes", func() query.Limits { l := query.DefaultLimits(); l.MaxBytes = 1024; return l }(), `{"success":true,"result":[{"success":true,"results":{"columns":["x"],"rows":[["` + strings.Repeat("x", 1500) + `"]]}}]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := fixture(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(test.result)) }, test.limits)
			sink := new(capture)
			defer sink.release()
			_, err := e.Execute(context.Background(), request(), sink)
			code(t, err, "RESOURCE_EXHAUSTED")
		})
	}
}

func TestD1DeadlineCancelsHTTPAndRejectsWritesBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	e := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}, func() query.Limits { l := query.DefaultLimits(); l.Timeout = 40 * time.Millisecond; return l }())
	sink := new(capture)
	defer sink.release()
	write := request()
	write.SQL = "DELETE FROM events"
	if _, err := e.Execute(context.Background(), write, sink); err == nil {
		t.Fatal("write accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("write reached the network")
	}
	_, err := e.Execute(context.Background(), request(), sink)
	code(t, err, "DEADLINE_EXCEEDED")
}

func TestD1RejectsUnsafeConfiguration(t *testing.T) {
	t.Setenv("KELVO_SOURCE_D1_URL", "http://example.invalid")
	t.Setenv("KELVO_SOURCE_D1_TOKEN", "fixture")
	source := catalog.Source{ID: "d1", Type: "d1", URLEnv: "KELVO_SOURCE_D1_URL", TokenEnv: "KELVO_SOURCE_D1_TOKEN", Options: map[string]string{"account_id": account, "database_id": database}}
	if _, err := New(catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits()); err == nil {
		t.Fatal("non-HTTPS accepted")
	}
	t.Setenv("KELVO_SOURCE_D1_URL", "https://example.invalid")
	source.Options["account_id"] = "../other-account"
	if _, err := New(catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits()); err == nil {
		t.Fatal("path injection accepted")
	}
}
