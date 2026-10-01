// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package elasticsearch

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
	values []int64
	nulls  int
}

func (*capture) Schema(*arrow.Schema) error { return nil }
func (s *capture) Write(r arrow.RecordBatch) error {
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
	t.Setenv("ES_TEST_URL", server.URL)
	t.Setenv("ES_TEST_TOKEN", "test-only")
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "es", Type: "elasticsearch", URLEnv: "ES_TEST_URL", TokenEnv: "ES_TEST_TOKEN"}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e.client.HTTP.Transport = server.Client().Transport
	t.Cleanup(func() { e.Close() })
	return e
}
func request() query.Request {
	return query.Request{Mode: "native", ConnectionID: "es", SQL: "SELECT * FROM logs"}
}
func TestPaginationAndExactLongs(t *testing.T) {
	calls := 0
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/_sql" || r.Header.Get("Authorization") != "ApiKey test-only" {
			t.Error("invalid path or credential")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			t.Error("invalid body")
		}
		if calls == 1 {
			if body["query"] != request().SQL || body["field_multi_value_leniency"] != false {
				t.Error("wrong query options")
			}
			fmt.Fprint(w, `{"columns":[{"name":"id","type":"long"}],"rows":[[9223372036854775807],[null]],"cursor":"next"}`)
		} else {
			if body["cursor"] != "next" {
				t.Error("wrong cursor")
			}
			fmt.Fprint(w, `{"rows":[[-9223372036854775808]]}`)
		}
	})
	sink := &capture{}
	stats, err := e.Execute(context.Background(), request(), sink)
	if err != nil || stats.Rows != 3 || calls != 2 || sink.nulls != 1 || len(sink.values) != 2 || sink.values[0] != 9223372036854775807 || sink.values[1] != -9223372036854775808 {
		t.Fatalf("stats=%+v sink=%+v calls=%d err=%v", stats, sink, calls, err)
	}
}
func TestPartialUnsupportedAndOverflowCloseCursor(t *testing.T) {
	for _, payload := range []string{`{"columns":[{"name":"id","type":"long"}],"rows":[[1]],"is_partial":true,"cursor":"next"}`, `{"columns":[{"name":"id","type":"object"}],"rows":[],"cursor":"next"}`, `{"columns":[{"name":"id","type":"long"}],"rows":[[1],[2]],"cursor":"next"}`, `{"error":{"message":"private-secret"},"cursor":"next"}`} {
		t.Run(payload, func(t *testing.T) {
			var closed atomic.Bool
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/_sql/close" {
					closed.Store(true)
					fmt.Fprint(w, `{"succeeded":true}`)
					return
				}
				fmt.Fprint(w, payload)
			})
			e.limits.MaxRows = 1
			_, err := e.Execute(context.Background(), request(), &capture{})
			if err == nil || strings.Contains(err.Error(), "private-secret") || !closed.Load() {
				t.Fatalf("err=%v closed=%v", err, closed.Load())
			}
		})
	}
}
func TestCancelBetweenPagesClosesCursor(t *testing.T) {
	var closed atomic.Bool
	calls := 0
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		var body any
		json.NewDecoder(r.Body).Decode(&body)
		if r.URL.Path == "/_sql/close" {
			closed.Store(true)
			fmt.Fprint(w, `{}`)
			return
		}
		calls++
		if calls == 1 {
			fmt.Fprint(w, `{"columns":[{"name":"id","type":"long"}],"rows":[[1]],"cursor":"next"}`)
			return
		}
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	})
	e.limits.Timeout = 40 * time.Millisecond
	_, err := e.Execute(context.Background(), request(), &capture{})
	if !errors.Is(err, context.DeadlineExceeded) || !closed.Load() {
		t.Fatalf("err=%v close=%v", err, closed.Load())
	}
}

func TestParametersAreBoundWithoutSQLInterpolation(t *testing.T) {
	sql := "SELECT id FROM logs WHERE id > ? AND label = ? AND active = ? AND optional = ?"
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		d := json.NewDecoder(r.Body)
		d.UseNumber()
		var body struct {
			Query   string `json:"query"`
			Params  []any  `json:"params"`
			Partial *bool  `json:"allow_partial_search_results"`
		}
		if err := d.Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Query != sql || len(body.Params) != 4 || body.Params[0] != json.Number("9007199254740993") || body.Params[1] != "'; DELETE FROM logs; --" || body.Params[2] != true || body.Params[3] != nil || body.Partial == nil || *body.Partial {
			t.Errorf("incorrect bound parameters or partial-result setting")
		}
		fmt.Fprint(w, `{"columns":[{"name":"id","type":"long"}],"rows":[[9007199254740994]]}`)
	})
	r := request()
	r.SQL = sql
	r.Parameters = []query.Parameter{
		{Type: "int64", Value: json.RawMessage(`"9007199254740993"`)},
		{Type: "string", Value: json.RawMessage(`"'; DELETE FROM logs; --"`)},
		{Type: "bool", Value: json.RawMessage("true")},
		{Type: "null", Value: json.RawMessage("null")},
	}
	stats, err := e.Execute(context.Background(), r, &capture{})
	if err != nil || stats.Rows != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}

func TestUnsignedParameterOverflowFailsBeforeRequest(t *testing.T) {
	calls := 0
	e := setup(t, func(w http.ResponseWriter, r *http.Request) { calls++ })
	r := request()
	r.SQL = "SELECT id FROM logs WHERE id > ?"
	r.Parameters = []query.Parameter{{Type: "uint64", Value: json.RawMessage(`"18446744073709551615"`)}}
	_, err := e.Execute(context.Background(), r, &capture{})
	if err == nil || query.PublicError(err).Code != "UNSUPPORTED" || calls != 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}
