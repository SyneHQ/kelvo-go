// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package dynamodb

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

type capture struct {
	schema    *arrow.Schema
	documents []string
	fail      bool
}

func (s *capture) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *capture) Write(record arrow.RecordBatch) error {
	if s.fail {
		return errors.New("sink disconnected")
	}
	column := record.Column(0).(*array.Binary)
	for i := 0; i < column.Len(); i++ {
		if column.IsNull(i) {
			return errors.New("null document")
		}
		s.documents = append(s.documents, string(column.Value(i)))
	}
	return nil
}
func setup(t *testing.T, h http.HandlerFunc) *Engine {
	t.Helper()
	server := httptest.NewTLSServer(h)
	t.Cleanup(server.Close)
	t.Setenv("KELVO_SOURCE_DDB_TEST_URL", server.URL)
	t.Setenv("KELVO_SOURCE_DDB_TEST_ID", "ddb-id")
	t.Setenv("KELVO_SOURCE_DDB_TEST_SECRET", "private-ddb-secret")
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "documents", Type: "dynamodb", URLEnv: "KELVO_SOURCE_DDB_TEST_URL", UsernameEnv: "KELVO_SOURCE_DDB_TEST_ID", PasswordEnv: "KELVO_SOURCE_DDB_TEST_SECRET", Options: map[string]string{"region": "us-east-1"}}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e.c.HTTP.Transport = server.Client().Transport
	t.Cleanup(func() { _ = e.Close() })
	return e
}
func req() query.Request {
	return query.Request{SQL: `SELECT * FROM "facts"`, Mode: "native", ConnectionID: "documents"}
}

const fullDocument = `{"id":{"N":"12345678901234567890123456789012345678"},"decimal":{"N":"1.2345678901234567890123456789012345678E-100"},"null":{"NULL":true},"bool":{"BOOL":false},"binary":{"B":"AP8="},"text":{"S":""},"strings":{"SS":["one","two"]},"numbers":{"NS":["1","2.0000000000000000000000000000000000001"]},"binaries":{"BS":["AQ==","Ag=="]},"list":{"L":[{"M":{"v":{"S":"nested"}}},{"NULL":true}]},"empty":{"M":{}}}`

func TestLosslessDocumentsPaginationAndEmptyPages(t *testing.T) {
	var calls atomic.Int32
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["Statement"] != req().SQL || body["Limit"] != float64(1000) || r.Header.Get("X-Amz-Target") != "DynamoDB_20120810.ExecuteStatement" || !strings.Contains(r.Header.Get("Authorization"), "/us-east-1/dynamodb/aws4_request") {
			t.Error("incorrect request or credential scope")
		}
		switch call {
		case 1:
			if _, ok := body["NextToken"]; ok {
				t.Error("unexpected initial token")
			}
			fmt.Fprint(w, `{"Items":[],"NextToken":"opaque-1"}`)
		case 2:
			if body["NextToken"] != "opaque-1" {
				t.Error("missing token")
			}
			fmt.Fprintf(w, `{"Items":[%s],"NextToken":"opaque-2"}`, fullDocument)
		case 3:
			if body["NextToken"] != "opaque-2" {
				t.Error("missing second token")
			}
			fmt.Fprint(w, `{"Items":[{"different":{"S":"shape"}}]}`)
		default:
			t.Error("replayed query")
		}
	})
	sink := &capture{}
	stats, err := e.Execute(context.Background(), req(), sink)
	if err != nil || stats.Rows != 2 || stats.Batches != 1 || stats.Backend != "dynamodb" || !stats.EngineStreaming || stats.WireBytes == 0 || stats.DurationNS == 0 || stats.PrepareNS == 0 || calls.Load() != 3 {
		t.Fatalf("stats=%+v err=%v calls=%d", stats, err, calls.Load())
	}
	if len(sink.documents) != 2 || sink.documents[0] != fullDocument || sink.documents[1] != `{"different":{"S":"shape"}}` {
		t.Fatal("tagged AttributeValue JSON was changed")
	}
	if sink.schema.NumFields() != 1 || sink.schema.Field(0).Name != "document" || sink.schema.Field(0).Type.ID() != arrow.BINARY || sink.schema.Field(0).Nullable {
		t.Fatal("unexpected document schema")
	}
}
func TestEmptyResultHasStableSchema(t *testing.T) {
	e := setup(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"Items":[]}`) })
	sink := &capture{}
	stats, err := e.Execute(context.Background(), req(), sink)
	if err != nil || stats.Rows != 0 || sink.schema == nil || !sink.schema.Equal(documentSchema) {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}
func TestPaginationRejectsCycleAndUnusableLastKey(t *testing.T) {
	for _, test := range []struct {
		name, body string
		calls      int32
	}{
		{"cycle", `{"Items":[],"NextToken":"same"}`, 2},
		{"key only", `{"Items":[],"LastEvaluatedKey":{"id":{"N":"1"}}}`, 1},
		{"oversize token", `{"Items":[],"NextToken":"` + strings.Repeat("x", 32769) + `"}`, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			e := setup(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, test.body) })
			_, err := e.Execute(context.Background(), req(), &capture{})
			if err == nil || calls.Load() != test.calls {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
		})
	}
}
func TestReadOnlySourceBindingAndParameters(t *testing.T) {
	var calls atomic.Int32
	e := setup(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, `{"Items":[]}`) })
	for _, sql := range []string{"UPDATE facts SET v=1", "DELETE FROM facts", "INSERT INTO facts VALUE {}", "SELECT * FROM facts; DELETE FROM facts", "WITH data AS (SELECT 1) SELECT * FROM data", "SELECT " + strings.Repeat(" ", 8192) + "1"} {
		r := req()
		r.SQL = sql
		if _, err := e.Execute(context.Background(), r, &capture{}); err == nil {
			t.Fatalf("accepted %q", sql)
		}
	}
	r := req()
	r.ConnectionID = "other"
	if _, err := e.Execute(context.Background(), r, &capture{}); err == nil {
		t.Fatal("source escape")
	}
	r = req()
	r.SQL = "SELECT * FROM facts WHERE id=?"
	r.Parameters = []query.Parameter{{Type: "int64", Value: json.RawMessage(`1`)}}
	if _, err := e.Execute(context.Background(), r, &capture{}); err == nil {
		t.Fatal("ignored parameters")
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request contacted AWS")
	}
	r = req()
	r.SQL = "/* read */ -- allowed\nSELECT * FROM facts"
	if _, err := e.Execute(context.Background(), r, &capture{}); err != nil {
		t.Fatal(err)
	}
}
func TestLimitsFailureCancellationAndNoReplay(t *testing.T) {
	for _, test := range []string{"row", "bytes", "deadline", "provider", "sink", "malformed"} {
		t.Run(test, func(t *testing.T) {
			var calls atomic.Int32
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch test {
				case "row":
					fmt.Fprint(w, `{"Items":[{"id":{"N":"1"}},{"id":{"N":"2"}}]}`)
				case "bytes":
					fmt.Fprintf(w, `{"Items":[{"data":{"S":%q}}]}`, strings.Repeat("x", 2048))
				case "deadline":
					_, _ = io.Copy(io.Discard, r.Body)
					<-r.Context().Done()
				case "provider":
					w.WriteHeader(429)
					fmt.Fprint(w, `private-ddb-secret`)
				case "sink":
					fmt.Fprint(w, `{"Items":[{"id":{"N":"1"}}]}`)
				case "malformed":
					fmt.Fprint(w, `{"Items":[{"id":{"N":123}}]}`)
				}
			})
			if test == "row" {
				e.l.MaxRows = 1
			}
			if test == "bytes" {
				e.l.MaxBytes = 1024
				e.c.Limit = 1024
			}
			if test == "deadline" {
				e.l.Timeout = 30 * time.Millisecond
			}
			_, err := e.Execute(context.Background(), req(), &capture{fail: test == "sink"})
			if err == nil || calls.Load() != 1 || strings.Contains(err.Error(), "private-ddb-secret") {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
			if (test == "row" || test == "bytes") && query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
				t.Fatal(err)
			}
			if test == "deadline" && query.PublicError(err).Code != "DEADLINE_EXCEEDED" {
				t.Fatal(err)
			}
		})
	}
}
func TestAggregateWireBudgetBoundsEmptyPages(t *testing.T) {
	var calls atomic.Int32
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		fmt.Fprintf(w, `{"Items":[],"NextToken":"%d","padding":%q}`, n, strings.Repeat("x", 400))
	})
	e.l.MaxBytes = 1024
	_, err := e.Execute(context.Background(), req(), &capture{})
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" || calls.Load() != 3 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}
