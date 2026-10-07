// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/SYNEHQ/kelvo-go/client"
	"github.com/SYNEHQ/kelvo-go/query"
	"github.com/SYNEHQ/kelvo-go/resolver"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type publicSink struct{ rows int64 }

func (s *publicSink) Schema(*arrow.Schema) error           { return nil }
func (s *publicSink) Write(record arrow.RecordBatch) error { s.rows += record.NumRows(); return nil }

var _ query.Sink = (*publicSink)(nil)
var _ resolver.LeaseVerifier = (*client.Client)(nil)

func TestExternalConsumerQueryLifecycle(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "count", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.Append(7)
	column := builder.NewArray()
	defer column.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 1)
	defer record.Release()
	var data bytes.Buffer
	writer := ipc.NewWriter(&data, ipc.WithSchema(schema))
	if err := writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	state := "queued"
	var stateMu sync.Mutex
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stateMu.Lock()
		defer stateMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/queries":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(client.QueryHandle{ID: "public-query", State: state})
		case "/v1/queries/public-query":
			_ = json.NewEncoder(w).Encode(client.QueryStatus{ID: "public-query", State: state, Stats: query.Stats{Rows: 1, Batches: 1, WireBytes: int64(data.Len())}})
		case "/v1/queries/public-query/results":
			state = "succeeded"
			w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
			_, _ = w.Write(data.Bytes())
		case "/v1/queries/public-query/cancel":
			_ = json.NewEncoder(w).Encode(client.QueryHandle{ID: "public-query", State: state})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	c, err := client.New(client.Config{URL: server.URL, TLSConfig: &tls.Config{RootCAs: roots}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	auth := client.Authority{}
	handle, err := c.Submit(ctx, query.Request{Mode: "native", ConnectionID: "configured_source", SQL: "SELECT 7"}, auth)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := c.Status(ctx, handle.ID, auth); err != nil || status.State != "queued" {
		t.Fatalf("status: %+v %v", status, err)
	}
	sink := &publicSink{}
	if stats, err := c.Results(ctx, handle.ID, auth, sink); err != nil || stats.Rows != 1 || sink.rows != 1 {
		t.Fatalf("result: %+v %v", stats, err)
	}
	if handle, err := c.Cancel(ctx, handle.ID, auth); err != nil || handle.State != "succeeded" {
		t.Fatalf("cancel: %+v %v", handle, err)
	}
}
