// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/SYNEHQ/kelvo-go/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const clientFixtureToken = "fixture-token-for-client-protocol-tests-only-0001"

type clientFixtureSink struct {
	schema func(*arrow.Schema) error
	write  func(arrow.RecordBatch) error
}

func (s *clientFixtureSink) Schema(schema *arrow.Schema) error {
	if s.schema != nil {
		return s.schema(schema)
	}
	return nil
}

func (s *clientFixtureSink) Write(batch arrow.RecordBatch) error {
	if s.write != nil {
		return s.write(batch)
	}
	return nil
}

func clientFixtureRequest() query.Request {
	return query.Request{SQL: "SELECT value FROM source", Mode: "native", ConnectionID: "source"}
}

func clientFixtureIPC(t *testing.T) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "signed", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "unsigned", Type: arrow.PrimitiveTypes.Uint64},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{math.MaxInt64, -9007199254740993, 0}, []bool{true, true, false})
	builder.Field(1).(*array.Uint64Builder).AppendValues([]uint64{math.MaxUint64, 9007199254740993, 0}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()
	var out bytes.Buffer
	writer := ipc.NewWriter(&out, ipc.WithSchema(schema))
	if err := writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func clientFixtureConfig(t *testing.T, server *httptest.Server) Config {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	return Config{URL: server.URL, BearerToken: clientFixtureToken, TLSConfig: &tls.Config{RootCAs: roots}, Timeout: 5 * time.Second, MaxConcurrent: 1, MaxRows: 100, MaxDecodedBytes: 1 << 20, MaxWireBytes: 1 << 20}
}

func clientFixtureServer(t *testing.T, handler http.HandlerFunc, tlsMaximum uint16) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, MaxVersion: tlsMaximum}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func clientFixtureClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	client, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("client cleanup: %v", err)
		}
	})
	return client
}

func clientFixtureAccepted(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = io.WriteString(w, `{"id":"fixture_handle","state":"queued"}`)
}

func clientFixtureResult(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.Header().Set("Kelvo-Result-Completion", "durable-eos-v1")
	_, _ = w.Write(data)
}

func clientFixtureError(t *testing.T, err error, want string) {
	t.Helper()
	var actual *Error
	if !errors.As(err, &actual) || actual.Code != want {
		t.Fatalf("error = %v; want code %s", err, want)
	}
	if strings.Contains(err.Error(), clientFixtureToken) || strings.Contains(err.Error(), "remote-secret-diagnostic") {
		t.Fatal("error disclosed fixture credentials or upstream diagnostics")
	}
}

func clientFixtureAwait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("fixture did not finish")
		var zero T
		return zero
	}
}
