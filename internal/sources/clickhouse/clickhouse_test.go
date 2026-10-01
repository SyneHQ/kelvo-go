// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type testSink struct {
	schema *arrow.Schema
	writes int
	write  func(arrow.RecordBatch) error
}

func (s *testSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *testSink) Write(record arrow.RecordBatch) error {
	s.writes++
	if s.write != nil {
		return s.write(record)
	}
	return nil
}

func fixture(t *testing.T) ([]byte, int64) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 20, Scale: 4}},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Uint64Builder).AppendValues([]uint64{9007199254740993, 18446744073709551615}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"alpha", ""}, []bool{true, false})
	builder.Field(2).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(1234567), decimal128.FromI64(-42)}, nil)
	builder.Field(3).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1720000000000001, 1720000000000002}, nil)
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
	// IPC omits unused validity bitmaps and trims builder allocation padding.
	// Delivered buffers: two uint64s; one validity byte, three int32 offsets and
	// five string bytes; two decimal128s; two int64 timestamps. The mutable
	// builder's allocation size is not the size of the decoded source result.
	expectedDecodedBytes := int64(2*8 + 1 + 3*4 + len("alpha") + 2*16 + 2*8)
	return out.Bytes(), expectedDecodedBytes
}

func newTestEngine(t *testing.T, endpoint string, limits query.Limits) *Engine {
	t.Helper()
	t.Setenv("KELVO_TEST_CLICKHOUSE_URL", endpoint)
	t.Setenv("KELVO_TEST_CLICKHOUSE_USER", "reader")
	t.Setenv("KELVO_TEST_CLICKHOUSE_PASSWORD", "test-only-password")
	engine, err := New(catalog.Config{Sources: []catalog.Source{{
		ID: "analytics", Type: "clickhouse", URLEnv: "KELVO_TEST_CLICKHOUSE_URL",
		UsernameEnv: "KELVO_TEST_CLICKHOUSE_USER", PasswordEnv: "KELVO_TEST_CLICKHOUSE_PASSWORD",
	}}}, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func request() query.Request {
	return query.Request{SQL: "SELECT id, label, amount, at FROM sample", Mode: "native", ConnectionID: "analytics"}
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var public *query.Error
	if !errors.As(err, &public) || public.Code != code {
		t.Fatalf("got error %v; want code %s", err, code)
	}
}

func TestExecuteArrowPreservesTypesAndSourceSettings(t *testing.T) {
	data, expectedBytes := fixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != request().SQL {
			t.Errorf("query body mismatch: %v", err)
		}
		username, password, ok := r.BasicAuth()
		if !ok || username != "reader" || password != "test-only-password" {
			t.Error("missing source authentication")
		}
		params := r.URL.Query()
		for key, value := range map[string]string{
			"readonly": "1", "default_format": "ArrowStream", "max_result_rows": "1000000",
			"max_result_bytes": "268435456", "result_overflow_mode": "throw", "max_execution_time": "30",
			"cancel_http_readonly_queries_on_client_close": "1", "output_format_arrow_compression_method": "none",
			"wait_end_of_query": "0", "database": "reports",
		} {
			if params.Get(key) != value {
				t.Errorf("setting %s = %q; want %q", key, params.Get(key), value)
			}
		}
		if !strings.HasPrefix(params.Get("query_id"), "kelvo-") || len(params.Get("query_id")) != 38 {
			t.Error("missing unique query ID")
		}
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		_, _ = w.Write(data)
	}))
	defer server.Close()
	engine := newTestEngine(t, server.URL+"?database=reports", query.DefaultLimits())
	sink := &testSink{write: func(record arrow.RecordBatch) error {
		for column, lengths := range [][]int{{0, 16}, {1, 12, 5}, {0, 32}, {0, 16}} {
			buffers := record.Column(column).Data().Buffers()
			if len(buffers) != len(lengths) {
				t.Errorf("column %d has %d buffers; want %d", column, len(buffers), len(lengths))
				continue
			}
			for index, expected := range lengths {
				actual := 0
				if buffers[index] != nil {
					actual = buffers[index].Len()
				}
				if actual != expected {
					t.Errorf("column %d buffer %d has %d bytes; want %d", column, index, actual, expected)
				}
			}
		}
		if record.NumRows() != 2 || record.Column(0).(*array.Uint64).Value(0) != 9007199254740993 || record.Column(0).(*array.Uint64).Value(1) != 18446744073709551615 {
			t.Error("uint64 precision lost")
		}
		labels := record.Column(1).(*array.String)
		if labels.Value(0) != "alpha" || !labels.IsNull(1) {
			t.Error("string or NULL lost")
		}
		if record.Column(2).(*array.Decimal128).Value(0) != decimal128.FromI64(1234567) || record.Column(3).(*array.Timestamp).Value(0) != 1720000000000001 {
			t.Error("decimal or timestamp precision lost")
		}
		return nil
	}}
	stats, err := engine.Execute(context.Background(), request(), sink)
	if err != nil {
		t.Fatal(err)
	}
	if sink.schema == nil || sink.writes != 1 || stats.Rows != 2 || stats.Batches != 1 || stats.Bytes != expectedBytes || stats.Backend != "clickhouse" || !stats.EngineStreaming {
		t.Fatalf("unexpected stats or sink: %+v, writes=%d", stats, sink.writes)
	}
}

func TestSourceErrorsDoNotExposeResponseOrURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, strings.Repeat("source-secret test-only-password ", 1000))
	}))
	defer server.Close()
	engine := newTestEngine(t, server.URL, query.DefaultLimits())
	_, err := engine.Execute(context.Background(), request(), &testSink{})
	requireCode(t, err, "QUERY_FAILED")
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), server.URL) {
		t.Fatal("source error leaked sensitive content")
	}
}

func TestCancellationClosesSourceRequest(t *testing.T) {
	started, closed := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(closed)
	}))
	defer server.Close()
	engine := newTestEngine(t, server.URL, query.DefaultLimits())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := engine.Execute(ctx, request(), &testSink{}); done <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("source request did not start")
	}
	cancel()
	select {
	case err := <-done:
		requireCode(t, err, "CANCELLED")
	case <-time.After(3 * time.Second):
		t.Fatal("execution did not cancel")
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("source request was not closed")
	}
}

func TestResultLimitsAreCheckedBeforeWriting(t *testing.T) {
	data, _ := fixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(data) }))
	defer server.Close()
	for _, kind := range []string{"rows", "bytes"} {
		t.Run(kind, func(t *testing.T) {
			limits := query.DefaultLimits()
			if kind == "rows" {
				limits.MaxRows = 1
			} else {
				limits.MaxBytes = 1
			}
			engine := newTestEngine(t, server.URL, limits)
			sink := &testSink{}
			stats, err := engine.Execute(context.Background(), request(), sink)
			requireCode(t, err, "RESOURCE_EXHAUSTED")
			if sink.writes != 0 || stats.Rows != 0 || stats.Bytes != 0 {
				t.Fatal("oversized batch was delivered")
			}
		})
	}
}

func TestMalformedAndIncompleteStreamsFail(t *testing.T) {
	data, _ := fixture(t)
	for name, body := range map[string][]byte{
		"text error after headers": []byte("Code: secret source error"),
		"no end marker":            data[:len(data)-8],
		"error after end marker":   append(append([]byte(nil), data...), []byte("Code: late error")...),
		"truncated batch":          data[:len(data)/2],
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) }))
			defer server.Close()
			engine := newTestEngine(t, server.URL, query.DefaultLimits())
			_, err := engine.Execute(context.Background(), request(), &testSink{})
			if err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestOversizedMetadataFailsBeforeAllocation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte{255, 255, 255, 255, 255, 255, 255, 127})
	}))
	defer server.Close()
	engine := newTestEngine(t, server.URL, query.DefaultLimits())
	_, err := engine.Execute(context.Background(), request(), &testSink{})
	requireCode(t, err, "RESOURCE_EXHAUSTED")
}

func TestMultipleBatchesAndDictionaryChanges(t *testing.T) {
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	schema := arrow.NewSchema([]arrow.Field{{Name: "category", Type: dictType}}, nil)
	var stream bytes.Buffer
	writer := ipc.NewWriter(&stream, ipc.WithSchema(schema), ipc.WithDictionaryDeltas(true))
	values := [][]string{{"alpha", "beta"}, {"alpha", "beta"}, {"gamma", "delta"}}
	for _, labels := range values {
		indicesBuilder := array.NewInt8Builder(memory.DefaultAllocator)
		indicesBuilder.AppendValues([]int8{0, 1}, nil)
		indices := indicesBuilder.NewArray()
		indicesBuilder.Release()
		labelsBuilder := array.NewStringBuilder(memory.DefaultAllocator)
		labelsBuilder.AppendValues(labels, nil)
		dictionary := labelsBuilder.NewArray()
		labelsBuilder.Release()
		column := array.NewDictionaryArray(dictType, indices, dictionary)
		indices.Release()
		dictionary.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{column}, 2)
		column.Release()
		err := writer.Write(record)
		record.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Fragment reads at boundaries unrelated to Arrow message boundaries.
		data := stream.Bytes()
		for len(data) > 0 {
			n := min(len(data), 13)
			_, _ = w.Write(data[:n])
			w.(http.Flusher).Flush()
			data = data[n:]
		}
	}))
	defer server.Close()
	engine := newTestEngine(t, server.URL, query.DefaultLimits())
	batch := 0
	sink := &testSink{write: func(record arrow.RecordBatch) error {
		if batch >= len(values) {
			t.Fatal("received an extra batch")
		}
		dictionary := record.Column(0).(*array.Dictionary)
		for row, expected := range values[batch] {
			actual := dictionary.Dictionary().(*array.String).Value(dictionary.GetValueIndex(row))
			if actual != expected {
				t.Errorf("batch %d row %d = %q; want %q", batch, row, actual, expected)
			}
		}
		batch++
		return nil
	}}
	stats, err := engine.Execute(context.Background(), request(), sink)
	if err != nil {
		t.Fatal(err)
	}
	if batch != 3 || stats.Batches != 3 || stats.Rows != 6 {
		t.Fatalf("multiple batches were not delivered: %+v", stats)
	}
}

func TestLateSourceMemoryFailureIsNotMisreportedAsDecoderLimit(t *testing.T) {
	data, _ := fixture(t)
	// A real ClickHouse 26.9 query exceeded its 512 MiB source memory budget
	// after emitting successful batches, with HTTP 200 already sent.
	data = append(append([]byte(nil), data[:len(data)-8]...), []byte("\r\n__exception__\r\nCode: 241. DB::Exception: source-secret MEMORY_LIMIT_EXCEEDED")...)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(data) }))
	defer server.Close()
	engine := newTestEngine(t, server.URL, query.DefaultLimits())
	sink := &testSink{}
	stats, err := engine.Execute(context.Background(), request(), sink)
	requireCode(t, err, "RESOURCE_EXHAUSTED")
	if !strings.Contains(err.Error(), "ClickHouse source query") || !strings.Contains(err.Error(), "241") || strings.Contains(err.Error(), "source-secret") {
		t.Fatalf("source failure classification is incorrect: %v", err)
	}
	if stats.Rows != 2 || sink.writes != 1 {
		t.Fatal("fixture did not deliver a successful batch before failing")
	}
}

func TestRequestCannotChooseEndpointOrPassUnsupportedParameters(t *testing.T) {
	engine := newTestEngine(t, "http://127.0.0.1:1/", query.DefaultLimits())
	for name, req := range map[string]query.Request{
		"URL is not a connection ID": {SQL: "SELECT 1", ConnectionID: "http://other.invalid"},
		"missing connection ID":      {SQL: "SELECT 1"},
		"parameters":                 {SQL: "SELECT ?", ConnectionID: "analytics", Parameters: []query.Parameter{{Type: "int64", Value: json.RawMessage("1")}}},
		"federation":                 {SQL: "SELECT 1", ConnectionID: "analytics", Sources: []string{"analytics"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := engine.Execute(context.Background(), req, &testSink{})
			if err == nil || strings.Contains(err.Error(), "execute source") {
				t.Fatalf("request was not rejected locally: %v", err)
			}
		})
	}
}

func TestSourceURLValidation(t *testing.T) {
	for _, endpoint := range []string{"", "file:///tmp/data", "http:///missing", "https://user:secret@example.com", "https://example.com#fragment", "https://example.com?query=SELECT+1", "https://example.com?readonly=0", "https://example.com?database=a&database=b"} {
		t.Run(endpoint, func(t *testing.T) {
			t.Setenv("KELVO_TEST_INVALID_URL", endpoint)
			_, err := New(catalog.Config{Sources: []catalog.Source{{ID: "analytics", Type: "clickhouse", URLEnv: "KELVO_TEST_INVALID_URL"}}}, query.DefaultLimits())
			requireCode(t, err, "INVALID_ARGUMENT")
			if strings.Contains(err.Error(), "secret") {
				t.Fatal("invalid URL leaked credentials")
			}
		})
	}
}

func TestRedirectDoesNotForwardCredentials(t *testing.T) {
	called := false
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	engine := newTestEngine(t, server.URL, query.DefaultLimits())
	_, err := engine.Execute(context.Background(), request(), &testSink{})
	requireCode(t, err, "QUERY_FAILED")
	if called {
		t.Fatal("followed source redirect")
	}
}
