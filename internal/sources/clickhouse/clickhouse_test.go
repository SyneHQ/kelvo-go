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
	"sync/atomic"
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

func fixture(t *testing.T, options ...ipc.Option) ([]byte, int64) {
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
	writer := ipc.NewWriter(&out, append([]ipc.Option{ipc.WithSchema(schema)}, options...)...)
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

func newTestEngine(t *testing.T, endpoint string, limits query.Limits, options ...map[string]string) *Engine {
	t.Helper()
	t.Setenv("KELVO_TEST_CLICKHOUSE_URL", endpoint)
	t.Setenv("KELVO_TEST_CLICKHOUSE_USER", "reader")
	t.Setenv("KELVO_TEST_CLICKHOUSE_PASSWORD", "test-only-password")
	var sourceOptions map[string]string
	if len(options) != 0 {
		sourceOptions = options[0]
	}
	engine, err := New(catalog.Config{Sources: []catalog.Source{{
		ID: "analytics", Type: "clickhouse", URLEnv: "KELVO_TEST_CLICKHOUSE_URL",
		UsernameEnv: "KELVO_TEST_CLICKHOUSE_USER", PasswordEnv: "KELVO_TEST_CLICKHOUSE_PASSWORD",
		Options: sourceOptions,
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
	for _, codec := range []string{"", "none", "lz4_frame"} {
		t.Run("compression="+codec, func(t *testing.T) { testExecuteArrowPreservesTypesAndSourceSettings(t, codec) })
	}
}

func testExecuteArrowPreservesTypesAndSourceSettings(t *testing.T, codec string) {
	var options map[string]string
	var writerOptions []ipc.Option
	if codec == "" {
		codec = "none"
	} else {
		options = map[string]string{"arrow_compression": codec}
	}
	if codec == "lz4_frame" {
		writerOptions = append(writerOptions, ipc.WithLZ4())
	}
	data, expectedBytes := fixture(t, writerOptions...)
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
			"cancel_http_readonly_queries_on_client_close": "1", "output_format_arrow_compression_method": codec,
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
	engine := newTestEngine(t, server.URL+"?database=reports", query.DefaultLimits(), options)
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
	if sink.schema == nil || sink.writes != 1 || stats.Rows != 2 || stats.Batches != 1 || stats.Bytes != expectedBytes || stats.SourceWireBytes != int64(len(data)) || stats.Backend != "clickhouse" || !stats.EngineStreaming {
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
	for _, codec := range []string{"none", "lz4_frame"} {
		t.Run(codec, func(t *testing.T) { testMultipleBatchesAndDictionaryChanges(t, codec) })
	}
}

func testMultipleBatchesAndDictionaryChanges(t *testing.T, codec string) {
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	schema := arrow.NewSchema([]arrow.Field{{Name: "category", Type: dictType}}, nil)
	var stream bytes.Buffer
	options := []ipc.Option{ipc.WithSchema(schema), ipc.WithDictionaryDeltas(true)}
	if codec == "lz4_frame" {
		options = append(options, ipc.WithLZ4())
	}
	writer := ipc.NewWriter(&stream, options...)
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
	engine := newTestEngine(t, server.URL, query.DefaultLimits(), map[string]string{"arrow_compression": codec})
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
	if batch != 3 || stats.Batches != 3 || stats.Rows != 6 || stats.SourceWireBytes != int64(stream.Len()) {
		t.Fatalf("multiple batches were not delivered: %+v", stats)
	}
}

func TestCompressionOptionRejectsUnsupportedValuesBeforeHTTP(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1) }))
	defer server.Close()
	t.Setenv("KELVO_TEST_CLICKHOUSE_URL", server.URL)
	for _, codec := range []string{"", "lz4", "zstd", "LZ4_FRAME", "lz4_frame&readonly=0", "private-invalid-value"} {
		t.Run(codec, func(t *testing.T) {
			_, err := New(catalog.Config{Sources: []catalog.Source{{ID: "analytics", Type: "clickhouse",
				URLEnv: "KELVO_TEST_CLICKHOUSE_URL", Options: map[string]string{"arrow_compression": codec}}}}, query.DefaultLimits())
			requireCode(t, err, "INVALID_ARGUMENT")
			if strings.Contains(err.Error(), "private-invalid-value") {
				t.Fatal("invalid option value leaked into error")
			}
		})
	}
	if requests.Load() != 0 {
		t.Fatal("invalid compression option reached the source")
	}
}

func TestCompressionOptionIsFrozenAtConstruction(t *testing.T) {
	data, _ := fixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("output_format_arrow_compression_method") != "none" {
			t.Error("caller mutation changed the validated compression option")
		}
		_, _ = w.Write(data)
	}))
	defer server.Close()
	options := map[string]string{"arrow_compression": "none"}
	engine := newTestEngine(t, server.URL, query.DefaultLimits(), options)
	options["arrow_compression"] = "zstd"
	if _, err := engine.Execute(context.Background(), request(), &testSink{}); err != nil {
		t.Fatal(err)
	}
}

func TestLZ4MalformedAndIncompleteStreamsFail(t *testing.T) {
	data, _ := fixture(t, ipc.WithLZ4())
	brokenCodec := append([]byte(nil), data...)
	frame := bytes.Index(brokenCodec, []byte{0x04, 0x22, 0x4d, 0x18})
	if frame < 0 {
		t.Fatal("fixture has no LZ4 frame")
	}
	brokenCodec[frame] ^= 0xff
	for name, body := range map[string][]byte{
		"invalid codec frame": brokenCodec,
		"truncated batch":     data[:len(data)/2],
		"missing EOS":         data[:len(data)-8],
		"trailing error":      append(append([]byte(nil), data...), []byte("private-source-error")...),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) }))
			defer server.Close()
			engine := newTestEngine(t, server.URL, query.DefaultLimits(), map[string]string{"arrow_compression": "lz4_frame"})
			stats, err := engine.Execute(context.Background(), request(), &testSink{})
			requireCode(t, err, "QUERY_FAILED")
			if strings.Contains(err.Error(), "private-source-error") || stats.SourceWireBytes <= 0 || stats.SourceWireBytes > int64(len(body)) {
				t.Fatalf("incorrect failed-stream evidence: stats=%+v error=%v", stats, err)
			}
		})
	}
}

func TestLZ4DecodedLimitsRemainEnforced(t *testing.T) {
	// A small, compressible 2 MiB value exercises expansion without allocating a
	// giant fixture. Both cases admit its encoded bytes but reject decoded data.
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	builder.Append(strings.Repeat("x", 2<<20))
	column := builder.NewArray()
	builder.Release()
	defer column.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 1)
	defer record.Release()
	var encoded bytes.Buffer
	writer := ipc.NewWriter(&encoded, ipc.WithSchema(schema), ipc.WithLZ4())
	if err := writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if encoded.Len() >= 1<<20 {
		t.Fatal("fixture did not compress below the allocation budget")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(encoded.Bytes()) }))
	defer server.Close()
	for _, limit := range []string{"decoded bytes", "decoded allocation"} {
		t.Run(limit, func(t *testing.T) {
			limits := query.DefaultLimits()
			if limit == "decoded bytes" {
				limits.MaxBytes = int64(encoded.Len()) + 1024
			} else {
				limits.MemoryMB = 1
			}
			engine := newTestEngine(t, server.URL, limits, map[string]string{"arrow_compression": "lz4_frame"})
			sink := &testSink{}
			stats, err := engine.Execute(context.Background(), request(), sink)
			requireCode(t, err, "RESOURCE_EXHAUSTED")
			if sink.writes != 0 || stats.Rows != 0 || stats.Bytes != 0 || stats.SourceWireBytes <= 0 || stats.SourceWireBytes > int64(encoded.Len()) {
				t.Fatalf("decoded limit failed before delivery: stats=%+v writes=%d", stats, sink.writes)
			}
		})
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
