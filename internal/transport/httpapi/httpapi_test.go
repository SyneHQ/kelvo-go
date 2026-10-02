// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type fakeExecutor struct {
	calls atomic.Int32
	err   error
	rows  int
}

func (f *fakeExecutor) Execute(_ context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	f.calls.Add(1)
	if f.err != nil {
		return query.Stats{}, f.err
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	for i := 0; i < f.rows; i++ {
		builder.Append(int64(i))
	}
	values := builder.NewArray()
	defer values.Release()
	record := array.NewRecord(schema, []arrow.Array{values}, int64(f.rows))
	defer record.Release()
	if err := sink.Write(record); err != nil {
		return query.Stats{}, err
	}
	return query.Stats{Backend: "fake"}, nil
}

func newTestServer(t *testing.T, executor query.Executor, ttl time.Duration, limits query.Limits) *Server {
	t.Helper()
	s, err := New(executor, Options{Token: "test-token", TTL: ttl, MaxQueries: 4, MaxConcurrent: 1, Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func testLimits() query.Limits {
	return query.Limits{MaxRows: 10, MaxBytes: 1 << 20, Timeout: time.Second}
}
func request(t *testing.T, s http.Handler, method, path, body string, auth bool) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if auth {
		r.Header.Set("Authorization", "Bearer test-token")
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}
func create(t *testing.T, s http.Handler) string {
	t.Helper()
	w := request(t, s, http.MethodPost, "/v1/queries", `{"sql":"SELECT 1","mode":"federated","sources":["orders"]}`, true)
	if w.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", w.Code, w.Body.String())
	}
	var result struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result.ID
}

func TestAuthenticationAndHealth(t *testing.T) {
	s := newTestServer(t, &fakeExecutor{}, time.Second, testLimits())
	if got := request(t, s, http.MethodGet, "/health", "", false).Code; got != http.StatusOK {
		t.Fatalf("health=%d", got)
	}
	if got := request(t, s, http.MethodPost, "/v1/queries", `{}`, false).Code; got != http.StatusUnauthorized {
		t.Fatalf("unauthenticated=%d", got)
	}
}
func TestSingleConsumerAndCompletion(t *testing.T) {
	exec := &fakeExecutor{rows: 1}
	s := newTestServer(t, exec, time.Second, testLimits())
	id := create(t, s)
	w := request(t, s, http.MethodGet, "/v1/queries/"+id+"/results", "", true)
	if w.Code != http.StatusOK || !strings.Contains(w.Header().Get("Content-Type"), "application/vnd.apache.arrow.stream") {
		t.Fatalf("results=%d content-type=%q", w.Code, w.Header().Get("Content-Type"))
	}
	if got := request(t, s, http.MethodGet, "/v1/queries/"+id+"/results", "", true).Code; got != http.StatusConflict {
		t.Fatalf("second consumer=%d", got)
	}
	if exec.calls.Load() != 1 {
		t.Fatalf("calls=%d", exec.calls.Load())
	}
}
func TestCancelBeforeRetrievalDoesNotExecute(t *testing.T) {
	exec := &fakeExecutor{}
	s := newTestServer(t, exec, time.Second, testLimits())
	id := create(t, s)
	if got := request(t, s, http.MethodPost, "/v1/queries/"+id+"/cancel", "", true).Code; got != http.StatusOK {
		t.Fatalf("cancel=%d", got)
	}
	if got := request(t, s, http.MethodGet, "/v1/queries/"+id+"/results", "", true).Code; got != http.StatusConflict {
		t.Fatalf("results=%d", got)
	}
	if exec.calls.Load() != 0 {
		t.Fatalf("cancelled query executed %d times", exec.calls.Load())
	}
}
func TestFailureAndRowLimitAreTerminal(t *testing.T) {
	t.Run("executor failure", func(t *testing.T) {
		s := newTestServer(t, &fakeExecutor{err: query.NewError("INVALID_ARGUMENT", "bad query")}, time.Second, testLimits())
		id := create(t, s)
		if got := request(t, s, http.MethodGet, "/v1/queries/"+id+"/results", "", true).Code; got != http.StatusBadRequest {
			t.Fatalf("failure=%d", got)
		}
	})
	t.Run("row limit", func(t *testing.T) {
		limits := testLimits()
		limits.MaxRows = 1
		s := newTestServer(t, &fakeExecutor{rows: 2}, time.Second, limits)
		id := create(t, s)
		_ = request(t, s, http.MethodGet, "/v1/queries/"+id+"/results", "", true)
		status := request(t, s, http.MethodGet, "/v1/queries/"+id, "", true)
		if !strings.Contains(status.Body.String(), `"state":"failed"`) {
			t.Fatalf("status=%s", status.Body.String())
		}
	})
}
func TestTTLExpiresQueuedQuery(t *testing.T) {
	s := newTestServer(t, &fakeExecutor{}, 15*time.Millisecond, testLimits())
	id := create(t, s)
	time.Sleep(30 * time.Millisecond)
	if got := request(t, s, http.MethodGet, "/v1/queries/"+id, "", true).Code; got != http.StatusNotFound {
		t.Fatalf("expired status=%d", got)
	}
}

func TestFederatedSmokeAndTypedInputValidation(t *testing.T) {
	s := newTestServer(t, &fakeExecutor{rows: 1}, time.Second, testLimits())
	w := request(t, s, http.MethodPost, "/v1/queries", `{"sql":"SELECT 1","mode":"federated"}`, true)
	if w.Code != http.StatusCreated {
		t.Fatalf("no-source federated smoke=%d body=%s", w.Code, w.Body.String())
	}
	bad := request(t, s, http.MethodPost, "/v1/queries", `{"sql":"SELECT ?","mode":"federated","parameters":[{"type":"int64","value":"not-an-int"}]}`, true)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("typed validation=%d", bad.Code)
	}
}

func TestMongoRequestUsesSharedValidation(t *testing.T) {
	s := newTestServer(t, &fakeExecutor{rows: 1}, time.Second, testLimits())
	valid := `{"mode":"native","connection_id":"documents","mongo":{"collection":"orders","pipeline":[{"$match":{"amount":{"$numberLong":"9223372036854775807"}}}]}}`
	w := request(t, s, http.MethodPost, "/v1/queries", valid, true)
	if w.Code != http.StatusCreated {
		t.Fatalf("Mongo submission=%d %s", w.Code, w.Body.String())
	}
	for _, body := range []string{
		`{"mode":"native","connection_id":"documents","sql":"SELECT 1","mongo":{"collection":"orders","pipeline":[]}}`,
		`{"mode":"federated","mongo":{"collection":"orders","pipeline":[]}}`,
		`{"mode":"native","connection_id":"documents","sources":["other"],"mongo":{"collection":"orders","pipeline":[]}}`,
		`{"mode":"native","connection_id":"documents","mongo":{"collection":"orders","pipeline":{}}}`,
		`{"mode":"native","connection_id":"documents","mongo":{"collection":"orders","pipeline":[],"unknown":true}}`,
		`{"mode":"native","connection_id":"not.a.catalog.id","sql":"SELECT 1"}`,
	} {
		if got := request(t, s, http.MethodPost, "/v1/queries", body, true).Code; got != http.StatusBadRequest {
			t.Fatalf("accepted invalid request (%d): %s", got, body)
		}
	}
}

type blockingExecutor struct{ started chan struct{} }

func (f *blockingExecutor) Execute(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	close(f.started)
	<-ctx.Done()
	return query.Stats{}, ctx.Err()
}

func TestActiveCancelAndCapacityBound(t *testing.T) {
	exec := &blockingExecutor{started: make(chan struct{})}
	s := newTestServer(t, exec, time.Second, testLimits())
	first := create(t, s)
	done := make(chan struct{})
	go func() { _ = request(t, s, http.MethodGet, "/v1/queries/"+first+"/results", "", true); close(done) }()
	<-exec.started
	second := create(t, s)
	if got := request(t, s, http.MethodGet, "/v1/queries/"+second+"/results", "", true).Code; got != http.StatusTooManyRequests {
		t.Fatalf("capacity=%d", got)
	}
	if got := request(t, s, http.MethodPost, "/v1/queries/"+first+"/cancel", "", true).Code; got != http.StatusOK {
		t.Fatalf("cancel=%d", got)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("active cancellation did not complete")
	}
	status := request(t, s, http.MethodGet, "/v1/queries/"+first, "", true)
	if !strings.Contains(status.Body.String(), `"state":"cancelled"`) {
		t.Fatalf("status=%s", status.Body.String())
	}
}
func TestExpiryCancelsActiveExecution(t *testing.T) {
	exec := &blockingExecutor{started: make(chan struct{})}
	s := newTestServer(t, exec, 15*time.Millisecond, testLimits())
	id := create(t, s)
	done := make(chan struct{})
	go func() { _ = request(t, s, http.MethodGet, "/v1/queries/"+id+"/results", "", true); close(done) }()
	<-exec.started
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expiry did not cancel execution")
	}
}

func TestCloseRejectsHealthAndQueryRoutes(t *testing.T) {
	s := newTestServer(t, &fakeExecutor{}, time.Second, testLimits())
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := request(t, s, http.MethodGet, "/health", "", false).Code; got != http.StatusServiceUnavailable {
		t.Fatalf("health after close=%d", got)
	}
	if got := request(t, s, http.MethodPost, "/v1/queries", `{"sql":"SELECT 1","mode":"federated"}`, true).Code; got != http.StatusServiceUnavailable {
		t.Fatalf("query after close=%d", got)
	}
}

// Result compression is independent of the connector: these borrowed batches
// have already crossed the worker's validated, uncompressed local pipe.
type repeatedBatchExecutor struct{ batches, rows int }

func (e repeatedBatchExecutor) Execute(_ context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for batch := 0; batch < e.batches; batch++ {
		for row := 0; row < e.rows; row++ {
			builder.Field(0).(*array.Int64Builder).Append(int64(batch*e.rows + row))
			if row%7 == 0 {
				builder.Field(1).(*array.StringBuilder).AppendNull()
			} else {
				builder.Field(1).(*array.StringBuilder).Append(strings.Repeat("Kelvo α", 16))
			}
		}
		record := builder.NewRecordBatch()
		err := sink.Write(record)
		record.Release()
		if err != nil {
			return query.Stats{}, err
		}
	}
	return query.Stats{Backend: "fixture"}, nil
}

func TestResultCompressionPreservesMultiBatchHTTPValues(t *testing.T) {
	const batches, rows = 3, 1024
	encoded := make(map[string][]byte)
	for _, codec := range []string{"", "none", "lz4_frame"} {
		t.Run(codec, func(t *testing.T) {
			limits := testLimits()
			limits.MaxRows = batches * rows
			limits.ResultCompression = codec
			s := newTestServer(t, repeatedBatchExecutor{batches: batches, rows: rows}, time.Second, limits)
			id := create(t, s)
			response := request(t, s, http.MethodGet, "/v1/queries/"+id+"/results", "", true)
			if response.Code != http.StatusOK || response.Header().Get("Content-Encoding") != "" {
				t.Fatalf("Arrow body compression is not HTTP Content-Encoding: status=%d headers=%v", response.Code, response.Header())
			}
			encoded[codec] = bytes.Clone(response.Body.Bytes())
			if !bytes.HasSuffix(encoded[codec], []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
				t.Fatal("completed stream has no Arrow EOS")
			}
			reader, err := ipc.NewReader(bytes.NewReader(encoded[codec]))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Release()
			batch := 0
			for reader.Next() {
				record := reader.RecordBatch()
				if record.NumRows() != rows || record.NumCols() != 2 {
					t.Fatalf("unexpected record shape: %d rows, %d columns", record.NumRows(), record.NumCols())
				}
				ids := record.Column(0).(*array.Int64)
				texts := record.Column(1).(*array.String)
				for row := 0; row < rows; row++ {
					if ids.Value(row) != int64(batch*rows+row) || texts.IsNull(row) != (row%7 == 0) {
						t.Fatalf("batch=%d row=%d changed integer or NULL", batch, row)
					}
					if !texts.IsNull(row) && texts.Value(row) != strings.Repeat("Kelvo α", 16) {
						t.Fatalf("batch=%d row=%d changed string", batch, row)
					}
				}
				batch++
			}
			if reader.Err() != nil || batch != batches {
				t.Fatalf("decoded %d batches: %v", batch, reader.Err())
			}
			status := request(t, s, http.MethodGet, "/v1/queries/"+id, "", true)
			var result struct {
				State string      `json:"state"`
				Stats query.Stats `json:"stats"`
			}
			if err := json.Unmarshal(status.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.State != "succeeded" || result.Stats.Rows != batches*rows || result.Stats.Batches != batches || result.Stats.WireBytes != int64(len(encoded[codec])) {
				t.Fatalf("incorrect terminal accounting: %+v", result)
			}
		})
	}
	if !bytes.Equal(encoded[""], encoded["none"]) {
		t.Fatal("explicit none changed the default Arrow wire format")
	}
	if len(encoded["lz4_frame"])*2 >= len(encoded["none"]) {
		t.Fatalf("repeated fixture was not compressed: lz4=%d none=%d", len(encoded["lz4_frame"]), len(encoded["none"]))
	}
}

func TestCompressedHTTPResultKeepsRowAndByteLimits(t *testing.T) {
	for _, tc := range []struct {
		name     string
		executor query.Executor
		maxRows  int64
		maxBytes int64
	}{
		{name: "rows", executor: repeatedBatchExecutor{batches: 2, rows: 1024}, maxRows: 1024, maxBytes: 1 << 20},
		{name: "decoded bytes", executor: repeatedBatchExecutor{batches: 2, rows: 1024}, maxRows: 2048, maxBytes: 150000},
		{name: "encoded bytes", executor: &fakeExecutor{rows: 1}, maxRows: 1, maxBytes: 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits := testLimits()
			limits.ResultCompression = "lz4_frame"
			limits.MaxRows, limits.MaxBytes = tc.maxRows, tc.maxBytes
			s := newTestServer(t, tc.executor, time.Second, limits)
			id := create(t, s)
			response := request(t, s, http.MethodGet, "/v1/queries/"+id+"/results", "", true)
			status := request(t, s, http.MethodGet, "/v1/queries/"+id, "", true)
			if !strings.Contains(status.Body.String(), `"state":"failed"`) || !strings.Contains(status.Body.String(), `"code":"RESOURCE_EXHAUSTED"`) {
				t.Fatalf("limit was not terminal: %s", status.Body.String())
			}
			if bytes.HasSuffix(response.Body.Bytes(), []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
				t.Fatal("failed compressed stream included successful EOS")
			}
		})
	}
}

func TestHTTPRejectsUnknownResultCompressionBeforeExecution(t *testing.T) {
	for _, codec := range []string{"lz4", "zstd", "LZ4_FRAME"} {
		executor := &fakeExecutor{}
		limits := testLimits()
		limits.ResultCompression = codec
		s, err := New(executor, Options{Token: "test-token", TTL: time.Second, MaxQueries: 1, MaxConcurrent: 1, Limits: limits})
		if err == nil {
			s.Close()
			t.Fatalf("unsupported codec %q accepted", codec)
		}
		if executor.calls.Load() != 0 {
			t.Fatal("invalid configuration executed a query")
		}
	}
	s := newTestServer(t, &fakeExecutor{}, time.Second, testLimits())
	response := request(t, s, http.MethodPost, "/v1/queries", `{"sql":"SELECT 1","mode":"federated","result_compression":"lz4_frame"}`, true)
	if response.Code != http.StatusBadRequest {
		t.Fatal("request overrode operator result compression")
	}
}

type dictionaryFailureExecutor struct {
	allocator       memory.Allocator
	ignoreSinkError bool
}

func (e dictionaryFailureExecutor) Execute(_ context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	dictionaryType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	schema := arrow.NewSchema([]arrow.Field{{Name: "label", Type: dictionaryType}}, nil)
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	indicesBuilder := array.NewInt8Builder(e.allocator)
	indicesBuilder.AppendValues([]int8{0, 1}, nil)
	indices := indicesBuilder.NewArray()
	indicesBuilder.Release()
	defer indices.Release()
	labelsBuilder := array.NewStringBuilder(e.allocator)
	labelsBuilder.AppendValues([]string{"north", "south"}, nil)
	labels := labelsBuilder.NewArray()
	labelsBuilder.Release()
	defer labels.Release()
	column := array.NewDictionaryArray(dictionaryType, indices, labels)
	defer column.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 2)
	defer record.Release()
	if err := sink.Write(record); err != nil {
		return query.Stats{}, err
	}
	if e.ignoreSinkError {
		_ = sink.Write(record)
		return query.Stats{}, nil
	}
	return query.Stats{}, query.NewError("QUERY_FAILED", "fixture failed after a valid batch")
}

func TestFailedHTTPResultReleasesDictionariesWithoutEOS(t *testing.T) {
	for _, codec := range []string{"none", "lz4_frame"} {
		for _, ignoreSinkError := range []bool{false, true} {
			name := codec + "/execution-error"
			if ignoreSinkError {
				name = codec + "/ignored-sink-error"
			}
			t.Run(name, func(t *testing.T) {
				allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
				limits := testLimits()
				limits.ResultCompression = codec
				limits.MaxRows = 2
				executor := dictionaryFailureExecutor{allocator: allocator, ignoreSinkError: ignoreSinkError}
				s := newTestServer(t, executor, time.Second, limits)
				id := create(t, s)
				response := request(t, s, http.MethodGet, "/v1/queries/"+id+"/results", "", true)
				allocator.AssertSize(t, 0)
				if bytes.HasSuffix(response.Body.Bytes(), []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
					t.Fatal("failed result cleanup emitted EOS")
				}
				status := request(t, s, http.MethodGet, "/v1/queries/"+id, "", true)
				var result struct {
					State string      `json:"state"`
					Stats query.Stats `json:"stats"`
				}
				if err := json.Unmarshal(status.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if result.State != "failed" || result.Stats.WireBytes != int64(response.Body.Len()) {
					t.Fatalf("cleanup changed failure state or wire accounting: %+v", result)
				}
			})
		}
	}
}
