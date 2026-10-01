// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package httpapi

import (
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
