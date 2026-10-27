// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package pgwire

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// pgError is sent as an ErrorResponse.
type pgError struct{ code, msg string }

func (e *pgError) Error() string { return e.msg }

// pipeWriter is an in-process http.ResponseWriter whose body is read as it is written.
type pipeWriter struct {
	header http.Header
	status int
	ready  chan struct{}
	once   sync.Once
	pw     *io.PipeWriter
}

func (w *pipeWriter) Header() http.Header { return w.header }
func (w *pipeWriter) WriteHeader(code int) {
	w.once.Do(func() { w.status = code; close(w.ready) })
}
func (w *pipeWriter) Write(p []byte) (int, error) { w.WriteHeader(http.StatusOK); return w.pw.Write(p) }
func (w *pipeWriter) Flush()                      {}

// response is one in-flight handler call; done closes when ServeHTTP returns.
type response struct {
	status int
	body   *io.PipeReader
	done   chan struct{}
}

func (r *response) close() { _ = r.body.Close(); <-r.done }

// call invokes the HTTP query API handler exactly as a remote client would.
func (s *Server) call(ctx context.Context, method, path, key, remote string, body any) (*response, error) {
	var reader io.Reader = http.NoBody
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://kelvo.internal"+path, reader)
	if err != nil {
		return nil, err
	}
	req.RemoteAddr = remote
	req.Header.Set("Authorization", "Bearer "+key)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	pr, pw := io.Pipe()
	w := &pipeWriter{header: http.Header{}, ready: make(chan struct{}), pw: pw}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if p := recover(); p != nil {
				w.WriteHeader(http.StatusInternalServerError)
				pw.CloseWithError(errors.New("query handler failed"))
			}
		}()
		s.handler.ServeHTTP(w, req)
		w.WriteHeader(http.StatusOK)
		_ = pw.Close()
	}()
	<-w.ready
	return &response{status: w.status, body: pr, done: done}, nil
}

// jsonCall performs a small JSON request and decodes success into out.
func (s *Server) jsonCall(ctx context.Context, method, path, key, remote string, body, out any) *pgError {
	r, err := s.call(ctx, method, path, key, remote, body)
	if err != nil {
		return &pgError{code: "XX000", msg: "query request failed"}
	}
	defer r.close()
	data, err := io.ReadAll(io.LimitReader(r.body, 1<<20))
	if err != nil {
		return transportError(ctx)
	}
	if r.status/100 != 2 {
		return httpError(r.status, data)
	}
	if out != nil && json.Unmarshal(data, out) != nil {
		return &pgError{code: "XX000", msg: "invalid query service response"}
	}
	return nil
}

// sqlstate maps public Kelvo error codes.
var sqlstate = map[string]string{
	"UNAUTHENTICATED":    "28P01",
	"PERMISSION_DENIED":  "42501",
	"INVALID_ARGUMENT":   "22023",
	"NOT_FOUND":          "42704",
	"RESOURCE_EXHAUSTED": "53000",
	"UNAVAILABLE":        "57P03",
	"DEADLINE_EXCEEDED":  "57014",
	"CANCELLED":          "57014",
	"UNSUPPORTED":        "0A000",
	"CONFLICT":           "55000",
	"ALREADY_CONSUMED":   "55000",
}

func httpError(status int, data []byte) *pgError {
	var env struct {
		Error *query.Error `json:"error"`
	}
	_ = json.Unmarshal(data, &env)
	e := env.Error
	if e == nil || e.Message == "" {
		e = &query.Error{Code: "QUERY_FAILED", Message: "Query failed"}
	}
	code, ok := sqlstate[e.Code]
	if status == http.StatusUnauthorized {
		code, ok = "28P01", true
	}
	if !ok {
		code = "XX000"
	}
	return &pgError{code: code, msg: e.Message}
}

// authenticate proves the key against the query API: an authenticated status
// lookup of a random handle is 404, an unauthenticated one 401.
func (s *Server) authenticate(ctx context.Context, key, remote string) *pgError {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return &pgError{code: "XX000", msg: "authentication is unavailable"}
	}
	err := s.jsonCall(ctx, http.MethodGet, "/v1/queries/pgwire-auth-"+hex.EncodeToString(b), key, remote, nil, nil)
	if err != nil && err.code == "42704" {
		return nil
	}
	if err == nil || err.code == "28P01" {
		return &pgError{code: "28P01", msg: "password authentication failed"}
	}
	return err
}

// execute submits, consumes and verifies one query through the HTTP query API.
// It succeeds only after a complete Arrow stream and a terminal succeeded state.
func (c *conn) execute(ctx context.Context, sql string, params []query.Parameter, onSchema func(*arrow.Schema) error, onBatch func(arrow.RecordBatch) error) error {
	request := query.Request{Mode: "federated", Sources: c.sources, SQL: sql, Parameters: params}
	var created struct {
		ID string `json:"id"`
	}
	if err := c.s.jsonCall(ctx, http.MethodPost, "/v1/queries", c.key, c.remote, request, &created); err != nil {
		return err
	}
	if created.ID == "" {
		return &pgError{code: "XX000", msg: "invalid query service response"}
	}
	path := "/v1/queries/" + created.ID
	// Release the handle once its result is read so statements never pile up
	// against the shared handle registry; an unfinished one is cancelled first.
	defer func() {
		bg, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if ctx.Err() != nil || c.s.jsonCall(bg, http.MethodDelete, path, c.key, c.remote, nil, nil) != nil {
			_ = c.s.jsonCall(bg, http.MethodPost, path+"/cancel", c.key, c.remote, nil, nil)
			_ = c.s.jsonCall(bg, http.MethodDelete, path, c.key, c.remote, nil, nil)
		}
	}()
	r, err := c.s.call(ctx, http.MethodGet, path+"/results", c.key, c.remote, nil)
	if err != nil {
		return &pgError{code: "XX000", msg: "query request failed"}
	}
	if r.status != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(r.body, 1<<20))
		r.close()
		if ctx.Err() != nil {
			return transportError(ctx)
		}
		return httpError(r.status, data)
	}
	streamErr := func() error {
		rdr, err := ipc.NewReader(r.body)
		if err != nil {
			return err
		}
		defer rdr.Release()
		if err := onSchema(rdr.Schema()); err != nil {
			return err
		}
		for rdr.Next() {
			if err := onBatch(rdr.RecordBatch()); err != nil {
				return err
			}
		}
		return rdr.Err()
	}()
	r.close()
	var pe *pgError
	if errors.As(streamErr, &pe) {
		return pe
	}
	if ctx.Err() != nil {
		return transportError(ctx)
	}
	// Truncation is never success: the terminal state must say succeeded.
	var status struct {
		State string       `json:"state"`
		Error *query.Error `json:"error"`
	}
	if err := c.s.jsonCall(ctx, http.MethodGet, path, c.key, c.remote, nil, &status); err != nil {
		return err
	}
	if status.State != "succeeded" {
		if status.Error != nil {
			data, _ := json.Marshal(map[string]any{"error": status.Error})
			return httpError(http.StatusOK, data)
		}
		return &pgError{code: "XX000", msg: "query did not complete"}
	}
	if streamErr != nil {
		return &pgError{code: "XX000", msg: "query result stream was incomplete"}
	}
	return nil
}

// transportError explains why a context ended.
func transportError(ctx context.Context) *pgError {
	if cause := context.Cause(ctx); cause != nil {
		var pe *pgError
		if errors.As(cause, &pe) {
			return pe
		}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &pgError{code: "57014", msg: "canceling statement due to statement timeout"}
	}
	return &pgError{code: "57014", msg: "canceling statement due to user request"}
}
