// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package httpapi provides the bounded HTTP query transport.
package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/httpstream"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

const maxRequestBytes = 1 << 20

// Options bounds the in-memory request registry and each execution.
type Options struct {
	Token         string
	Limits        query.Limits
	MaxQueries    int
	MaxConcurrent int
	TTL           time.Duration
}

// Server is an HTTP handler whose registry is deliberately bounded and single-use.
type Server struct {
	executor query.Executor
	opts     Options

	mu      sync.Mutex
	queries map[string]*entry
	permits chan struct{}
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
	closed  bool
}

type state string

const (
	queued    state = "queued"
	running   state = "running"
	streaming state = "streaming"
	succeeded state = "succeeded"
	failed    state = "failed"
	cancelled state = "cancelled"
	expired   state = "expired"
)

type entry struct {
	request   query.Request
	createdAt time.Time
	expiresAt time.Time
	state     state
	claimed   bool
	stats     query.Stats
	err       *query.Error
	cancel    context.CancelFunc
}

// New constructs a server. A shared service token is intentional for this first trust domain.
func New(executor query.Executor, opts Options) (*Server, error) {
	if executor == nil {
		return nil, errors.New("httpapi: executor is required")
	}
	if opts.Token == "" {
		return nil, errors.New("httpapi: token is required")
	}
	if opts.MaxQueries <= 0 || opts.MaxConcurrent <= 0 || opts.TTL <= 0 {
		return nil, errors.New("httpapi: MaxQueries, MaxConcurrent, and TTL must be positive")
	}
	if opts.Limits.MaxRows <= 0 || opts.Limits.MaxBytes <= 0 || opts.Limits.Timeout <= 0 {
		return nil, errors.New("httpapi: positive query limits are required")
	}
	if _, err := query.ResultIPCOptions(opts.Limits.ResultCompression); err != nil {
		return nil, err
	}
	s := &Server{executor: executor, opts: opts, queries: make(map[string]*entry), permits: make(chan struct{}, opts.MaxConcurrent), stop: make(chan struct{}), done: make(chan struct{})}
	go s.sweep()
	return s, nil
}

// Close cancels active execution and stops expiry processing.
func (s *Server) Close() error {
	s.once.Do(func() {
		close(s.stop)
		s.mu.Lock()
		s.closed = true
		for _, e := range s.queries {
			if e.cancel != nil {
				e.cancel()
			}
			if e.state == queued || e.state == running || e.state == streaming {
				e.state, e.err = cancelled, &query.Error{Code: "CANCELLED", Message: "Query cancelled"}
			}
		}
		s.mu.Unlock()
		<-s.done
	})
	return nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.isClosed() {
		if r.Method == http.MethodGet && r.URL.Path == "/health" {
			s.writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		} else {
			s.writeError(w, http.StatusServiceUnavailable, &query.Error{Code: "UNAVAILABLE", Message: "Service unavailable"})
		}
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/health" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"ok"}`)
		return
	}
	if !s.authorized(r) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		s.writeError(w, http.StatusUnauthorized, &query.Error{Code: "UNAUTHENTICATED", Message: "Authentication required"})
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "v1" || parts[1] != "queries" {
		s.writeError(w, http.StatusNotFound, &query.Error{Code: "NOT_FOUND", Message: "Not found"})
		return
	}
	if len(parts) == 2 && r.Method == http.MethodPost {
		s.create(w, r)
		return
	}
	if len(parts) == 3 && r.Method == http.MethodGet {
		s.status(w, parts[2])
		return
	}
	if len(parts) == 4 && parts[3] == "results" && r.Method == http.MethodGet {
		s.results(w, r, parts[2])
		return
	}
	if len(parts) == 4 && parts[3] == "cancel" && r.Method == http.MethodPost {
		s.cancel(w, parts[2])
		return
	}
	s.writeError(w, http.StatusNotFound, &query.Error{Code: "NOT_FOUND", Message: "Not found"})
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	defer r.Body.Close()
	var request query.Request
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		s.writeError(w, http.StatusBadRequest, &query.Error{Code: "INVALID_ARGUMENT", Message: "Invalid query request"})
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		s.writeError(w, http.StatusBadRequest, &query.Error{Code: "INVALID_ARGUMENT", Message: "Query request must contain one object"})
		return
	}
	if err := validate(request); err != nil {
		s.writeError(w, http.StatusBadRequest, query.PublicError(err))
		return
	}
	now := time.Now()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.writeError(w, http.StatusServiceUnavailable, &query.Error{Code: "UNAVAILABLE", Message: "Service unavailable"})
		return
	}
	if len(s.queries) >= s.opts.MaxQueries {
		s.mu.Unlock()
		s.writeError(w, http.StatusTooManyRequests, &query.Error{Code: "RESOURCE_EXHAUSTED", Message: "Too many active queries"})
		return
	}
	id, err := newID()
	if err == nil {
		s.queries[id] = &entry{request: request, createdAt: now, expiresAt: now.Add(s.opts.TTL), state: queued}
	}
	s.mu.Unlock()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, &query.Error{Code: "INTERNAL", Message: "Unable to create query"})
		return
	}
	s.writeJSON(w, http.StatusCreated, map[string]string{"id": id, "state": string(queued)})
}

func validate(r query.Request) error {
	return query.ValidateRequest(r)
}

func (s *Server) status(w http.ResponseWriter, id string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.writeError(w, http.StatusServiceUnavailable, &query.Error{Code: "UNAVAILABLE", Message: "Service unavailable"})
		return
	}
	e, ok := s.queries[id]
	if ok && time.Now().After(e.expiresAt) {
		s.expireLocked(id, e)
		ok = false
	}
	if !ok {
		s.mu.Unlock()
		s.writeError(w, http.StatusNotFound, &query.Error{Code: "NOT_FOUND", Message: "Query not found"})
		return
	}
	response := map[string]any{"id": id, "state": e.state, "stats": e.stats}
	if e.err != nil {
		response["error"] = e.err
	}
	s.mu.Unlock()
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) cancel(w http.ResponseWriter, id string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.writeError(w, http.StatusServiceUnavailable, &query.Error{Code: "UNAVAILABLE", Message: "Service unavailable"})
		return
	}
	e, ok := s.queries[id]
	if !ok {
		s.mu.Unlock()
		s.writeError(w, http.StatusNotFound, &query.Error{Code: "NOT_FOUND", Message: "Query not found"})
		return
	}
	if e.state == queued || e.state == running || e.state == streaming {
		e.state, e.err = cancelled, &query.Error{Code: "CANCELLED", Message: "Query cancelled"}
		if e.cancel != nil {
			e.cancel()
		}
	}
	current := e.state
	s.mu.Unlock()
	s.writeJSON(w, http.StatusOK, map[string]string{"id": id, "state": string(current)})
}

func (s *Server) results(w http.ResponseWriter, r *http.Request, id string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.writeError(w, http.StatusServiceUnavailable, &query.Error{Code: "UNAVAILABLE", Message: "Service unavailable"})
		return
	}
	e, ok := s.queries[id]
	if !ok || time.Now().After(e.expiresAt) {
		if ok {
			s.expireLocked(id, e)
		}
		s.mu.Unlock()
		s.writeError(w, http.StatusNotFound, &query.Error{Code: "NOT_FOUND", Message: "Query not found"})
		return
	}
	if e.state == cancelled || e.state == expired || e.state == failed {
		err := e.err
		s.mu.Unlock()
		s.writeError(w, terminalStatus(err), err)
		return
	}
	if e.state == succeeded || e.claimed {
		s.mu.Unlock()
		s.writeError(w, http.StatusConflict, &query.Error{Code: "ALREADY_CONSUMED", Message: "Query results are single-consumer"})
		return
	}
	select {
	case s.permits <- struct{}{}:
	default:
		s.mu.Unlock()
		s.writeError(w, http.StatusTooManyRequests, &query.Error{Code: "RESOURCE_EXHAUSTED", Message: "Execution capacity is unavailable"})
		return
	}
	e.claimed, e.state = true, running
	ctx, cancel := context.WithTimeout(r.Context(), s.opts.Limits.Timeout)
	e.cancel = cancel
	request := e.request
	s.mu.Unlock()
	stopWrites := httpstream.WatchWriteDeadline(ctx, w, time.Now().Add(s.opts.Limits.Timeout))
	defer func() {
		stopWrites()
		cancel()
		<-s.permits
	}()

	sink := &arrowSink{w: w, maxRows: s.opts.Limits.MaxRows, maxBytes: s.opts.Limits.MaxBytes, compression: s.opts.Limits.ResultCompression, onStart: func() {
		s.mu.Lock()
		if current, present := s.queries[id]; present && current.state == running {
			current.state = streaming
		}
		s.mu.Unlock()
	}}
	stats, err := s.executor.Execute(ctx, request, sink)
	if err == nil && sink.err != nil {
		err = sink.err
	}
	if err == nil && sink.writer == nil {
		err = query.NewError("QUERY_FAILED", "Query returned no Arrow schema")
	}
	if err == nil {
		if contextErr := ctx.Err(); contextErr != nil {
			err = contextErr
		} else if closeErr := sink.writer.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if err != nil {
		sink.abort()
	}
	public := query.PublicError(err)
	s.mu.Lock()
	if current, present := s.queries[id]; present {
		current.stats = stats
		current.stats.Rows, current.stats.Batches = sink.rows, sink.batches
		current.stats.WireBytes = sink.bytes
		current.cancel = nil
		switch {
		case current.state == cancelled || current.state == expired:
			// Cancellation/expiry wins even when an engine reports success during the race.
		case err != nil:
			current.state, current.err = failed, public
		default:
			current.state, current.err = succeeded, nil
		}
	}
	s.mu.Unlock()
	if err != nil && !sink.started {
		s.writeError(w, terminalStatus(public), public)
	}
}

type arrowSink struct {
	w            http.ResponseWriter
	writer       *ipc.Writer
	output       *limitWriter
	started      bool
	rows         int64
	batches      int64
	bytes        int64
	decodedBytes int64
	maxRows      int64
	maxBytes     int64
	compression  string
	onStart      func()
	err          error
}

func (s *arrowSink) Schema(schema *arrow.Schema) error {
	if s.err != nil {
		return s.err
	}
	if s.writer != nil {
		return s.fail(query.NewError("QUERY_FAILED", "Query emitted more than one schema"))
	}
	if schema == nil {
		return s.fail(query.NewError("QUERY_FAILED", "Query emitted a nil schema"))
	}
	options, err := query.ResultIPCOptions(s.compression)
	if err != nil {
		return s.fail(err)
	}
	options = append(options, ipc.WithSchema(schema))
	s.w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	s.w.Header().Set("Cache-Control", "no-store")
	s.w.WriteHeader(http.StatusOK)
	s.started = true
	if s.onStart != nil {
		s.onStart()
	}
	s.output = &limitWriter{w: s.w, used: &s.bytes, max: s.maxBytes}
	s.writer = ipc.NewWriter(s.output, options...)
	return nil
}

func (s *arrowSink) Write(batch arrow.RecordBatch) error {
	if s.err != nil {
		return s.err
	}
	if s.writer == nil {
		return s.fail(query.NewError("QUERY_FAILED", "Query emitted batches before its schema"))
	}
	if batch == nil {
		return s.fail(query.NewError("QUERY_FAILED", "Query emitted a nil batch"))
	}
	if batch.NumRows() > s.maxRows-s.rows {
		return s.fail(query.NewError("RESOURCE_EXHAUSTED", "Query row limit exceeded"))
	}
	// The encoded limit alone would let a highly compressible result bypass
	// its original Arrow buffer budget. Bound both representations separately.
	size := arrowutil.TotalRecordSize(batch)
	if size < 0 || size > s.maxBytes-s.decodedBytes {
		return s.fail(query.NewError("RESOURCE_EXHAUSTED", "Query result byte limit exceeded"))
	}
	if err := s.writer.Write(batch); err != nil {
		return s.fail(err)
	}
	s.rows += batch.NumRows()
	s.batches++
	s.decodedBytes += size
	return nil
}

func (s *arrowSink) fail(err error) error {
	if s.err == nil {
		s.err = err
		s.abort()
	}
	return s.err
}

// Close releases retained Arrow dictionaries and compression state, but the
// failed stream must never receive EOS. Discard cleanup writes without counting
// them as client bytes, including when execution fails after valid batches.
func (s *arrowSink) abort() {
	if s.writer != nil && s.output != nil && !s.output.discard {
		s.output.discard = true
		_ = s.writer.Close()
	}
}

type limitWriter struct {
	w       http.ResponseWriter
	used    *int64
	max     int64
	discard bool
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if w.discard {
		return len(p), nil
	}
	if int64(len(p)) > w.max-*w.used {
		return 0, query.NewError("RESOURCE_EXHAUSTED", "Query result byte limit exceeded")
	}
	n, err := w.w.Write(p)
	*w.used += int64(n)
	return n, err
}

func terminalStatus(err *query.Error) int {
	if err == nil {
		return http.StatusConflict
	}
	switch err.Code {
	case "CANCELLED":
		return http.StatusConflict
	case "RESOURCE_EXHAUSTED":
		return http.StatusTooManyRequests
	case "INVALID_ARGUMENT":
		return http.StatusBadRequest
	case "DATASET_UNAVAILABLE":
		return http.StatusServiceUnavailable
	default:
		return http.StatusConflict
	}
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) authorized(r *http.Request) bool {
	const prefix = "Bearer "
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	token := strings.TrimPrefix(value, prefix)
	return len(token) == len(s.opts.Token) && subtle.ConstantTimeCompare([]byte(token), []byte(s.opts.Token)) == 1
}

func (s *Server) sweep() {
	tick := time.NewTicker(minDuration(s.opts.TTL/4, time.Second))
	defer func() { tick.Stop(); close(s.done) }()
	for {
		select {
		case <-s.stop:
			return
		case now := <-tick.C:
			s.mu.Lock()
			for id, e := range s.queries {
				if !now.Before(e.expiresAt) {
					s.expireLocked(id, e)
				}
			}
			s.mu.Unlock()
		}
	}
}
func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 || a > b {
		return b
	}
	return a
}
func (s *Server) expireLocked(id string, e *entry) {
	if e.cancel != nil {
		e.cancel()
	}
	if e.state == running || e.state == streaming {
		e.state, e.err = expired, &query.Error{Code: "EXPIRED", Message: "Query expired"}
		return
	}
	delete(s.queries, id)
}
func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}
func (s *Server) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func (s *Server) writeError(w http.ResponseWriter, status int, err *query.Error) {
	if err == nil {
		err = &query.Error{Code: "QUERY_FAILED", Message: "Query failed"}
	}
	s.writeJSON(w, status, map[string]any{"error": err})
}

var _ http.Handler = (*Server)(nil)
var _ query.Sink = (*arrowSink)(nil)
