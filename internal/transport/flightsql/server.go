// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package flightsql serves a read-only Arrow Flight SQL front end over Kelvo's
// HTTP query API. Every statement, result and metadata probe is an in-process
// call to that API's http.Handler with the caller's own key, so admission,
// principal authorization, policy and execution are the HTTP path's.
package flightsql

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// Per-key bounds; distinct keys are bounded by Options.MaxSessions.
	maxTicketsPerSession  = 64
	maxPreparedPerSession = 64
	maxSQLBytes           = 64 << 10 // the HTTP API's own request SQL limit
	maxPreparedBytes      = 1 << 20  // prepared SQL plus parameters per key
	maxPreparedLifetime   = 10       // prepared statements live at most this many ticket TTLs
	maxProbesPerCall      = 64       // metadata probes per call; more fails, never truncates
	maxRecvBytes          = 2 << 20
	maxJSONBytes          = 1 << 20
	// CatalogName is the single Flight SQL catalog Kelvo reports.
	CatalogName = "kelvo"
	// SourcesHeader optionally lists the federated sources a statement attaches.
	SourcesHeader = "x-kelvo-sources"
)

// Options configures the server. Handler is the HTTP query API (the standalone
// serve handler or a cluster gateway); Catalog supplies table names for
// metadata probes, which the handler authorizes per caller.
type Options struct {
	Handler     http.Handler
	Catalog     catalog.Config
	MaxSessions int
	TicketTTL   time.Duration
}

// Server implements flightsql.Server. Unimplemented write, ingest, substrait
// and transaction commands fall through to BaseServer, which returns Unimplemented.
type Server struct {
	flightsql.BaseServer
	opts   Options
	tables []tableRef

	mu       sync.Mutex
	sessions map[[32]byte]*session
	filling  map[[32]byte]chan struct{}
}

type tableRef struct{ source, schema, name, sql string }

// session is the state one key holds. key is kept only to cancel the
// key's own HTTP queries when its tickets expire or are evicted.
type session struct {
	key       string
	lastUsed  time.Time
	tickets   map[string]*ticket
	prepared  map[string]*prepared
	grants    []grantedTable
	grantsEnd time.Time
}

type ticket struct {
	queryID string
	bytes   []byte
	expires time.Time
	claimed bool
}

type prepared struct {
	sql      string
	sources  []string
	params   []query.Parameter
	bytes    int
	expires  time.Time
	deadline time.Time // absolute; idle renewal never passes it
}

type keyContext struct{}

// New validates options and registers read-only SqlInfo.
func New(opts Options) (*Server, error) {
	if opts.Handler == nil || opts.MaxSessions <= 0 || opts.TicketTTL <= 0 {
		return nil, errors.New("flightsql: handler, positive max sessions and ticket TTL are required")
	}
	s := &Server{opts: opts, sessions: map[[32]byte]*session{}, filling: map[[32]byte]chan struct{}{}}
	for _, src := range opts.Catalog.Sources {
		if src.Federation == nil {
			// Only file sources are a single queryable table; native-only
			// connections are not listed.
			switch catalog.CanonicalType(src.Type) {
			case "csv", "parquet", "json", "jsonl":
				s.tables = append(s.tables, tableRef{src.ID, "main", src.ID, src.ID})
			}
			continue
		}
		for _, t := range src.Federation.Tables {
			s.tables = append(s.tables, tableRef{src.ID, src.ID, t.Name, src.ID + "." + t.Name})
		}
	}
	if a := opts.Catalog.Acceleration; a != nil {
		for _, d := range a.Datasets {
			s.tables = append(s.tables, tableRef{d.ID, "main", d.ID, d.ID})
		}
	}
	for id, v := range map[flightsql.SqlInfo]any{
		flightsql.SqlInfoFlightSqlServerName:        "Kelvo",
		flightsql.SqlInfoFlightSqlServerVersion:     "1",
		flightsql.SqlInfoFlightSqlServerReadOnly:    true,
		flightsql.SqlInfoFlightSqlServerSql:         true,
		flightsql.SqlInfoFlightSqlServerSubstrait:   false,
		flightsql.SqlInfoFlightSqlServerTransaction: int32(flightsql.SqlTransactionNone),
		flightsql.SqlInfoFlightSqlServerCancel:      true,
	} {
		if err := s.RegisterSqlInfo(id, v); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// NewGRPC returns a Flight server with Kelvo authentication. A nil TLS config
// serves plaintext; callers restrict that to loopback.
func (s *Server) NewGRPC(tc *tls.Config) flight.Server {
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxRecvBytes),
		grpc.MaxSendMsgSize(1 << 28),
		grpc.MaxConcurrentStreams(64),
		grpc.ConnectionTimeout(10 * time.Second),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second}),
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: 5 * time.Minute}),
	}
	if tc != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tc)))
	}
	srv := flight.NewServerWithMiddleware([]flight.ServerMiddleware{{Unary: s.unaryAuth, Stream: s.streamAuth}}, opts...)
	srv.RegisterFlightService(flightsql.NewFlightServer(s))
	return srv
}

// --- authentication -------------------------------------------------------

// callerKey accepts the same API key the HTTP API accepts, as a bearer token
// or as the password of Flight basic authentication.
func callerKey(ctx context.Context) (string, bool) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 {
		return "", false
	}
	v := values[0]
	if strings.HasPrefix(v, "Bearer ") {
		return strings.TrimPrefix(v, "Bearer "), true
	}
	if strings.HasPrefix(v, "Basic ") {
		raw, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(strings.TrimPrefix(v, "Basic "), "="))
		if _, password, ok := strings.Cut(string(raw), ":"); err == nil && ok && password != "" {
			return password, true
		}
	}
	return "", false
}

// validate authenticates a key through the HTTP API before any state is
// created for it: an unknown handle is 404 only after authentication.
func (s *Server) validate(ctx context.Context) (string, error) {
	key, ok := callerKey(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "Authentication required")
	}
	code, body, err := s.call(ctx, key, http.MethodGet, "/v1/queries/00000000000000000000000000000000", nil)
	if err != nil {
		return "", err
	}
	if code != http.StatusNotFound {
		return "", httpError(code, body)
	}
	return key, nil
}

func (s *Server) unaryAuth(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	key, err := s.validate(ctx)
	if err != nil {
		return nil, err
	}
	return h(context.WithValue(ctx, keyContext{}, key), req)
}

type authStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (a authStream) Context() context.Context { return a.ctx }

func (s *Server) streamAuth(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	key, err := s.validate(ss.Context())
	if err != nil {
		return err
	}
	if strings.HasSuffix(info.FullMethod, "/Handshake") {
		// The client sends the validated key back as a bearer token.
		return ss.SetHeader(metadata.Pairs("authorization", "Bearer "+key))
	}
	return h(srv, authStream{ss, context.WithValue(ss.Context(), keyContext{}, key)})
}

func keyFrom(ctx context.Context) string { k, _ := ctx.Value(keyContext{}).(string); return k }

// --- in-process HTTP ------------------------------------------------------

// pipeResponse streams a handler's body to the caller without buffering.
type pipeResponse struct {
	header http.Header
	pw     *io.PipeWriter
	once   sync.Once
	code   int
	ready  chan struct{}
}

func (p *pipeResponse) Header() http.Header { return p.header }
func (p *pipeResponse) WriteHeader(code int) {
	p.once.Do(func() { p.code = code; close(p.ready) })
}
func (p *pipeResponse) Write(b []byte) (int, error) {
	p.WriteHeader(http.StatusOK)
	return p.pw.Write(b)
}
func (p *pipeResponse) Flush() {}

// open starts one handler request and returns its status and body. The body
// must be drained or ctx cancelled; done closes when the handler returns.
func (s *Server) open(ctx context.Context, key, method, path string, body any) (int, *io.PipeReader, <-chan struct{}, error) {
	var payload io.Reader = http.NoBody
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, nil, status.Error(codes.Internal, "Unable to encode request")
		}
		payload = bytes.NewReader(raw)
	}
	r, err := http.NewRequestWithContext(ctx, method, "http://kelvo.local"+path, payload)
	if err != nil {
		return 0, nil, nil, status.Error(codes.Internal, "Unable to build request")
	}
	r.Header.Set("Authorization", "Bearer "+key)
	r.Header.Set("Content-Type", "application/json")
	r.RemoteAddr = "127.0.0.1:0"
	pr, pw := io.Pipe()
	w := &pipeResponse{header: http.Header{}, pw: pw, ready: make(chan struct{})}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { pr.CloseWithError(ctx.Err()) })
	go func() {
		defer close(done)
		defer stop()
		defer pw.Close()
		defer w.WriteHeader(http.StatusOK)
		s.opts.Handler.ServeHTTP(w, r)
	}()
	select {
	case <-w.ready:
		return w.code, pr, done, nil
	case <-ctx.Done():
		return 0, nil, nil, contextError(ctx.Err())
	}
}

// call performs a bounded JSON request.
func (s *Server) call(ctx context.Context, key, method, path string, body any) (int, []byte, error) {
	code, r, done, err := s.open(ctx, key, method, path, body)
	if err != nil {
		return 0, nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(r, maxJSONBytes+1))
	// Closing the pipe unblocks a handler still writing an oversized body.
	r.CloseWithError(errors.New("response limit reached"))
	<-done
	if err != nil {
		return 0, nil, contextError(err)
	}
	if len(raw) > maxJSONBytes {
		return 0, nil, status.Error(codes.Internal, "HTTP API response exceeds its limit")
	}
	return code, raw, nil
}

// httpError maps an HTTP API error to an explicit gRPC code.
func httpError(code int, body []byte) error {
	var e struct {
		Error query.Error `json:"error"`
	}
	_ = json.Unmarshal(body, &e)
	msg := e.Error.Message
	if msg == "" {
		msg = http.StatusText(code)
	}
	switch e.Error.Code {
	case "CANCELLED":
		return status.Error(codes.Canceled, msg)
	case "EXPIRED", "DEADLINE_EXCEEDED", "TIMEOUT":
		return status.Error(codes.DeadlineExceeded, msg)
	case "RESOURCE_EXHAUSTED":
		return status.Error(codes.ResourceExhausted, msg)
	case "PERMISSION_DENIED":
		return status.Error(codes.PermissionDenied, msg)
	case "ALREADY_CONSUMED":
		return status.Error(codes.FailedPrecondition, msg)
	}
	switch code {
	case http.StatusBadRequest:
		return status.Error(codes.InvalidArgument, msg)
	case http.StatusUnauthorized:
		return status.Error(codes.Unauthenticated, msg)
	case http.StatusForbidden:
		return status.Error(codes.PermissionDenied, msg)
	case http.StatusNotFound:
		return status.Error(codes.NotFound, msg)
	case http.StatusTooManyRequests:
		return status.Error(codes.ResourceExhausted, msg)
	case http.StatusServiceUnavailable:
		return status.Error(codes.Unavailable, msg)
	case http.StatusGatewayTimeout:
		return status.Error(codes.DeadlineExceeded, msg)
	case http.StatusConflict:
		return status.Error(codes.Aborted, msg)
	}
	return status.Error(codes.Internal, msg)
}

func contextError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "Deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "Query cancelled")
	}
	return status.Error(codes.Internal, "Result transport failed")
}

// --- read-only statements -------------------------------------------------

var (
	readStart = map[string]bool{"SELECT": true, "WITH": true, "VALUES": true, "TABLE": true, "FROM": true}
	writeWord = map[string]bool{"INSERT": true, "UPDATE": true, "DELETE": true, "MERGE": true, "UPSERT": true, "COPY": true, "CREATE": true, "DROP": true, "ALTER": true, "ATTACH": true, "DETACH": true, "INSTALL": true, "LOAD": true, "PRAGMA": true, "SET": true, "RESET": true, "CALL": true, "EXPORT": true, "IMPORT": true, "VACUUM": true, "CHECKPOINT": true, "TRUNCATE": true, "GRANT": true, "REVOKE": true, "BEGIN": true, "COMMIT": true, "ROLLBACK": true, "USE": true}
)

// scanSQL returns bare words and identifiers (bare or double-quoted) outside
// string literals and comments, and whether a ';' precedes further text.
func scanSQL(sql string) (words, idents []string, multi bool) {
	semicolon := false
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '\'' || c == '"':
			j := i + 1
			for ; j < len(sql); j++ {
				if sql[j] == c {
					if j+1 < len(sql) && sql[j+1] == c {
						j++
						continue
					}
					break
				}
			}
			if j >= len(sql) || strings.ContainsRune(sql[i:j], '\\') {
				return words, idents, true // unterminated or escape-bearing literal: fail closed
			}
			if c == '"' {
				idents = append(idents, strings.ToLower(sql[i+1:j]))
			}
			multi = multi || semicolon
			i = j + 1
		case strings.HasPrefix(sql[i:], "--"):
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case strings.HasPrefix(sql[i:], "/*"):
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return words, idents, true // unterminated comment: fail closed
			}
			i += end + 4
		case c == '$':
			return words, idents, true // dollar quoting and $n parameters: fail closed
		case c == ';':
			semicolon = true
			i++
		case c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
			j := i
			for j < len(sql) && (sql[j] == '_' || sql[j] >= '0' && sql[j] <= '9' || sql[j] >= 'A' && sql[j] <= 'Z' || sql[j] >= 'a' && sql[j] <= 'z') {
				j++
			}
			words = append(words, strings.ToUpper(sql[i:j]))
			idents = append(idents, strings.ToLower(sql[i:j]))
			multi = multi || semicolon
			i = j
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '(':
			i++
		default:
			multi = multi || semicolon
			i++
		}
	}
	return words, idents, multi
}

// statement applies a conservative lexical guard: one statement, starting with a
// query keyword, containing no write/DDL/session keyword outside quotes.
// ponytail: lexical, not a parser; it over-rejects bare identifiers named
// like keywords (quote them). The engine's own guards still apply.
func (s *Server) statement(ctx context.Context, sql string) ([]string, error) {
	words, idents, multi := scanSQL(sql)
	if multi || len(words) == 0 || !readStart[words[0]] {
		return nil, status.Error(codes.PermissionDenied, "Flight SQL accepts one read-only query statement")
	}
	for _, w := range words {
		if writeWord[w] {
			return nil, status.Error(codes.PermissionDenied, "Write, DDL and session statements are not permitted")
		}
	}
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get(SourcesHeader); len(v) > 0 {
		var out []string
		for _, id := range strings.Split(strings.Join(v, ","), ",") {
			if id = strings.TrimSpace(id); id != "" {
				out = append(out, id)
			}
		}
		return out, nil
	}
	// Default: attach catalog sources and snapshot datasets the statement names.
	named := map[string]bool{}
	for _, id := range idents {
		named[id] = true
	}
	var out []string
	for _, t := range s.tables {
		if named[strings.ToLower(t.source)] && !slices.Contains(out, t.source) {
			out = append(out, t.source)
		}
	}
	return out, nil
}

// --- sessions and tickets -------------------------------------------------

func newHandle() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", status.Error(codes.Internal, "Unable to create handle")
	}
	return hex.EncodeToString(raw[:]), nil
}

// sessionLocked returns the caller's session, pruning expired state. create
// admits a new key; at the MaxSessions ceiling it evicts the least recently
// used key that is not streaming, so no set of keys can lock others out.
// Queries behind dropped tickets are cancelled through the HTTP API.
func (s *Server) sessionLocked(key string, create bool) (*session, error) {
	now := time.Now()
	for id, se := range s.sessions {
		for h, t := range se.tickets {
			if !t.claimed && now.After(t.expires) {
				delete(se.tickets, h)
				s.cancelLater(se.key, t.queryID)
			}
		}
		for h, p := range se.prepared {
			if now.After(p.expires) {
				delete(se.prepared, h)
			}
		}
		if len(se.tickets) == 0 && len(se.prepared) == 0 && now.After(se.grantsEnd) {
			delete(s.sessions, id)
		}
	}
	id := sha256.Sum256([]byte(key))
	se := s.sessions[id]
	if se == nil && create {
		if len(s.sessions) >= s.opts.MaxSessions && !s.evictLocked() {
			return nil, status.Error(codes.ResourceExhausted, "Flight SQL session limit reached")
		}
		se = &session{key: key, tickets: map[string]*ticket{}, prepared: map[string]*prepared{}}
		s.sessions[id] = se
	}
	if se != nil {
		se.lastUsed = now
	}
	return se, nil
}

// evictLocked drops the least recently used session with no active stream.
func (s *Server) evictLocked() bool {
	var oldest [32]byte
	var found *session
	for id, se := range s.sessions {
		busy := false
		for _, t := range se.tickets {
			busy = busy || t.claimed
		}
		if !busy && (found == nil || se.lastUsed.Before(found.lastUsed)) {
			oldest, found = id, se
		}
	}
	if found == nil {
		return false
	}
	for _, t := range found.tickets {
		s.cancelLater(found.key, t.queryID)
	}
	delete(s.sessions, oldest)
	return true
}

// releaseQuery deletes a finished handle (DELETE /v1/queries/{id}). A 409 for
// a still-active query is ignored; the handle then expires with result-ttl.
func (s *Server) releaseQuery(key, id string) {
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	_, _, _ = s.call(ctx, key, http.MethodDelete, "/v1/queries/"+id, nil)
}

func (s *Server) cancelLater(key, id string) {
	go func() {
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _, _ = s.call(ctx, key, http.MethodPost, "/v1/queries/"+id+"/cancel", nil)
	}()
}

// submit admits a query through the HTTP API and binds a ticket to the caller.
func (s *Server) submit(ctx context.Context, desc *flight.FlightDescriptor, sql string, sources []string, params []query.Parameter) (*flight.FlightInfo, error) {
	info, _, err := s.submitHandle(ctx, desc, sql, sources, params)
	return info, err
}

func (s *Server) submitHandle(ctx context.Context, desc *flight.FlightDescriptor, sql string, sources []string, params []query.Parameter) (*flight.FlightInfo, string, error) {
	key := keyFrom(ctx)
	s.mu.Lock()
	se, err := s.sessionLocked(key, true)
	if err == nil && len(se.tickets) >= maxTicketsPerSession {
		err = status.Error(codes.ResourceExhausted, "Too many outstanding Flight SQL tickets")
	}
	s.mu.Unlock()
	if err != nil {
		return nil, "", err
	}
	code, body, err := s.call(ctx, key, http.MethodPost, "/v1/queries", query.Request{Mode: "federated", SQL: sql, Sources: sources, Parameters: params})
	if err != nil {
		return nil, "", err
	}
	if code != http.StatusCreated {
		return nil, "", httpError(code, body)
	}
	var created struct{ ID string }
	if json.Unmarshal(body, &created) != nil || created.ID == "" {
		return nil, "", status.Error(codes.Internal, "Invalid query handle")
	}
	handle, err := newHandle()
	if err != nil {
		return nil, "", err
	}
	raw, err := flightsql.CreateStatementQueryTicket([]byte(handle))
	if err != nil {
		return nil, "", status.Error(codes.Internal, "Unable to create ticket")
	}
	s.mu.Lock()
	se, err = s.sessionLocked(key, true)
	if err == nil {
		se.tickets[handle] = &ticket{queryID: created.ID, bytes: raw, expires: time.Now().Add(s.opts.TicketTTL)}
	}
	s.mu.Unlock()
	if err != nil {
		_, _, _ = s.call(context.WithoutCancel(ctx), key, http.MethodPost, "/v1/queries/"+created.ID+"/cancel", nil)
		return nil, "", err
	}
	return &flight.FlightInfo{Endpoint: []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: raw}}}, FlightDescriptor: desc, TotalRecords: -1, TotalBytes: -1}, handle, nil
}

func (s *Server) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(cmd.GetTransactionId()) != 0 {
		return nil, status.Error(codes.Unimplemented, "Transactions are not supported")
	}
	if len(cmd.GetQuery()) > maxSQLBytes {
		return nil, status.Error(codes.InvalidArgument, "Statement exceeds 64 KiB")
	}
	sources, err := s.statement(ctx, cmd.GetQuery())
	if err != nil {
		return nil, err
	}
	return s.submit(ctx, desc, cmd.GetQuery(), sources, nil)
}

// DoGetStatement claims a caller-bound, unexpired ticket once and streams the
// HTTP API's Arrow result. The results request re-authorizes the caller; the
// stream succeeds only when the query's terminal state is succeeded.
func (s *Server) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	key, handle := keyFrom(ctx), string(cmd.GetStatementHandle())
	s.mu.Lock()
	se, _ := s.sessionLocked(key, false)
	var t *ticket
	if se != nil {
		t = se.tickets[handle]
	}
	if t == nil || t.claimed || time.Now().After(t.expires) {
		s.mu.Unlock()
		return nil, nil, status.Error(codes.NotFound, "Ticket not found or expired")
	}
	t.claimed = true
	id := t.queryID
	s.mu.Unlock()
	// release runs once the handler has returned: it drops the ticket and
	// releases the finished HTTP handle so it stops counting against max-queries.
	release := func() {
		s.mu.Lock()
		if se, _ := s.sessionLocked(key, false); se != nil {
			delete(se.tickets, handle)
		}
		s.mu.Unlock()
		s.releaseQuery(key, id)
	}
	ctx, cancel := context.WithCancel(ctx)
	code, body, done, err := s.open(ctx, key, http.MethodGet, "/v1/queries/"+id+"/results", nil)
	if err != nil {
		cancel()
		release()
		return nil, nil, err
	}
	fail := func(err error) (*arrow.Schema, <-chan flight.StreamChunk, error) {
		cancel()
		<-done
		release()
		return nil, nil, err
	}
	if code != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(body, maxJSONBytes))
		return fail(httpError(code, raw))
	}
	rdr, err := ipc.NewReader(body, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		_, _ = io.Copy(io.Discard, body)
		<-done
		if terr := s.terminal(ctx, key, id); terr != nil {
			return fail(terr)
		}
		return fail(status.Error(codes.Internal, "Query returned no Arrow schema"))
	}
	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(ch)
		defer release()
		defer cancel()
		defer rdr.Release()
		for rdr.Next() {
			b := rdr.RecordBatch()
			b.Retain()
			select {
			case ch <- flight.StreamChunk{Data: b}:
			case <-ctx.Done():
				b.Release()
				<-done
				return
			}
		}
		readErr := rdr.Err()
		_, _ = io.Copy(io.Discard, body)
		<-done
		err := s.terminal(ctx, key, id)
		if err == nil && readErr != nil {
			err = status.Error(codes.Internal, "Arrow result stream is invalid")
		}
		if err != nil {
			select {
			case ch <- flight.StreamChunk{Err: err}:
			case <-ctx.Done():
			}
		}
	}()
	return rdr.Schema(), ch, nil
}

// terminal returns nil only for a succeeded query: truncation is never success.
func (s *Server) terminal(ctx context.Context, key, id string) error {
	if ctx.Err() != nil {
		return contextError(ctx.Err())
	}
	code, body, err := s.call(ctx, key, http.MethodGet, "/v1/queries/"+id, nil)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return httpError(code, body)
	}
	var st struct {
		State string
		Error *query.Error
	}
	if json.Unmarshal(body, &st) != nil {
		return status.Error(codes.Internal, "Invalid query status")
	}
	if st.State == "succeeded" {
		return nil
	}
	if st.Error != nil {
		raw, _ := json.Marshal(map[string]any{"error": st.Error})
		return httpError(http.StatusConflict, raw)
	}
	return status.Error(codes.Aborted, "Query did not complete")
}

// CancelFlightInfo cancels the caller's own query through the HTTP API.
func (s *Server) CancelFlightInfo(ctx context.Context, req *flight.CancelFlightInfoRequest) (flight.CancelFlightInfoResult, error) {
	st := flight.CancelStatusNotCancellable
	key := keyFrom(ctx)
	if req.GetInfo() == nil {
		return flight.CancelFlightInfoResult{Status: st}, status.Error(codes.InvalidArgument, "FlightInfo is required")
	}
	for _, ep := range req.GetInfo().GetEndpoint() {
		raw := ep.GetTicket().GetTicket()
		s.mu.Lock()
		se, _ := s.sessionLocked(key, false)
		var id string
		if se != nil {
			for h, t := range se.tickets {
				if bytes.Equal(t.bytes, raw) {
					id = t.queryID
					if !t.claimed {
						delete(se.tickets, h)
					}
				}
			}
		}
		s.mu.Unlock()
		if id == "" {
			continue
		}
		code, body, err := s.call(ctx, key, http.MethodPost, "/v1/queries/"+id+"/cancel", nil)
		if err != nil {
			return flight.CancelFlightInfoResult{Status: st}, err
		}
		if code != http.StatusOK {
			return flight.CancelFlightInfoResult{Status: st}, httpError(code, body)
		}
		st = flight.CancelStatusCancelled
	}
	return flight.CancelFlightInfoResult{Status: st}, nil
}

// --- prepared statements --------------------------------------------------

func (s *Server) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	var out flightsql.ActionCreatePreparedStatementResult
	if len(req.GetTransactionId()) != 0 {
		return out, status.Error(codes.Unimplemented, "Transactions are not supported")
	}
	if len(req.GetQuery()) > maxSQLBytes {
		return out, status.Error(codes.InvalidArgument, "Statement exceeds 64 KiB")
	}
	sources, err := s.statement(ctx, req.GetQuery())
	if err != nil {
		return out, err
	}
	handle, err := newHandle()
	if err != nil {
		return out, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	se, err := s.sessionLocked(keyFrom(ctx), true)
	if err != nil {
		return out, err
	}
	// Evict the key's own least recently used statements to stay within bounds.
	for len(se.prepared) >= maxPreparedPerSession || preparedBytes(se)+len(req.GetQuery()) > maxPreparedBytes {
		var oldest string
		for h, p := range se.prepared {
			if oldest == "" || p.expires.Before(se.prepared[oldest].expires) {
				oldest = h
			}
		}
		if oldest == "" {
			return out, status.Error(codes.ResourceExhausted, "Prepared statement bytes exhausted")
		}
		delete(se.prepared, oldest)
	}
	now := time.Now()
	se.prepared[handle] = &prepared{sql: req.GetQuery(), sources: sources, bytes: len(req.GetQuery()), expires: now.Add(s.opts.TicketTTL), deadline: now.Add(maxPreparedLifetime * s.opts.TicketTTL)}
	out.Handle = []byte(handle)
	return out, nil
}

func preparedBytes(se *session) int {
	n := 0
	for _, p := range se.prepared {
		n += p.bytes
	}
	return n
}

// preparedFor renews an idle statement, never past its absolute deadline.
func (s *Server) preparedFor(ctx context.Context, handle []byte) (*prepared, error) {
	se, _ := s.sessionLocked(keyFrom(ctx), false)
	if se == nil || se.prepared[string(handle)] == nil {
		return nil, status.Error(codes.NotFound, "Prepared statement not found or expired")
	}
	p := se.prepared[string(handle)]
	p.expires = minTime(time.Now().Add(s.opts.TicketTTL), p.deadline)
	return p, nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (s *Server) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.preparedFor(ctx, req.GetPreparedStatementHandle()); err != nil {
		return err
	}
	se, _ := s.sessionLocked(keyFrom(ctx), false)
	delete(se.prepared, string(req.GetPreparedStatementHandle()))
	return nil
}

// DoPutPreparedStatementQuery binds one row of typed parameters.
func (s *Server) DoPutPreparedStatementQuery(ctx context.Context, cmd flightsql.PreparedStatementQuery, r flight.MessageReader, _ flight.MetadataWriter) ([]byte, error) {
	var params []query.Parameter
	rows := 0
	for r.Next() {
		b := r.RecordBatch()
		for row := 0; row < int(b.NumRows()); row++ {
			rows++
			if rows > 1 {
				return nil, status.Error(codes.InvalidArgument, "Only one parameter row is supported")
			}
			for _, col := range b.Columns() {
				p, err := parameter(col, row)
				if err != nil {
					return nil, err
				}
				params = append(params, p)
			}
		}
	}
	if err := r.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, status.Error(codes.InvalidArgument, "Invalid parameter stream")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.preparedFor(ctx, cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, err
	}
	size := len(p.sql)
	for _, v := range params {
		size += len(v.Value) + len(v.Type)
	}
	se, _ := s.sessionLocked(keyFrom(ctx), false)
	if preparedBytes(se)-p.bytes+size > maxPreparedBytes {
		return nil, status.Error(codes.ResourceExhausted, "Prepared statement bytes exhausted")
	}
	p.params, p.bytes = params, size
	return cmd.GetPreparedStatementHandle(), nil
}

func parameter(col arrow.Array, row int) (query.Parameter, error) {
	if col.IsNull(row) {
		return query.Parameter{Type: "null", Value: json.RawMessage("null")}, nil
	}
	var typ string
	var v any
	switch a := col.(type) {
	case *array.Int8:
		typ, v = "int64", fmt.Sprint(a.Value(row))
	case *array.Int16:
		typ, v = "int64", fmt.Sprint(a.Value(row))
	case *array.Int32:
		typ, v = "int64", fmt.Sprint(a.Value(row))
	case *array.Int64:
		typ, v = "int64", fmt.Sprint(a.Value(row))
	case *array.Uint8:
		typ, v = "uint64", fmt.Sprint(a.Value(row))
	case *array.Uint16:
		typ, v = "uint64", fmt.Sprint(a.Value(row))
	case *array.Uint32:
		typ, v = "uint64", fmt.Sprint(a.Value(row))
	case *array.Uint64:
		typ, v = "uint64", fmt.Sprint(a.Value(row))
	case *array.Float32:
		typ, v = "float64", float64(a.Value(row))
	case *array.Float64:
		typ, v = "float64", a.Value(row)
	case *array.String:
		typ, v = "string", a.Value(row)
	case *array.LargeString:
		typ, v = "string", a.Value(row)
	case *array.Boolean:
		typ, v = "bool", a.Value(row)
	default:
		return query.Parameter{}, status.Errorf(codes.InvalidArgument, "Unsupported parameter type %s", col.DataType())
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return query.Parameter{}, status.Error(codes.InvalidArgument, "Invalid parameter value")
	}
	return query.Parameter{Type: typ, Value: raw}, nil
}

func (s *Server) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	s.mu.Lock()
	p, err := s.preparedFor(ctx, cmd.GetPreparedStatementHandle())
	var sql string
	var sources []string
	var params []query.Parameter
	if err == nil {
		sql, sources, params = p.sql, p.sources, p.params
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.submit(ctx, desc, sql, sources, params)
}

// DoGetPreparedStatement is unused: prepared FlightInfo returns statement tickets.
func (s *Server) DoGetPreparedStatement(context.Context, flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Error(codes.InvalidArgument, "Use the ticket returned by GetFlightInfo")
}

func (s *Server) DoPutCommandStatementUpdate(context.Context, flightsql.StatementUpdate) (int64, error) {
	return 0, status.Error(codes.PermissionDenied, "Flight SQL is read-only")
}

func (s *Server) DoPutPreparedStatementUpdate(context.Context, flightsql.PreparedStatementUpdate, flight.MessageReader) (int64, error) {
	return 0, status.Error(codes.PermissionDenied, "Flight SQL is read-only")
}

func (s *Server) DoPutCommandStatementIngest(context.Context, flightsql.StatementIngest, flight.MessageReader) (int64, error) {
	return 0, status.Error(codes.PermissionDenied, "Flight SQL is read-only")
}

// --- metadata -------------------------------------------------------------

func metadataInfo(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	return &flight.FlightInfo{Endpoint: []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}}, FlightDescriptor: desc, Schema: flight.SerializeSchema(schema, memory.DefaultAllocator), TotalRecords: -1, TotalBytes: -1}
}

func single(schema *arrow.Schema, fill func(b *array.RecordBuilder)) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	fill(b)
	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: b.NewRecordBatch()}
	close(ch)
	return schema, ch, nil
}

type grantedTable struct {
	tableRef
	arrow *arrow.Schema
}

// granted probes each catalog table with the caller's key through the HTTP
// API and caches the result for the key for one ticket TTL. Only tables the
// caller may read are returned, with the policy-shaped schema the API itself
// produces. A denied table (PermissionDenied/NotFound) is omitted; any other
// probe failure fails the call, so a list is never silently partial.
// Each probe handle is released as soon as its result is read. Cached names
// and schemas can outlive a revocation by one ticket TTL; queries cannot.
func (s *Server) granted(ctx context.Context, schemaPattern, tablePattern *string) ([]grantedTable, error) {
	key := keyFrom(ctx)
	id := sha256.Sum256([]byte(key))
	var all []grantedTable
	var cached bool
	var fill chan struct{}
	// One fill per key at a time; concurrent callers wait for it and reuse it.
	for {
		s.mu.Lock()
		se, err := s.sessionLocked(key, true)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		if cached = time.Now().Before(se.grantsEnd); cached {
			all = se.grants
			s.mu.Unlock()
			break
		}
		wait := s.filling[id]
		if wait == nil {
			fill = make(chan struct{})
			s.filling[id] = fill
			s.mu.Unlock()
			break
		}
		s.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, contextError(ctx.Err())
		}
	}
	if fill != nil {
		defer func() {
			s.mu.Lock()
			delete(s.filling, id)
			s.mu.Unlock()
			close(fill)
		}()
	}
	if !cached {
		if len(s.tables) > maxProbesPerCall {
			return nil, status.Errorf(codes.ResourceExhausted, "Catalog exceeds %d metadata tables", maxProbesPerCall)
		}
		for _, t := range s.tables {
			g, ok, err := s.probe(ctx, t)
			if err != nil {
				return nil, err
			}
			if ok {
				all = append(all, g)
			}
		}
		s.mu.Lock()
		if se, err := s.sessionLocked(key, true); err == nil {
			se.grants, se.grantsEnd = all, time.Now().Add(s.opts.TicketTTL)
		}
		s.mu.Unlock()
	}
	var out []grantedTable
	for _, g := range all {
		if like(schemaPattern, g.tableRef.schema) && like(tablePattern, g.name) {
			out = append(out, g)
		}
	}
	return out, nil
}

func (s *Server) probe(ctx context.Context, t tableRef) (grantedTable, bool, error) {
	denied := func(err error) (grantedTable, bool, error) {
		if c := status.Code(err); (c == codes.PermissionDenied || c == codes.NotFound) && ctx.Err() == nil {
			return grantedTable{}, false, nil
		}
		if c := status.Code(err); c == codes.Unauthenticated || c == codes.ResourceExhausted || c == codes.DeadlineExceeded || c == codes.Canceled {
			return grantedTable{}, false, err
		}
		return grantedTable{}, false, status.Errorf(codes.Unavailable, "Metadata probe for %s failed", t.name)
	}
	_, handle, err := s.submitHandle(ctx, nil, "SELECT * FROM "+t.sql+" LIMIT 0", []string{t.source}, nil)
	if err != nil {
		return denied(err)
	}
	schema, ch, err := s.DoGetStatement(ctx, statementTicket(handle))
	if err != nil {
		return denied(err)
	}
	var streamErr error
	for c := range ch {
		if c.Data != nil {
			c.Data.Release()
		}
		if c.Err != nil {
			streamErr = c.Err
		}
	}
	if streamErr != nil {
		return denied(streamErr)
	}
	return grantedTable{t, schema}, true, nil
}

type statementTicket []byte

func (t statementTicket) GetStatementHandle() []byte { return t }

func like(pattern *string, v string) bool {
	if pattern == nil || *pattern == "" {
		return true
	}
	var b strings.Builder
	b.WriteString("^")
	for _, r := range *pattern {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	ok, _ := regexp.MatchString(b.String(), v)
	return ok
}

func catalogMatch(c *string) bool { return c == nil || *c == "" || *c == CatalogName }

func (s *Server) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return metadataInfo(desc, schema_ref.Catalogs), nil
}

func (s *Server) DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return single(schema_ref.Catalogs, func(b *array.RecordBuilder) { b.Field(0).(*array.StringBuilder).Append(CatalogName) })
}

func (s *Server) GetFlightInfoSchemas(_ context.Context, _ flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return metadataInfo(desc, schema_ref.DBSchemas), nil
}

func (s *Server) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var tables []grantedTable
	if catalogMatch(cmd.GetCatalog()) {
		var err error
		if tables, err = s.granted(ctx, cmd.GetDBSchemaFilterPattern(), nil); err != nil {
			return nil, nil, err
		}
	}
	return single(schema_ref.DBSchemas, func(b *array.RecordBuilder) {
		seen := map[string]bool{}
		for _, t := range tables {
			if !seen[t.tableRef.schema] {
				seen[t.tableRef.schema] = true
				b.Field(0).(*array.StringBuilder).Append(CatalogName)
				b.Field(1).(*array.StringBuilder).Append(t.tableRef.schema)
			}
		}
	})
}

func (s *Server) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if cmd.GetIncludeSchema() {
		return metadataInfo(desc, schema_ref.TablesWithIncludedSchema), nil
	}
	return metadataInfo(desc, schema_ref.Tables), nil
}

func (s *Server) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var tables []grantedTable
	typeOK := len(cmd.GetTableTypes()) == 0
	for _, t := range cmd.GetTableTypes() {
		typeOK = typeOK || strings.EqualFold(t, "TABLE")
	}
	if catalogMatch(cmd.GetCatalog()) && typeOK {
		var err error
		if tables, err = s.granted(ctx, cmd.GetDBSchemaFilterPattern(), cmd.GetTableNameFilterPattern()); err != nil {
			return nil, nil, err
		}
	}
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	return single(schema, func(b *array.RecordBuilder) {
		for _, t := range tables {
			b.Field(0).(*array.StringBuilder).Append(CatalogName)
			b.Field(1).(*array.StringBuilder).Append(t.tableRef.schema)
			b.Field(2).(*array.StringBuilder).Append(t.name)
			b.Field(3).(*array.StringBuilder).Append("TABLE")
			if cmd.GetIncludeSchema() {
				b.Field(4).(*array.BinaryBuilder).Append(flight.SerializeSchema(t.arrow, memory.DefaultAllocator))
			}
		}
	})
}

func (s *Server) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return metadataInfo(desc, schema_ref.TableTypes), nil
}

func (s *Server) DoGetTableTypes(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return single(schema_ref.TableTypes, func(b *array.RecordBuilder) { b.Field(0).(*array.StringBuilder).Append("TABLE") })
}
