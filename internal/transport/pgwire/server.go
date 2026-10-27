// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package pgwire

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	maxMessageBytes = 1 << 20
	maxQueryBytes   = 1 << 20 // the HTTP query API's request bound
	startupTimeout  = 10 * time.Second
	maxStatements   = 256      // per simple-query message
	maxPrepared     = 256      // named statements per connection
	maxPortals      = 256      // portals per connection
	maxPortalBytes  = 64 << 20 // buffered portal rows per connection
	maxHandshakes   = 64       // concurrent unauthenticated handshakes

	// Every execution holds an HTTP query handle until its result TTL. Catalog
	// introspection is cached per connection and budgeted so one BI tool's
	// metadata burst cannot exhaust the shared handle registry.
	catalogCacheTTL     = time.Minute
	catalogCacheEntries = 64
	catalogBudget       = 32 // fresh catalog executions per connection per window
	catalogWindow       = time.Minute
)

// Server accepts PostgreSQL clients and forwards statements to handler.
type Server struct {
	cfg     Config
	handler http.Handler
	tls     *tls.Config
	slots   chan struct{}
	shakes  chan struct{}

	mu     sync.Mutex
	ln     net.Listener
	conns  map[uint32]*conn
	closed bool
	wg     sync.WaitGroup
}

// New loads the TLS identity. handler must be the HTTP query API handler.
func New(cfg Config, handler http.Handler) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("pgwire: query handler is required")
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
	if err != nil {
		return nil, errors.New("postgres_wire TLS certificate could not be loaded")
	}
	return &Server{cfg: cfg, handler: handler, tls: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		slots: make(chan struct{}, cfg.MaxConnections), shakes: make(chan struct{}, maxHandshakes), conns: map[uint32]*conn{}}, nil
}

// Serve accepts until Close.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return net.ErrClosed
	}
	s.ln = ln
	s.mu.Unlock()
	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveConn(nc)
		}()
	}
}

// Close stops accepting, cancels running statements and waits for sessions.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	if s.ln != nil {
		_ = s.ln.Close()
	}
	for _, c := range s.conns {
		c.cancel(&pgError{code: "57P01", msg: "terminating connection due to administrator command"})
		_ = c.nc.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

type statement struct {
	sql       string
	kind      int
	paramOIDs []uint32
	cols      []column // set once described
	described bool
}

type portal struct {
	stmt    *statement
	params  []query.Parameter
	casts   map[int]string
	formats []int16
	cols    []column
	rows    [][][]byte // buffered when described or suspended
	bytes   int
	ran     bool
	tag     string
	sent    int
}

type conn struct {
	s       *Server
	nc      net.Conn
	be      *pgproto3.Backend
	remote  string
	key     string
	user    string
	db      string
	sources []string
	pid     uint32
	secret  []byte
	types   *pgtype.Map

	settings map[string]string
	stmts    map[string]*statement
	portals  map[string]*portal
	tx       byte // 'I', 'T' or 'E'
	buffered int  // encoded bytes held by portals

	mu       sync.Mutex
	cancelFn context.CancelCauseFunc

	cache       map[string]*cachedResult
	budgetStart time.Time
	budgetUsed  int
}

type cachedResult struct {
	at      time.Time
	schema  *arrow.Schema
	batches []arrow.RecordBatch
}

func (c *conn) releaseCache() {
	for _, e := range c.cache {
		for _, b := range e.batches {
			b.Release()
		}
	}
	c.cache = nil
}

func (c *conn) cancel(cause *pgError) {
	c.mu.Lock()
	if c.cancelFn != nil {
		c.cancelFn(cause)
	}
	c.mu.Unlock()
}

func (s *Server) serveConn(nc net.Conn) {
	defer nc.Close()
	// Unauthenticated peers get their own small pool and the startup deadline;
	// they never hold a session slot.
	select {
	case s.shakes <- struct{}{}:
	default:
		return
	}
	shaking := true
	endHandshake := func() {
		if shaking {
			shaking = false
			<-s.shakes
		}
	}
	defer endHandshake()
	_ = nc.SetDeadline(time.Now().Add(startupTimeout))
	be := pgproto3.NewBackend(nc, nc)
	be.SetMaxBodyLen(maxMessageBytes)
	msg, err := be.ReceiveStartupMessage()
	if err != nil {
		return
	}
	if _, ok := msg.(*pgproto3.GSSEncRequest); ok {
		if _, err = nc.Write([]byte{'N'}); err != nil {
			return
		}
		if msg, err = be.ReceiveStartupMessage(); err != nil {
			return
		}
	}
	switch m := msg.(type) {
	case *pgproto3.CancelRequest:
		s.cancelRequest(m.ProcessID, m.SecretKey)
		return
	case *pgproto3.SSLRequest:
		if _, err = nc.Write([]byte{'S'}); err != nil {
			return
		}
		tc := tls.Server(nc, s.tls)
		if tc.Handshake() != nil {
			return
		}
		nc = tc
		defer tc.Close()
		be = pgproto3.NewBackend(tc, tc)
		be.SetMaxBodyLen(maxMessageBytes)
		if msg, err = be.ReceiveStartupMessage(); err != nil {
			return
		}
	}
	if m, ok := msg.(*pgproto3.CancelRequest); ok {
		s.cancelRequest(m.ProcessID, m.SecretKey)
		return
	}
	startup, ok := msg.(*pgproto3.StartupMessage)
	fatal := func(code, text string) {
		be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: code, Message: text})
		_ = be.Flush()
	}
	if !ok {
		fatal("08P01", "unsupported startup message")
		return
	}
	if _, isTLS := nc.(*tls.Conn); !isTLS {
		fatal("28000", "Kelvo requires TLS (sslmode=require or stricter)")
		return
	}
	if startup.ProtocolVersion != pgproto3.ProtocolVersion30 {
		be.Send(&pgproto3.NegotiateProtocolVersion{NewestMinorProtocol: 0})
	}
	// Only identity strings exist before the key is proven; session state is
	// allocated after authentication.
	c := &conn{s: s, nc: nc, be: be, remote: nc.RemoteAddr().String(), user: startup.Parameters["user"], db: startup.Parameters["database"], tx: 'I'}
	appName := startup.Parameters["application_name"]
	if c.db == "" {
		c.db = c.user
	}
	for _, id := range strings.Split(c.db, ",") {
		if id = strings.TrimSpace(id); id != "" {
			c.sources = append(c.sources, id)
		}
	}
	if len(c.sources) == 0 {
		fatal("3D000", "database must name one or more Kelvo source IDs separated by commas")
		return
	}
	be.Send(&pgproto3.AuthenticationCleartextPassword{})
	if be.Flush() != nil || be.SetAuthType(pgproto3.AuthTypeCleartextPassword) != nil {
		return
	}
	pm, err := be.Receive()
	if err != nil {
		return
	}
	pw, ok := pm.(*pgproto3.PasswordMessage)
	if !ok || pw.Password == "" {
		fatal("28P01", "password authentication failed")
		return
	}
	c.key = pw.Password
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	authErr := s.authenticate(ctx, c.key, c.remote)
	cancel()
	if authErr != nil {
		fatal(authErr.code, authErr.msg)
		return
	}
	endHandshake()
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		fatal("53300", "sorry, too many clients already")
		return
	}
	if !s.register(c) {
		fatal("57P01", "server is shutting down")
		return
	}
	defer s.unregister(c)
	c.types, c.stmts, c.portals, c.cache = pgtype.NewMap(), map[string]*statement{}, map[string]*portal{}, map[string]*cachedResult{}
	c.settings = map[string]string{"application_name": appName}
	defer c.releaseCache()
	be.Send(&pgproto3.AuthenticationOk{})
	for _, p := range [][2]string{{"server_version", ServerVersion}, {"server_encoding", "UTF8"}, {"client_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"}, {"TimeZone", "UTC"}, {"integer_datetimes", "on"}, {"standard_conforming_strings", "on"},
		{"IntervalStyle", "postgres"}, {"is_superuser", "off"}, {"session_authorization", c.user}, {"application_name", c.settings["application_name"]},
		{"default_transaction_read_only", "on"}, {"in_hot_standby", "off"}} {
		be.Send(&pgproto3.ParameterStatus{Name: p[0], Value: p[1]})
	}
	be.Send(&pgproto3.BackendKeyData{ProcessID: c.pid, SecretKey: c.secret})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if be.Flush() != nil {
		return
	}
	c.loop()
}

func (s *Server) register(c *conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	c.secret = make([]byte, 4)
	for {
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			return false
		}
		if _, err := rand.Read(c.secret); err != nil {
			return false
		}
		c.pid = binary.BigEndian.Uint32(b[:]) & 0x7fffffff
		if _, taken := s.conns[c.pid]; !taken && c.pid != 0 {
			s.conns[c.pid] = c
			return true
		}
	}
}

func (s *Server) unregister(c *conn) {
	s.mu.Lock()
	delete(s.conns, c.pid)
	s.mu.Unlock()
}

func (s *Server) cancelRequest(pid uint32, secret []byte) {
	s.mu.Lock()
	c := s.conns[pid]
	s.mu.Unlock()
	if c != nil && subtle.ConstantTimeCompare(c.secret, secret) == 1 {
		c.cancel(&pgError{code: "57014", msg: "canceling statement due to user request"})
	}
}

func (c *conn) send(msg pgproto3.BackendMessage) { c.be.Send(msg) }

func (c *conn) dropPortal(name string) {
	if p := c.portals[name]; p != nil {
		c.buffered -= p.bytes
		delete(c.portals, name)
	}
}

func (c *conn) sendError(e *pgError) {
	c.send(&pgproto3.ErrorResponse{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: e.code, Message: e.msg})
	if c.tx == 'T' {
		c.tx = 'E'
	}
}

func asPG(err error) *pgError {
	var pe *pgError
	if errors.As(err, &pe) {
		return pe
	}
	return &pgError{code: "08006", msg: "connection failure"}
}

func (c *conn) loop() {
	skipToSync := false
	for {
		_ = c.nc.SetDeadline(time.Now().Add(c.s.cfg.IdleTimeout))
		msg, err := c.be.Receive()
		if err != nil {
			return
		}
		_ = c.nc.SetDeadline(time.Time{})
		if skipToSync {
			switch msg.(type) {
			case *pgproto3.Sync:
				skipToSync = false
			case *pgproto3.Terminate:
				return
			default:
				continue
			}
		}
		var perr *pgError
		switch m := msg.(type) {
		case *pgproto3.Query:
			c.simpleQuery(m.String)
			c.send(&pgproto3.ReadyForQuery{TxStatus: c.tx})
		case *pgproto3.Parse:
			perr = c.parse(m)
		case *pgproto3.Bind:
			perr = c.bind(m)
		case *pgproto3.Describe:
			perr = c.describe(m)
		case *pgproto3.Execute:
			perr = c.executePortal(m)
		case *pgproto3.Close:
			if m.ObjectType == 'S' {
				delete(c.stmts, m.Name)
			} else {
				c.dropPortal(m.Name)
			}
			c.send(&pgproto3.CloseComplete{})
		case *pgproto3.Sync:
			c.dropPortal("")
			c.send(&pgproto3.ReadyForQuery{TxStatus: c.tx})
		case *pgproto3.Flush:
		case *pgproto3.Terminate:
			return
		case *pgproto3.CopyData, *pgproto3.CopyDone, *pgproto3.CopyFail:
			perr = readOnly("copy")
		default:
			perr = unsupported(fmt.Sprintf("protocol message %T", msg))
		}
		if perr != nil {
			c.sendError(perr)
			skipToSync = true
		}
		if c.be.Flush() != nil {
			return
		}
	}
}

// local answers session statements that never reach the engine.
func (c *conn) local(kind int, sql string) (tag string, cols []column, rows [][][]byte, err *pgError) {
	if c.tx == 'E' && kind != kindRollback && kind != kindCommit {
		return "", nil, nil, &pgError{code: "25P02", msg: "current transaction is aborted, commands ignored until end of transaction block"}
	}
	switch kind {
	case kindBegin:
		c.tx = 'T'
		return "BEGIN", nil, nil, nil
	case kindCommit:
		tag = "COMMIT"
		if c.tx == 'E' {
			tag = "ROLLBACK"
		}
		c.tx = 'I'
		return tag, nil, nil, nil
	case kindRollback:
		c.tx = 'I'
		return "ROLLBACK", nil, nil, nil
	case kindSet:
		w := words(sql)
		if len(w) >= 3 && (w[1] == "session" || w[1] == "local") {
			w = w[1:]
		}
		if len(w) >= 2 && w[1] != "transaction" && w[1] != "characteristics" && w[1] != "session" {
			_, value, _ := strings.Cut(sql, "=")
			if value == "" {
				lower := strings.ToLower(sql)
				if i := strings.Index(lower, " to "); i >= 0 {
					value = sql[i+4:]
				}
			}
			c.settings[w[1]] = strings.Trim(strings.TrimSpace(value), "'\"")
		}
		return "SET", nil, nil, nil
	case kindShow:
		w := words(sql)
		if len(w) < 2 {
			return "", nil, nil, &pgError{code: "42601", msg: "syntax error at end of input"}
		}
		name := strings.Join(w[1:], " ")
		value, ok := c.setting(name)
		if !ok {
			return "", nil, nil, &pgError{code: "42704", msg: "unrecognized configuration parameter \"" + name + "\""}
		}
		cols = []column{{name: name, oid: pgtype.TextOID, size: -1, mod: -1}}
		return "SHOW", cols, [][][]byte{{[]byte(value)}}, nil
	case kindNoop:
		return strings.ToUpper(words(sql)[0]), nil, nil, nil
	}
	return "", nil, nil, nil
}

func (c *conn) setting(name string) (string, bool) {
	if v, ok := c.settings[name]; ok {
		return v, true
	}
	switch name {
	case "server_version":
		return ServerVersion, true
	case "server_version_num":
		return "160000", true
	case "server_encoding", "client_encoding":
		return "UTF8", true
	case "datestyle":
		return "ISO, MDY", true
	case "timezone":
		return "UTC", true
	case "transaction_isolation", "default_transaction_isolation", "transaction isolation level":
		return "read committed", true
	case "transaction_read_only", "default_transaction_read_only":
		return "on", true
	case "standard_conforming_strings", "integer_datetimes":
		return "on", true
	case "search_path":
		return "main", true
	case "max_identifier_length":
		return "63", true
	case "statement_timeout":
		return strconv.FormatInt(c.s.cfg.StatementTimeout.Milliseconds(), 10), true
	case "lc_collate", "lc_ctype":
		return "C", true
	}
	return "", false
}

func (c *conn) simpleQuery(sql string) {
	if len(sql) > maxQueryBytes {
		c.sendError(&pgError{code: "54000", msg: "query text exceeds 1 MiB"})
		return
	}
	stmts := split(sql)
	if len(stmts) == 0 {
		c.send(&pgproto3.EmptyQueryResponse{})
		return
	}
	if len(stmts) > maxStatements {
		c.sendError(&pgError{code: "54000", msg: "too many statements in one query"})
		return
	}
	// Classify every statement first: one write rejects the whole message
	// before any earlier statement runs.
	kinds := make([]int, len(stmts))
	for i, s := range stmts {
		kind, err := classify(s)
		if err != nil {
			c.sendError(err)
			return
		}
		kinds[i] = kind
	}
	for i, s := range stmts {
		if err := c.runSimple(kinds[i], s); err != nil {
			c.sendError(err)
			return
		}
	}
}

func (c *conn) runSimple(kind int, sql string) *pgError {
	if kind != kindQuery {
		tag, cols, rows, err := c.local(kind, sql)
		if err != nil {
			return err
		}
		if cols != nil {
			c.send(rowDescription(cols, nil))
			for _, r := range rows {
				c.send(&pgproto3.DataRow{Values: r})
			}
			tag = "SHOW"
		}
		c.send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
		return nil
	}
	if c.tx == 'E' {
		_, _, _, err := c.local(kind, sql)
		return err
	}
	var cols []column
	var n int64
	err := c.run(sql, nil, nil, func(s *arrow.Schema) error {
		var e *pgError
		if cols, e = columns(s); e != nil {
			return e
		}
		c.send(rowDescription(cols, nil))
		return nil
	}, func(b arrow.RecordBatch) error {
		return encodeRows(c.types, cols, nil, b, func(v [][]byte) error {
			n++
			c.send(&pgproto3.DataRow{Values: v})
			if n%256 == 0 {
				return c.be.Flush()
			}
			return nil
		})
	})
	if err != nil {
		return asPG(err)
	}
	c.send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT " + strconv.FormatInt(n, 10))})
	return nil
}

// run executes under the statement timeout and the session's cancel key.
func (c *conn) run(sql string, params []query.Parameter, casts map[int]string, onSchema func(*arrow.Schema) error, onBatch func(arrow.RecordBatch) error) error {
	ctx, cancel := context.WithCancelCause(context.Background())
	ctx, stop := context.WithTimeout(ctx, c.s.cfg.StatementTimeout)
	defer stop()
	c.mu.Lock()
	c.cancelFn = cancel
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.cancelFn = nil
		c.mu.Unlock()
		cancel(nil)
	}()
	rewritten := rewrite(sql, c.db, casts)
	if !isCatalog(sql) {
		err := c.execute(ctx, rewritten, params, onSchema, onBatch)
		if err != nil && ctx.Err() != nil {
			return transportError(ctx)
		}
		return err
	}
	key, _ := json.Marshal(struct {
		SQL    string
		Params []query.Parameter
	}{rewritten, params})
	if e := c.cache[string(key)]; e != nil && time.Since(e.at) < catalogCacheTTL {
		if err := onSchema(e.schema); err != nil {
			return err
		}
		for _, b := range e.batches {
			if err := onBatch(b); err != nil {
				return err
			}
		}
		return nil
	}
	if now := time.Now(); now.Sub(c.budgetStart) >= catalogWindow {
		c.budgetStart, c.budgetUsed = now, 0
	}
	if c.budgetUsed >= catalogBudget {
		return &pgError{code: "53400", msg: "catalog introspection limit reached for this connection; retry in a minute"}
	}
	c.budgetUsed++
	entry := &cachedResult{at: time.Now()}
	err := c.execute(ctx, rewritten, params, func(s *arrow.Schema) error {
		entry.schema = s
		return onSchema(s)
	}, func(b arrow.RecordBatch) error {
		b.Retain()
		entry.batches = append(entry.batches, b)
		return onBatch(b)
	})
	if err != nil {
		for _, b := range entry.batches {
			b.Release()
		}
		if ctx.Err() != nil {
			return transportError(ctx)
		}
		return err
	}
	if len(c.cache) >= catalogCacheEntries {
		c.releaseCache()
		c.cache = map[string]*cachedResult{}
	}
	c.cache[string(key)] = entry
	return nil
}

// isCatalog reports metadata reads: pg_catalog, information_schema, pg_*
// relations, and the LIMIT 0 runs that describe prepared statements.
func isCatalog(sql string) bool {
	if strings.Contains(sql, "kelvo_describe") {
		return true
	}
	for _, w := range words(sql) {
		if w == "information_schema" || strings.HasPrefix(w, "pg_") || w == "duckdb_tables" || w == "duckdb_columns" {
			return true
		}
	}
	return false
}

func (c *conn) parse(m *pgproto3.Parse) *pgError {
	if len(m.Query) > maxQueryBytes {
		return &pgError{code: "54000", msg: "query text exceeds 1 MiB"}
	}
	stmts := split(m.Query)
	if len(stmts) > 1 {
		return &pgError{code: "42601", msg: "cannot insert multiple commands into a prepared statement"}
	}
	st := &statement{paramOIDs: append([]uint32(nil), m.ParameterOIDs...)}
	if len(stmts) == 1 {
		st.sql = stmts[0]
		kind, err := classify(st.sql)
		if err != nil {
			return err
		}
		st.kind = kind
	}
	if m.Name != "" {
		if _, exists := c.stmts[m.Name]; exists {
			return &pgError{code: "42P05", msg: "prepared statement \"" + m.Name + "\" already exists"}
		}
	}
	if _, replace := c.stmts[m.Name]; !replace && len(c.stmts) >= maxPrepared {
		return &pgError{code: "53000", msg: "too many prepared statements"}
	}
	// Infer the parameter count from the highest $n so Describe can report it.
	for _, s := range lex(st.sql) {
		if s.code {
			for _, p := range paramRE.FindAllStringSubmatch(s.text, -1) {
				if n, _ := strconv.Atoi(p[1]); n > len(st.paramOIDs) && n <= 65535 {
					st.paramOIDs = append(st.paramOIDs, make([]uint32, n-len(st.paramOIDs))...)
				}
			}
		}
	}
	c.stmts[m.Name] = st
	c.send(&pgproto3.ParseComplete{})
	return nil
}

func (c *conn) bind(m *pgproto3.Bind) *pgError {
	st, ok := c.stmts[m.PreparedStatement]
	if !ok {
		return &pgError{code: "26000", msg: "prepared statement \"" + m.PreparedStatement + "\" does not exist"}
	}
	if len(m.Parameters) != len(st.paramOIDs) {
		return &pgError{code: "08P01", msg: fmt.Sprintf("bind message supplies %d parameters, but prepared statement requires %d", len(m.Parameters), len(st.paramOIDs))}
	}
	if n := len(m.ParameterFormatCodes); n > 1 && n != len(m.Parameters) {
		return &pgError{code: "08P01", msg: "invalid parameter format code count"}
	}
	p := &portal{stmt: st, casts: map[int]string{}, formats: append([]int16(nil), m.ResultFormatCodes...)}
	for i, raw := range m.Parameters {
		if raw != nil {
			raw = append([]byte{}, raw...)
		}
		param, cast, err := bindParameter(c.types, st.paramOIDs[i], formatFor(m.ParameterFormatCodes, i), raw)
		if err != nil {
			return err
		}
		p.params = append(p.params, param)
		if cast != "" {
			p.casts[i+1] = cast
		}
	}
	if _, replace := c.portals[m.DestinationPortal]; !replace && len(c.portals) >= maxPortals {
		return &pgError{code: "53000", msg: "too many portals"}
	}
	c.dropPortal(m.DestinationPortal)
	c.portals[m.DestinationPortal] = p
	c.send(&pgproto3.BindComplete{})
	return nil
}

func (c *conn) describe(m *pgproto3.Describe) *pgError {
	if m.ObjectType == 'S' {
		st, ok := c.stmts[m.Name]
		if !ok {
			return &pgError{code: "26000", msg: "prepared statement \"" + m.Name + "\" does not exist"}
		}
		oids := make([]uint32, len(st.paramOIDs))
		for i, o := range st.paramOIDs {
			if oids[i] = o; o == 0 {
				oids[i] = pgtype.TextOID
			}
		}
		c.send(&pgproto3.ParameterDescription{ParameterOIDs: oids})
		cols, err := c.describeStatement(st)
		if err != nil {
			return err
		}
		if cols == nil {
			c.send(&pgproto3.NoData{})
		} else {
			c.send(rowDescription(cols, nil))
		}
		return nil
	}
	p, ok := c.portals[m.Name]
	if !ok {
		return &pgError{code: "34000", msg: "portal \"" + m.Name + "\" does not exist"}
	}
	if err := c.fill(p); err != nil {
		return err
	}
	if p.cols == nil {
		c.send(&pgproto3.NoData{})
	} else {
		c.send(rowDescription(p.cols, p.formats))
	}
	return nil
}

// describeStatement learns result columns with NULL parameters and LIMIT 0.
func (c *conn) describeStatement(st *statement) ([]column, *pgError) {
	if st.described || st.sql == "" {
		return st.cols, nil
	}
	if st.kind != kindQuery {
		if st.kind == kindShow {
			_, cols, _, err := c.local(st.kind, st.sql)
			return cols, err
		}
		return nil, nil
	}
	params := make([]query.Parameter, len(st.paramOIDs))
	casts := map[int]string{}
	for i, o := range st.paramOIDs {
		params[i] = query.Parameter{Type: "null", Value: []byte("null")}
		if cast := castFor(o); cast != "" {
			casts[i+1] = cast
		}
	}
	err := c.run("SELECT * FROM ("+st.sql+"\n) AS kelvo_describe LIMIT 0", params, casts, func(s *arrow.Schema) error {
		var e *pgError
		st.cols, e = columns(s)
		if e != nil {
			return e
		}
		return nil
	}, func(arrow.RecordBatch) error { return nil })
	if err != nil {
		return nil, asPG(err)
	}
	st.described = true
	return st.cols, nil
}

// fill runs a portal once and buffers its encoded rows; results stay within
// the HTTP query API's row and byte limits.
func (c *conn) fill(p *portal) *pgError {
	if p.ran {
		return nil
	}
	p.ran = true
	st := p.stmt
	if st.sql == "" {
		p.tag = ""
		return nil
	}
	if st.kind != kindQuery {
		tag, cols, rows, err := c.local(st.kind, st.sql)
		if err != nil {
			return err
		}
		p.tag, p.cols, p.rows = tag, cols, rows
		if cols != nil {
			p.tag = "SHOW"
		}
		return nil
	}
	if c.tx == 'E' {
		_, _, _, err := c.local(kindQuery, st.sql)
		return err
	}
	err := c.run(st.sql, p.params, p.casts, func(s *arrow.Schema) error {
		cols, e := columns(s)
		if e != nil {
			return e
		}
		if st.described && st.cols != nil && !sameShape(st.cols, cols) {
			return &pgError{code: "0A000", msg: "cached plan must not change result type"}
		}
		if n := len(p.formats); n > 1 && n != len(cols) {
			return &pgError{code: "08P01", msg: "invalid result format code count"}
		}
		p.cols = cols
		return nil
	}, func(b arrow.RecordBatch) error {
		return encodeRows(c.types, p.cols, p.formats, b, func(v [][]byte) error {
			n := 0
			for _, x := range v {
				n += len(x) + 4
			}
			if c.buffered+n > maxPortalBytes {
				return &pgError{code: "54000", msg: "portal results exceed 64 MiB buffered per connection; use the simple query protocol or a smaller result"}
			}
			c.buffered += n
			p.bytes += n
			p.rows = append(p.rows, v)
			return nil
		})
	})
	if err != nil {
		c.buffered -= p.bytes
		p.rows, p.bytes = nil, 0
		return asPG(err)
	}
	p.tag = "SELECT " + strconv.Itoa(len(p.rows))
	return nil
}

func (c *conn) executePortal(m *pgproto3.Execute) *pgError {
	p, ok := c.portals[m.Portal]
	if !ok {
		return &pgError{code: "34000", msg: "portal \"" + m.Portal + "\" does not exist"}
	}
	if err := c.fill(p); err != nil {
		return err
	}
	if p.stmt.sql == "" {
		c.send(&pgproto3.EmptyQueryResponse{})
		return nil
	}
	end := len(p.rows)
	if m.MaxRows > 0 && p.sent+int(m.MaxRows) < end {
		end = p.sent + int(m.MaxRows)
	}
	for ; p.sent < end; p.sent++ {
		c.send(&pgproto3.DataRow{Values: p.rows[p.sent]})
	}
	if p.sent < len(p.rows) {
		c.send(&pgproto3.PortalSuspended{})
		return nil
	}
	c.send(&pgproto3.CommandComplete{CommandTag: []byte(p.tag)})
	return nil
}
