// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package oracle

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	goora "github.com/sijms/go-ora/v2"
)

func validConnection() adapter.Connection {
	return adapter.Connection{Engine: "oracle", TenantID: "tenant", ConnectionID: "saved", Revision: "revision", Host: "db.example", Port: 2484, Namespace: "service", Username: "reader", Password: "test-only", Schema: "APP"}
}

func TestOracleConnectionUsesOnlyVerifiedTCPS(t *testing.T) {
	c := validConnection()
	c.Password = "test-only@:?&"
	c.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "private.example"}
	dsn, config, err := connectionDSN(c)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	if password != c.Password || u.Hostname() != c.Host || u.Path != "/service" || u.Query().Get("SSL") != "enable" || u.Query().Get("SSL VERIFY") != "true" || len(u.Query()) != 4 || config.InsecureSkipVerify || config.MinVersion != tls.VersionTLS13 || config.ServerName != "private.example" || config == c.TLS {
		t.Fatal("TCPS identity, escaping, or TLS clone lost")
	}
	for _, mutate := range []func(*adapter.Connection){
		func(c *adapter.Connection) { c.TLS = &tls.Config{InsecureSkipVerify: true} },
		func(c *adapter.Connection) { c.TLS = &tls.Config{MaxVersion: tls.VersionTLS11} },
		func(c *adapter.Connection) { c.Username = "SyS" },
		func(c *adapter.Connection) { c.Host = "db.example,other" },
		func(c *adapter.Connection) { c.Namespace = "service/other" },
		func(c *adapter.Connection) { c.Schema = "APP\x00OTHER" },
		func(c *adapter.Connection) { c.Endpoint = "oracle://untrusted" },
	} {
		bad := validConnection()
		mutate(&bad)
		if _, _, err := connectionDSN(bad); err == nil {
			t.Fatal("unsafe Oracle connection accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Driver{}).Open(ctx, c); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled open lost its context cause")
	}
}

type oracleState struct {
	opens, closes, rollbacks, commits, rowCloses atomic.Int32
	mu                                           sync.Mutex
	events                                       []string
	args                                         []driver.NamedValue
	failSetup, block                             bool
	onQueryStart                                 func()
	closed                                       chan struct{}
}
type oracleConnector struct{ state *oracleState }

func (c oracleConnector) Connect(context.Context) (driver.Conn, error) {
	c.state.opens.Add(1)
	return &oracleConn{c.state}, nil
}
func (c oracleConnector) Driver() driver.Driver { return oracleDriver{c.state} }

type oracleDriver struct{ state *oracleState }

func (d oracleDriver) Open(string) (driver.Conn, error) {
	return oracleConnector{d.state}.Connect(context.Background())
}

type oracleConn struct{ state *oracleState }

func (c *oracleConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *oracleConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected unscoped transaction")
}
func (c *oracleConn) Close() error {
	if c.state.closes.Add(1) == 1 && c.state.closed != nil {
		close(c.state.closed)
	}
	return nil
}
func (c *oracleConn) CheckNamedValue(*driver.NamedValue) error { return nil }
func (c *oracleConn) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	if options.ReadOnly || options.Isolation != 0 {
		return nil, adapter.ErrUnsupported
	}
	c.state.mu.Lock()
	c.state.events = append(c.state.events, "begin")
	c.state.mu.Unlock()
	return oracleTx{c.state}, nil
}
func (c *oracleConn) ExecContext(_ context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.events = append(c.state.events, statement)
	c.state.args = append([]driver.NamedValue(nil), args...)
	if c.state.failSetup && statement == "SET TRANSACTION READ ONLY" {
		return nil, errors.New("setup rejected")
	}
	return driver.RowsAffected(1), nil
}
func (c *oracleConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	c.state.events = append(c.state.events, statement)
	c.state.args = append([]driver.NamedValue(nil), args...)
	c.state.mu.Unlock()
	if c.state.onQueryStart != nil {
		c.state.onQueryStart()
	}
	if c.state.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &oracleRows{state: c.state}, nil
}

type oracleTx struct{ state *oracleState }

func (tx oracleTx) Commit() error   { tx.state.commits.Add(1); return nil }
func (tx oracleTx) Rollback() error { tx.state.rollbacks.Add(1); return nil }

type oracleRows struct {
	state *oracleState
	row   int
}

func (*oracleRows) Columns() []string { return []string{"amount", "created_at"} }
func (r *oracleRows) Close() error    { r.state.rowCloses.Add(1); return nil }
func (*oracleRows) ColumnTypeDatabaseTypeName(i int) string {
	if i == 0 {
		return "NUMBER"
	}
	return "TimeStampTZ_DTY"
}
func (*oracleRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) { return 30, 4, i == 0 }
func (r *oracleRows) Next(values []driver.Value) error {
	if r.row == 3 {
		return io.EOF
	}
	values[0] = "12345678901234567890.1234"
	if r.row == 1 {
		values[0] = nil
	}
	values[1] = time.Date(2026, 10, 6, 12, 34, 56, 123456789, time.FixedZone("source", 19800))
	r.row++
	return nil
}

type oracleSink struct {
	schema  *arrow.Schema
	records []arrow.RecordBatch
	fail    bool
}

func (s *oracleSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *oracleSink) Write(record arrow.RecordBatch) error {
	if s.fail {
		return errors.New("consumer stopped")
	}
	record.Retain()
	s.records = append(s.records, record)
	return nil
}
func (s *oracleSink) close() {
	for _, record := range s.records {
		record.Release()
	}
}
func oracleTestSession(t *testing.T) (*Session, *oracleState) {
	t.Helper()
	state := &oracleState{}
	db := sql.OpenDB(&sessionConnector{Connector: oracleConnector{state}, schema: `APP"NAME`})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return &Session{Session: &sqlsession.Session{Pool: db, Engine: "oracle"}, database: "service", schema: `APP"NAME`}, state
}
func oracleQuery() adapter.Query {
	return adapter.Query{Statement: "SELECT amount,created_at FROM items WHERE amount=:1", Parameters: []operations.Parameter{{Type: "decimal128", Value: json.RawMessage(`"12345678901234567890.1234"`)}}, MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 2}
}

func TestOracleReadInitializesAndDiscardsEveryConnection(t *testing.T) {
	s, state := oracleTestSession(t)
	for range 2 {
		sink := &oracleSink{}
		stats, err := s.Query(context.Background(), oracleQuery(), sink)
		if err != nil {
			sink.close()
			t.Fatal(err)
		}
		if stats.Rows != 3 || len(sink.records) != 2 || sink.records[0].NumRows() != 2 || !sink.records[0].Column(0).IsNull(1) {
			sink.close()
			t.Fatal("row/null/batch contract changed")
		}
		if !arrow.TypeEqual(sink.schema.Field(0).Type, &arrow.Decimal128Type{Precision: 30, Scale: 4}) {
			sink.close()
			t.Fatal("decimal precision lost")
		}
		want := time.Date(2026, 10, 6, 7, 4, 56, 123456789, time.UTC).UnixNano()
		if int64(sink.records[0].Column(1).(*array.Timestamp).Value(0)) != want {
			sink.close()
			t.Fatal("timezone instant lost")
		}
		sink.close()
	}
	want := []string{"ALTER SESSION SET TIME_ZONE = '+00:00'", "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'", `ALTER SESSION SET CURRENT_SCHEMA = "APP""NAME"`, "begin", "SET TRANSACTION READ ONLY", oracleQuery().Statement}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !reflect.DeepEqual(state.events, append(append([]string{}, want...), want...)) || state.opens.Load() != 2 || state.closes.Load() != 2 || state.rollbacks.Load() != 2 || state.commits.Load() != 0 || state.rowCloses.Load() != 2 {
		t.Fatal("read setup/order or cleanup broken")
	}
	if len(state.args) != 1 {
		t.Fatal("parameters lost")
	}
	number, ok := state.args[0].Value.(*goora.Number)
	if !ok {
		t.Fatal("decimal was not bound as Oracle NUMBER")
	}
	value, err := number.String()
	if err != nil || value != "12345678901234567890.1234" {
		t.Fatal("exact parameter changed")
	}
}

func TestOracleReadFailuresNeverRunOutsideReadOnlyTransaction(t *testing.T) {
	for _, mode := range []string{"setup", "sink", "cancel", "write"} {
		t.Run(mode, func(t *testing.T) {
			s, state := oracleTestSession(t)
			request := oracleQuery()
			sink := &oracleSink{}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			state.closed = make(chan struct{})
			switch mode {
			case "setup":
				state.failSetup = true
			case "sink":
				sink.fail = true
			case "cancel":
				state.block = true
				state.onQueryStart = cancel
			case "write":
				request.Statement = "DELETE FROM items"
			}
			_, err := s.Query(ctx, request, sink)
			sink.close()
			if err == nil {
				t.Fatal("failure ignored")
			}
			if mode == "write" {
				if state.opens.Load() != 0 {
					t.Fatal("mutation reached source")
				}
				return
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation cause lost")
			}
			// database/sql may finish cancellation rollback asynchronously.
			// Observe physical close before checking exactly-once cleanup.
			select {
			case <-state.closed:
			case <-time.After(5 * time.Second):
				t.Fatal("failed operation did not close its source connection")
			}
			if state.opens.Load() != 1 || state.closes.Load() != 1 || state.rollbacks.Load() != 1 || state.commits.Load() != 0 {
				t.Fatal("failed operation leaked or retried")
			}
			if mode == "setup" {
				state.mu.Lock()
				defer state.mu.Unlock()
				if len(state.events) != 5 {
					t.Fatal("user query ran after rejected read-only setup")
				}
			}
		})
	}
}

func TestOracleUnsupportedChangeFailsBeforeOpening(t *testing.T) {
	for _, change := range []adapter.Change{
		{Statements: []string{"UPDATE items SET n=1"}, Role: "admin"},
		{Statements: []string{"UPDATE items SET n=1"}, Transaction: true, Isolation: "serializable"},
		{Statements: []string{"CREATE TABLE items(n INT)"}, Transaction: true},
	} {
		s, state := oracleTestSession(t)
		result, err := s.Execute(context.Background(), change)
		if err == nil || result.Outcome != "failed" || result.Attempted != 0 || state.opens.Load() != 0 {
			t.Fatal("unsupported change reached source")
		}
	}
	s, state := oracleTestSession(t)
	result, err := s.Execute(context.Background(), adapter.Change{Statements: []string{"UPDATE items SET n=:1"}, Parameters: [][]operations.Parameter{{{Type: "uint64", Value: json.RawMessage(`"18446744073709551615"`)}}}, Transaction: true, Isolation: "default"})
	if err != nil || result.Outcome != "succeeded" || result.Completed != 1 || state.commits.Load() != 1 || state.closes.Load() != 1 {
		t.Fatal("DML transaction failed", err)
	}
}

func TestOracleParametersPreserveNUMBERAndRejectUndeclaredTypes(t *testing.T) {
	values, err := oracleParameters([]operations.Parameter{{Type: "uint64", Value: json.RawMessage(`"18446744073709551615"`)}, {Type: "decimal128", Value: json.RawMessage(`"-12345678901234567890.1234"`)}})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"18446744073709551615", "-12345678901234567890.1234"} {
		got, err := values[i].(*goora.Number).String()
		if err != nil || got != want {
			t.Fatal("NUMBER precision changed")
		}
	}
	for _, kind := range []string{"json", "bool", "decimal256"} {
		if _, err := oracleParameters([]operations.Parameter{{Type: kind, Value: json.RawMessage(`null`)}}); !errors.Is(err, adapter.ErrUnsupported) {
			t.Fatal("undeclared parameter accepted")
		}
	}
}
