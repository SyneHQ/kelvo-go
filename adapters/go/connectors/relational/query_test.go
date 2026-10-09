package relational

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type readState struct {
	opens, closes, queries, rowCloses, rollbacks, commits atomic.Int32
	readOnly                                              atomic.Bool
	isolation                                             atomic.Int32
	mu                                                    sync.Mutex
	args                                                  []driver.NamedValue
	executed                                              []string
	engine                                                string
	block                                                 bool
}
type readConnector struct{ s *readState }

func (c readConnector) Connect(context.Context) (driver.Conn, error) {
	c.s.opens.Add(1)
	return &readConn{s: c.s}, nil
}
func (c readConnector) Driver() driver.Driver { return readDriver{c.s} }

type readDriver struct{ s *readState }

func (d readDriver) Open(string) (driver.Conn, error) {
	d.s.opens.Add(1)
	return &readConn{s: d.s}, nil
}

type readConn struct{ s *readState }

func (c *readConn) Close() error                        { c.s.closes.Add(1); return nil }
func (c *readConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected prepare") }
func (c *readConn) Begin() (driver.Tx, error)           { return nil, errors.New("unscoped transaction") }
func (c *readConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.s.readOnly.Store(opts.ReadOnly)
	c.s.isolation.Store(int32(opts.Isolation))
	return readTx{c.s}, nil
}
func (c *readConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.s.mu.Lock()
	c.s.executed = append(c.s.executed, query)
	c.s.mu.Unlock()
	return driver.RowsAffected(0), nil
}
func (c *readConn) QueryContext(ctx context.Context, _ string, args []driver.NamedValue) (driver.Rows, error) {
	c.s.queries.Add(1)
	c.s.mu.Lock()
	c.s.args = append([]driver.NamedValue{}, args...)
	c.s.mu.Unlock()
	if c.s.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &readRows{s: c.s}, nil
}

type readTx struct{ s *readState }

func (t readTx) Commit() error   { t.s.commits.Add(1); return nil }
func (t readTx) Rollback() error { t.s.rollbacks.Add(1); return nil }

type readRows struct {
	s   *readState
	row int
}

func (r *readRows) Columns() []string { return []string{"id", "amount"} }
func (r *readRows) Close() error      { r.s.rowCloses.Add(1); return nil }
func (r *readRows) Next(dest []driver.Value) error {
	if r.row == 3 {
		return io.EOF
	}
	dest[0] = int64(9007199254740993) + int64(r.row)
	dest[1] = []byte("12345678901234567890.1234")
	if r.row == 1 {
		dest[1] = nil
	}
	r.row++
	return nil
}
func (r *readRows) ColumnTypeDatabaseTypeName(i int) string {
	if i == 1 {
		if r.s.engine == "postgresql" {
			return "NUMERIC"
		}
		return "DECIMAL"
	}
	if r.s.engine == "postgresql" {
		return "INT8"
	}
	return "BIGINT"
}
func (r *readRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	if i == 1 {
		return 30, 4, true
	}
	return 0, 0, false
}

type readSink struct {
	schema  *arrow.Schema
	records []arrow.RecordBatch
	fail    bool
}

func (s *readSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *readSink) Write(record arrow.RecordBatch) error {
	if s.fail {
		return errors.New("sink stopped")
	}
	record.Retain()
	s.records = append(s.records, record)
	return nil
}
func (s *readSink) Close() {
	for _, record := range s.records {
		record.Release()
	}
}

func newReadSession(t *testing.T, engine string) (*Session, *readState) {
	t.Helper()
	state := &readState{engine: engine}
	db := sql.OpenDB(readConnector{state})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return &Session{Session: &sqlsession.Session{Pool: db, Engine: engine}, database: "app"}, state
}
func readRequest() adapter.Query {
	return adapter.Query{Statement: "SELECT id,amount FROM items WHERE id=$1", Parameters: []operations.Parameter{{Type: "int64", Value: json.RawMessage(`9007199254740993`)}}, MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 2}
}

func TestRelationalReadPreservesTypesAndAlwaysRollsBack(t *testing.T) {
	for _, engine := range []string{"postgresql", "mysql", "mariadb"} {
		t.Run(engine, func(t *testing.T) {
			s, state := newReadSession(t, engine)
			sink := &readSink{}
			defer sink.Close()
			request := readRequest()
			if engine != "postgresql" {
				request.Statement = "SELECT id,amount FROM items WHERE id=?"
			}
			stats, err := s.Query(context.Background(), request, sink)
			if err != nil {
				t.Fatal(err)
			}
			if stats.Rows != 3 || len(sink.records) != 2 || sink.records[0].NumRows() != 2 || sink.records[1].NumRows() != 1 || !state.readOnly.Load() || state.commits.Load() != 0 || state.rollbacks.Load() != 1 || state.closes.Load() != 1 || state.rowCloses.Load() != 1 {
				t.Fatal("read isolation, lifecycle or batching broken")
			}
			if sink.records[0].Column(0).(*array.Int64).Value(0) != 9007199254740993 || !sink.records[0].Column(1).IsNull(1) || !arrow.TypeEqual(sink.schema.Field(1).Type, &arrow.Decimal128Type{Precision: 30, Scale: 4}) {
				t.Fatal("precision/null lost")
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if len(state.args) != 1 || state.args[0].Value != int64(9007199254740993) {
				t.Fatal("parameter precision lost")
			}
			if engine != "postgresql" && len(state.executed) != 1 {
				t.Fatal("source execution timeout missing")
			}
		})
	}
}

func TestRelationalRejectsWriteBeforeOpeningReadSession(t *testing.T) {
	s, state := newReadSession(t, "postgresql")
	request := readRequest()
	request.Statement = "DELETE FROM items"
	if _, err := s.Query(context.Background(), request, &readSink{}); err == nil || state.opens.Load() != 0 || state.queries.Load() != 0 {
		t.Fatal("write reached read session")
	}
}

func TestRelationalSinkFailureDiscardsSession(t *testing.T) {
	s, state := newReadSession(t, "postgresql")
	if _, err := s.Query(context.Background(), readRequest(), &readSink{fail: true}); err == nil {
		t.Fatal("sink error lost")
	}
	if state.rollbacks.Load() != 1 || state.closes.Load() != 1 || state.rowCloses.Load() != 1 || state.queries.Load() != 1 {
		t.Fatal("failed read leaked/retried")
	}
}

func TestRelationalReadCancelsDriver(t *testing.T) {
	s, state := newReadSession(t, "postgresql")
	state.block = true
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := s.Query(ctx, readRequest(), &readSink{}); err == nil || ctx.Err() == nil {
		t.Fatal("query did not cancel")
	}
	if state.queries.Load() != 1 {
		t.Fatal("cancelled query retried")
	}
}

func TestRelationalChangesApplyIsolationAndReportBatchCounts(t *testing.T) {
	s, state := newReadSession(t, "postgresql")
	result, err := s.Execute(context.Background(), adapter.Change{Statements: []string{"UPDATE items SET n=1", "DELETE FROM items WHERE n=0"}, Transaction: true, Isolation: "serializable", Role: "editor"})
	if err != nil || result.Outcome != "succeeded" || result.Attempted != 2 || result.Completed != 2 || state.isolation.Load() != int32(sql.LevelSerializable) || state.readOnly.Load() || state.commits.Load() != 1 || state.closes.Load() != 1 {
		t.Fatal(result, err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.executed) != 3 || state.executed[0] != `SET ROLE "editor"` {
		t.Fatal("role/order not applied")
	}
}

func TestRelationalChangeRejectsIsolationWithoutTransactionBeforeDial(t *testing.T) {
	s, state := newReadSession(t, "postgresql")
	if _, err := s.Execute(context.Background(), adapter.Change{Statements: []string{"UPDATE items SET n=1"}, Isolation: "serializable"}); err == nil || state.opens.Load() != 0 {
		t.Fatal("invalid isolation reached source")
	}
}

func TestReadDiscardsPreviouslyAdmittedPhysicalConnection(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			s, state := newReadSession(t, "postgresql")
			if err := s.Pool.PingContext(context.Background()); err != nil {
				t.Fatal(err)
			}
			if state.opens.Load() != 1 || state.closes.Load() != 0 {
				t.Fatal("fixture did not retain its admitted connection")
			}
			state.block = cancelled
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			sink := &readSink{}
			defer sink.Close()
			_, err := s.Query(ctx, readRequest(), sink)
			if cancelled && err == nil || !cancelled && err != nil {
				t.Fatalf("unexpected query result: %v", err)
			}
			deadline := time.Now().Add(time.Second)
			for state.closes.Load() != 1 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if state.opens.Load() != 1 || state.closes.Load() != 1 || state.rollbacks.Load() != 1 || s.Pool.Stats().Idle != 0 {
				t.Fatal("admitted physical session was reopened, retained, or not rolled back")
			}
		})
	}
}
