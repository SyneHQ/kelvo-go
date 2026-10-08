package sqlnative

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type fakeScenario struct {
	row                []driver.Value
	nextErr            error
	second, cancel     bool
	commits, rollbacks int
	queries            int
}

var fakeState struct {
	sync.Mutex
	s *fakeScenario
}

const fakeDriverName = "kelvo-sqlnative-execution-test"

func init() { sql.Register(fakeDriverName, fakeDriver{}) }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return fakeConn{}, nil }

type fakeConn struct{}

func (fakeConn) Prepare(string) (driver.Stmt, error)                          { return nil, errors.New("prepare unused") }
func (fakeConn) Close() error                                                 { return nil }
func (fakeConn) Begin() (driver.Tx, error)                                    { return fakeTx{}, nil }
func (fakeConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) { return fakeTx{}, nil }
func (fakeConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	fakeState.Lock()
	s := fakeState.s
	s.queries++
	fakeState.Unlock()
	if s.cancel {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &fakeRows{s: s}, nil
}

type fakeTx struct{}

func (fakeTx) Commit() error {
	fakeState.Lock()
	defer fakeState.Unlock()
	fakeState.s.commits++
	return nil
}
func (fakeTx) Rollback() error {
	fakeState.Lock()
	defer fakeState.Unlock()
	fakeState.s.rollbacks++
	return nil
}

type fakeRows struct {
	s              *fakeScenario
	sent, advanced bool
}

func (r *fakeRows) Columns() []string { return []string{"i", "f", "ts", "d"} }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.sent {
		return io.EOF
	}
	copy(dest, r.s.row)
	r.sent = true
	return nil
}
func (r *fakeRows) ColumnTypeDatabaseTypeName(i int) string {
	return []string{"INT4", "FLOAT4", "TIMESTAMP", "NUMERIC"}[i]
}
func (r *fakeRows) ColumnTypeScanType(int) reflect.Type { return nil }
func (r *fakeRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	if i == 3 {
		return 8, 2, true
	}
	return 0, 0, false
}
func (r *fakeRows) HasNextResultSet() bool {
	return r.s.second && !r.advanced || r.s.nextErr != nil && !r.advanced
}
func (r *fakeRows) NextResultSet() error { r.advanced = true; return r.s.nextErr }

type execSink struct {
	schema  *arrow.Schema
	records []arrow.RecordBatch
}

func (s *execSink) Schema(x *arrow.Schema) error { s.schema = x; return nil }
func (s *execSink) Write(r arrow.RecordBatch) error {
	r.Retain()
	s.records = append(s.records, r)
	return nil
}
func (s *execSink) close() {
	for _, r := range s.records {
		r.Release()
	}
}
func runEngine(t *testing.T, s *fakeScenario, ctx context.Context) (query.Stats, error, *execSink) {
	t.Helper()
	fakeState.Lock()
	fakeState.s = s
	fakeState.Unlock()
	t.Setenv("KELVO_SOURCE_FAKE_DSN", "x")
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "fake", Type: "fake", DSNEnv: "KELVO_SOURCE_FAKE_DSN"}}}, query.Limits{MaxRows: 10, MaxBytes: 1 << 20, Timeout: 40 * time.Millisecond, MemoryMB: 16, Threads: 1, MaxTempMB: 1}, Dialect{SourceType: "fake", DriverName: fakeDriverName})
	if err != nil {
		t.Fatal(err)
	}
	sink := &execSink{}
	stats, err := e.Execute(ctx, query.Request{Mode: "native", ConnectionID: "fake", SQL: "select 1"}, sink)
	return stats, err, sink
}
func TestExecuteTypedRowsRollsBack(t *testing.T) {
	s := &fakeScenario{row: []driver.Value{int64(7), float64(1.25), time.Date(2025, 1, 2, 3, 4, 5, 123456789, time.UTC), []byte("12.34")}}
	stats, err, sink := runEngine(t, s, context.Background())
	defer sink.close()
	if err != nil {
		t.Fatal(err)
	}
	if s.commits != 0 || s.rollbacks != 1 {
		t.Fatalf("commits=%d rollbacks=%d", s.commits, s.rollbacks)
	}
	if stats.Rows != 1 {
		t.Fatal(stats.Rows)
	}
	if sink.records[0].Column(0).(*array.Int32).Value(0) != 7 {
		t.Fatal("integer lost width/value")
	}
	if sink.records[0].Column(1).(*array.Float32).Value(0) != 1.25 {
		t.Fatal("float lost width/value")
	}
	if sink.records[0].Column(2).(*array.Timestamp).Value(0) != arrow.Timestamp(1735787045123456789) {
		t.Fatal("timestamp lost nanoseconds")
	}
}
func TestExecuteRejectsSecondResultAndNextResultError(t *testing.T) {
	for _, s := range []*fakeScenario{{row: []driver.Value{int64(1), float64(1), time.Now(), []byte("1.00")}, second: true}, {row: []driver.Value{int64(1), float64(1), time.Now(), []byte("1.00")}, nextErr: errors.New("next")}} {
		_, err, sink := runEngine(t, s, context.Background())
		sink.close()
		if err == nil {
			t.Fatal("accepted invalid result continuation")
		}
		if s.commits != 0 || s.rollbacks != 1 {
			t.Fatal("transaction was not rollback-only")
		}
	}
}
func TestExecuteCancellation(t *testing.T) {
	s := &fakeScenario{cancel: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err, sink := runEngine(t, s, ctx)
	sink.close()
	if err == nil {
		t.Fatal("accepted canceled query")
	}
}
