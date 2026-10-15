package relational

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlnative"
	"github.com/apache/arrow-go/v18/arrow"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ adapter.QuerySession = (*Session)(nil)

// rollbackPools tracks, per pool, connections held by a background rollback. A
// read that finds no idle connection waits for one of them (about one round
// trip) instead of dialing a new connection (several round trips).
var rollbackPools = struct {
	sync.Mutex
	m map[*sql.DB]*rollbackState
}{m: map[*sql.DB]*rollbackState{}}

type rollbackState struct {
	pending int
	wake    chan struct{}
}

func beginRollback(db *sql.DB) {
	rollbackPools.Lock()
	defer rollbackPools.Unlock()
	st := rollbackPools.m[db]
	if st == nil {
		st = &rollbackState{wake: make(chan struct{})}
		rollbackPools.m[db] = st
	}
	st.pending++
}

func endRollback(db *sql.DB) {
	rollbackPools.Lock()
	defer rollbackPools.Unlock()
	st := rollbackPools.m[db]
	close(st.wake)
	st.wake = make(chan struct{})
	if st.pending--; st.pending == 0 {
		delete(rollbackPools.m, db)
	}
}

func waitForRollback(ctx context.Context, db *sql.DB) {
	for {
		rollbackPools.Lock()
		st := rollbackPools.m[db]
		if st == nil || db.Stats().Idle > 0 {
			rollbackPools.Unlock()
			return
		}
		wake := st.wake
		rollbackPools.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return
		}
	}
}

// asyncRollbacks counts background rollbacks that still own a connection.
var asyncRollbacks atomic.Int64

func (s *Session) Query(ctx context.Context, request adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	if s == nil || s.Session == nil || request.Validate() != nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	if s.Engine == "sqlserver" {
		if s.schema != "" && s.schema != s.defaultSchema {
			return adapter.QueryStats{}, adapter.ErrUnsupported
		}
		if err := validateSQLServerRead(request.Statement); err != nil {
			return adapter.QueryStats{}, err
		}
	}
	statement, err := sqlguard.ReadOnlyWithOptions(request.Statement, s.Engine == "postgresql")
	if err != nil {
		return adapter.QueryStats{}, err
	}
	parameters, err := adapter.SQLParameters(request.Parameters)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	return s.read(ctx, statement, parameters, adapter.Limits{MaxRows: request.MaxRows, MaxBytes: request.MaxBytes, BatchRows: request.BatchRows}, sink)
}

// read runs one read. A warm connection that died while idle (PgBouncer or NAT
// timeout, failover, restart) shows up as a connection-level failure of the
// first, server-built statement, before any user SQL was sent. That case is
// retried on another connection, each failure discarding one dead connection,
// so after PoolSize+1 attempts the read has had a freshly dialed one. A failure
// of the user query is never retried.
func (s *Session) read(ctx context.Context, statement string, parameters []any, limits adapter.Limits, sink adapter.Sink) (stats adapter.QueryStats, err error) {
	for range PoolSize + 1 {
		stats, err = s.readOnce(ctx, statement, parameters, limits, sink)
		var stale *staleConn
		if !errors.As(err, &stale) {
			return stats, err
		}
		err = stale.err
		if ctx.Err() != nil {
			break
		}
	}
	return stats, err
}

// staleConn marks a failure of the first statement on a pooled connection,
// proving nothing of the caller's ran on the server.
type staleConn struct{ err error }

func (e *staleConn) Error() string { return e.err.Error() }
func (e *staleConn) Unwrap() error { return e.err }

func (s *Session) firstStatementErr(ctx context.Context, err error) error {
	if err != nil && s.reusable && ctx.Err() == nil && connectionLost(err) {
		return &staleConn{err}
	}
	return err
}

func connectionLost(err error) bool {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Severity == "FATAL" && (strings.HasPrefix(pg.Code, "57P") || strings.HasPrefix(pg.Code, "08"))
	}
	var network net.Error
	return errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, mysqldriver.ErrInvalidConn) || errors.As(err, &network) || pgconn.SafeToRetry(err)
}

func (s *Session) readOnce(ctx context.Context, statement string, parameters []any, limits adapter.Limits, sink adapter.Sink) (result adapter.QueryStats, resultErr error) {
	started := time.Now()
	if ctx == nil || sink == nil || s == nil || s.Session == nil || s.Pool == nil || limits.MaxBytes < 1024 || (adapter.Query{Statement: statement, MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}).Validate() != nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	timeout := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	if timeout <= 0 {
		return adapter.QueryStats{}, context.DeadlineExceeded
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if s.reusable {
		waitForRollback(ctx, s.Pool)
	}
	conn, err := s.Pool.Conn(ctx)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	// A reusable session's pool is shared by concurrent reads: any failure
	// (including a failed rollback) discards this physical connection so
	// database/sql drops it instead of returning it to the pool.
	// The tx is begun on a non-cancelling context (statements still carry the
	// real one), so database/sql's awaitDone never races our Rollback for the
	// Conn: it could close the Conn and make Raw panic, or lose the discard to
	// our own Close. Our Rollback therefore always owns the tx; ErrTxDone
	// would mean something else released it, so skip Raw then.
	var rollbackErr error
	var rollback func() error
	// handed is set when a background rollback owns conn and will release it.
	var handed bool
	defer func() {
		if handed {
			return
		}
		taken := errors.Is(rollbackErr, sql.ErrTxDone)
		if s.reusable && !taken && (resultErr != nil || rollbackErr != nil) {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}()
	// Consume the operation-owned pool after acquisition. The held connection
	// remains usable, but queued/new borrowers cannot receive this session.
	// database/sql owns rollback and physical driver release; do not use Raw
	// after cancellation can close the connection.
	// A warm worker keeps the pool: a healthy connection returns to it idle.
	// MySQL/MariaDB session timeouts are re-SET by every read on its own
	// connection, and postgres' statement_timeout is transaction-local, so a
	// pooled connection never carries a stale value into the next read.
	if !s.reusable {
		if err := s.Pool.Close(); err != nil {
			return adapter.QueryStats{}, err
		}
	}
	// The values are server-built integers or decimals, never caller text, and
	// the no-argument form runs as one text-protocol round trip (a bound
	// parameter would add a PREPARE round trip).
	if s.Engine == "mysql" {
		_, err = conn.ExecContext(ctx, "SET SESSION max_execution_time = "+strconv.FormatInt(int64((timeout+time.Millisecond-1)/time.Millisecond), 10))
	} else if s.Engine == "mariadb" {
		_, err = conn.ExecContext(ctx, "SET SESSION max_statement_time = "+strconv.FormatFloat(timeout.Seconds(), 'f', -1, 64))
	}
	if err != nil {
		return adapter.QueryStats{}, s.firstStatementErr(ctx, err)
	}
	// SQL Server has no read-only transaction flag; its source principal must
	// have only the permissions the caller is allowed to exercise.
	cancellation, err := beginMySQLReadCancellation(ctx, conn, s.openMySQLCancellation)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	finish := func() {
		if e := cancellation.finish(); e != nil && !errors.Is(resultErr, e) {
			resultErr = errors.Join(resultErr, e)
		}
		if e := ctx.Err(); e != nil && !errors.Is(resultErr, e) {
			resultErr = errors.Join(resultErr, e)
		}
	}
	defer finish()
	// The rollback is not on the caller's critical path: the result is fully
	// streamed, so a healthy warm connection finishes its rollback in the
	// background and only then returns to the pool. A one-shot session or a
	// failed read still rolls back before returning.
	defer func() {
		if rollback == nil {
			return
		}
		if s.reusable && resultErr == nil {
			handed = true
			asyncRollbacks.Add(1)
			beginRollback(s.Pool)
			go func() {
				defer asyncRollbacks.Add(-1)
				err := rollback()
				if err != nil && !errors.Is(err, sql.ErrTxDone) {
					_ = conn.Raw(func(any) error { return driver.ErrBadConn })
				}
				_ = conn.Close()
				endRollback(s.Pool) // after Close, so a waiter finds the connection idle
			}()
			return
		}
		rollbackErr = rollback()
	}()
	// The tx (or, for postgres, the explicit BEGIN) is begun on a non-cancelling
	// context (statements still carry the real one), so database/sql's awaitDone
	// never races our rollback for the Conn: it could close the Conn and make Raw
	// panic, or lose the discard to our own Close.
	var run func(context.Context, string, ...any) (*sql.Rows, error)
	if s.Engine == "postgresql" {
		// One simple-protocol round trip carries BEGIN READ ONLY and the
		// server-enforced deadline; both are server-built text (the deadline is
		// an integer of milliseconds).
		prelude, err := postgresReadPrelude(cancellation.context)
		if err != nil {
			return adapter.QueryStats{}, err
		}
		if _, err := conn.ExecContext(cancellation.context, prelude); err != nil {
			return adapter.QueryStats{}, s.firstStatementErr(ctx, err)
		}
		rollback = func() error {
			c, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer stop()
			_, err := conn.ExecContext(c, "ROLLBACK")
			return err
		}
		run = conn.QueryContext
	} else {
		tx, err := conn.BeginTx(context.WithoutCancel(cancellation.context), &sql.TxOptions{ReadOnly: s.Engine != "sqlserver"})
		if err != nil {
			if s.Engine == "sqlserver" { // MySQL's first statement is the SET above
				err = s.firstStatementErr(ctx, err)
			}
			return adapter.QueryStats{}, err
		}
		rollback = tx.Rollback
		run = tx.QueryContext
	}
	defer finish()
	rows, err := run(cancellation.context, statement, parameters...)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer rows.Close()
	defer finish()
	stats, err := sqlnative.StreamRows(ctx, rows, sqlnative.Dialect{SourceType: s.Engine}, query.Limits{MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, Timeout: timeout, MemoryMB: 16, Threads: 1, MaxTempMB: 1}, splitSink{next: sink, rows: int64(limits.BatchRows)})
	err = errors.Join(err, cancellation.finish())
	if closeErr := rows.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
	}
	return adapter.QueryStats{Rows: stats.Rows, Bytes: stats.Bytes, Elapsed: time.Since(started)}, err
}

type splitSink struct {
	next adapter.Sink
	rows int64
}

func (s splitSink) Schema(schema *arrow.Schema) error { return s.next.Schema(schema) }
func (s splitSink) Write(record arrow.RecordBatch) error {
	for start := int64(0); start < record.NumRows(); start += s.rows {
		part := record.NewSlice(start, min(start+s.rows, record.NumRows()))
		err := s.next.Write(part)
		part.Release()
		if err != nil {
			return err
		}
	}
	return nil
}
