package relational

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlnative"
	"github.com/apache/arrow-go/v18/arrow"
)

var _ adapter.QuerySession = (*Session)(nil)

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

func (s *Session) read(ctx context.Context, statement string, parameters []any, limits adapter.Limits, sink adapter.Sink) (result adapter.QueryStats, resultErr error) {
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
	conn, err := s.Pool.Conn(ctx)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer conn.Close()
	// Consume the operation-owned pool after acquisition. The held connection
	// remains usable, but queued/new borrowers cannot receive this session.
	// database/sql owns rollback and physical driver release; do not use Raw
	// after cancellation can close the connection.
	if err := s.Pool.Close(); err != nil {
		return adapter.QueryStats{}, err
	}
	if s.Engine == "mysql" {
		_, err = conn.ExecContext(ctx, "SET SESSION max_execution_time = ?", int64((timeout+time.Millisecond-1)/time.Millisecond))
	} else if s.Engine == "mariadb" {
		_, err = conn.ExecContext(ctx, "SET SESSION max_statement_time = ?", timeout.Seconds())
	}
	if err != nil {
		return adapter.QueryStats{}, err
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
	tx, err := conn.BeginTx(cancellation.context, &sql.TxOptions{ReadOnly: s.Engine != "sqlserver"})
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer tx.Rollback()
	defer finish()
	rows, err := tx.QueryContext(cancellation.context, statement, parameters...)
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
