// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package oracle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlnative"
	"github.com/apache/arrow-go/v18/arrow"
)

func (s *Session) Query(ctx context.Context, request adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	if request.Validate() != nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	statement, err := sqlguard.ReadOnly(request.Statement)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	parameters, err := oracleParameters(request.Parameters)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	return s.read(ctx, statement, parameters, adapter.Limits{MaxRows: request.MaxRows, MaxBytes: request.MaxBytes, BatchRows: request.BatchRows}, sink)
}

func (s *Session) read(ctx context.Context, statement string, parameters []any, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	started := time.Now()
	if s == nil || s.Session == nil || s.Pool == nil || ctx == nil || sink == nil || (adapter.Query{Statement: statement, MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}).Validate() != nil {
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
	defer conn.Raw(func(any) error { return driver.ErrBadConn })
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer tx.Rollback()
	// The Oracle driver rejects ReadOnly TxOptions; enforce it as the first
	// server transaction statement, before executing any user SQL.
	if _, err := tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
		return adapter.QueryStats{}, err
	}
	rows, err := tx.QueryContext(ctx, statement, parameters...)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer rows.Close()
	stats, err := sqlnative.StreamRows(ctx, rows, sqlnative.Dialect{SourceType: "oracle"}, query.Limits{MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, Timeout: timeout, MemoryMB: 16, Threads: 1, MaxTempMB: 1}, splitSink{next: sink, rows: int64(limits.BatchRows)})
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if ctx.Err() != nil {
		err = ctx.Err()
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
