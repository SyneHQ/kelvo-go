// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"time"
)

// PostgreSQL stores statement_timeout as a signed integer in milliseconds.
// Zero disables it. A smaller, nonzero source setting must remain in effect.
const statementTimeoutSQL = `SELECT pg_catalog.set_config('statement_timeout',
LEAST(COALESCE(NULLIF(setting::bigint, 0), $1), $1)::text, true)
FROM pg_catalog.pg_settings WHERE name = 'statement_timeout'`

func statementTimeoutMilliseconds(ctx context.Context, now time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, errors.New("source transaction requires a deadline")
	}
	remaining := deadline.Sub(now)
	if remaining < time.Millisecond {
		// Do not send zero or round up beyond the remaining request budget.
		return 0, context.DeadlineExceeded
	}
	return min(int64(remaining/time.Millisecond), int64(math.MaxInt32)), nil
}

func configureStatementTimeout(ctx context.Context, tx *sql.Tx) error {
	milliseconds, err := statementTimeoutMilliseconds(ctx, time.Now())
	if err != nil {
		return err
	}
	var selected string
	// true gives SET LOCAL semantics. Rollback restores the session setting.
	// PostgreSQL starts this relative timeout when it receives each statement.
	return tx.QueryRowContext(ctx, statementTimeoutSQL, milliseconds).Scan(&selected)
}
