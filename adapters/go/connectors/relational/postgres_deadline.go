package relational

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

const postgresReadDeadlineSQL = "SELECT pg_catalog.set_config('statement_timeout', $1, true)"

// Install the server timer inside the read-only transaction. The Go context
// still governs transport cancellation; this timer also survives child death.
func setPostgresReadDeadline(ctx context.Context, tx *sql.Tx) error {
	if ctx == nil || tx == nil {
		return adapter.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return adapter.ErrInvalid
	}
	value, err := postgresTimeoutValue(time.Until(deadline))
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, postgresReadDeadlineSQL, value)
	return err
}
func postgresTimeoutValue(remaining time.Duration) (string, error) {
	// Zero disables statement_timeout. Reject sub-millisecond budgets instead
	// of rounding them to zero or granting a longer server budget.
	milliseconds := remaining / time.Millisecond
	if milliseconds < 1 {
		return "", context.DeadlineExceeded
	}
	return strconv.FormatInt(int64(milliseconds), 10) + "ms", nil
}
