package relational

import (
	"context"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/jackc/pgx/v5/stdlib"
)

// Open the read-only transaction and install the server timer inside it, in
// one simple-protocol round trip. The Go context still governs transport
// cancellation; this timer also survives child death. Only server-built text
// reaches the statement: the timeout is a validated integer of milliseconds.
func postgresReadPrelude(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", adapter.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return "", adapter.ErrInvalid
	}
	value, err := postgresTimeoutValue(time.Until(deadline))
	if err != nil {
		return "", err
	}
	return "BEGIN READ ONLY; SET LOCAL statement_timeout = '" + value + "'", nil
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

// pgx pings a pooled connection that idled for over a second, which costs a
// full round trip on every read of a lightly used connection. A dead peer is
// still caught: the read fails and the warm pool discards that connection, and
// a connection idle for longer than this is verified before use.
const postgresPingIdle = 30 * time.Second

func postgresShouldPing(_ context.Context, p stdlib.ShouldPingParams) bool {
	return p.IdleDuration > postgresPingIdle
}
