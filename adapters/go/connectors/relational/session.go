package relational

import (
	"context"
	"database/sql"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
)

// Session owns one operation. Query, Inspect and Execute consume its SQL pool.
type Session struct {
	*sqlsession.Session
	database              string
	schema                string
	defaultSchema         string
	openMigrationPool     func(context.Context) (*sql.DB, error)
	openMySQLCancellation func(context.Context) (*sql.DB, error)
	// reusable is set only by a long-lived warm worker. See KeepOpen.
	reusable bool
}

// KeepOpen lets a warm worker run several reads on this session's single
// connection. One-shot operations never call it: their first read consumes the
// pool so nothing else can borrow the session. A warm worker instead exits on
// any failed or cancelled request, which closes the connection with it.
func (s *Session) KeepOpen() {
	if s == nil || s.Session == nil || s.Pool == nil {
		return
	}
	s.reusable = true
	// The worker's idle TTL bounds the connection, not the pool's own timers.
	s.Pool.SetConnMaxLifetime(0)
	s.Pool.SetConnMaxIdleTime(0)
}

var _ adapter.TestSession = (*Session)(nil)
var _ adapter.MetadataSession = (*Session)(nil)

func (s *Session) Test(ctx context.Context) error {
	if ctx == nil || s == nil || s.Session == nil || s.Pool == nil {
		return adapter.ErrInvalid
	}
	return s.Pool.PingContext(ctx)
}

func (s *Session) Execute(ctx context.Context, change adapter.Change) (adapter.ChangeResult, error) {
	if s == nil || s.Session == nil {
		return adapter.ChangeResult{Outcome: "failed"}, adapter.ErrInvalid
	}
	// EXECUTE AS USER may change SQL Server's default schema. A selected
	// schema cannot be applied as session state, so refuse that combination.
	if s.Engine == "sqlserver" && s.schema != "" && (s.schema != s.defaultSchema || change.Role != "") {
		return adapter.ChangeResult{Outcome: "failed"}, adapter.ErrUnsupported
	}
	return s.Session.Execute(ctx, change)
}
