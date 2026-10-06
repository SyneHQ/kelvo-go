package relational

import (
	"context"
	"database/sql"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
)

type Session struct {
	*sqlsession.Session
	database          string
	schema            string
	defaultSchema     string
	openMigrationPool func(context.Context) (*sql.DB, error)
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
