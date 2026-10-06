package sqlsession

import (
	"context"
	"database/sql"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

// Session owns its pool. A composition layer opens a new, credential-scoped pool
// with an explicit driver; this package never registers or selects SQL drivers.
type Session struct {
	Pool   *sql.DB
	Engine string
}

var _ adapter.ChangeSession = (*Session)(nil)

func (s *Session) Close() error {
	if s.Pool == nil {
		return nil
	}
	return s.Pool.Close()
}

func (s *Session) Execute(ctx context.Context, change adapter.Change) (adapter.ChangeResult, error) {
	if len(change.Parameters) != 0 && len(change.Parameters) != len(change.Statements) {
		return adapter.ChangeResult{Outcome: string(Failed)}, adapter.ErrInvalid
	}
	statements := make([]Statement, len(change.Statements))
	for i, statement := range change.Statements {
		statements[i].SQL = statement
		if len(change.Parameters) != 0 {
			parameters, err := adapter.SQLParameters(change.Parameters[i])
			if err != nil {
				return adapter.ChangeResult{Outcome: string(Failed)}, err
			}
			statements[i].Parameters = parameters
		}
	}
	result, err := ExecuteStatements(ctx, s.Pool, s.Engine, statements, Options{
		Transaction: change.Transaction,
		Role:        change.Role,
		Isolation:   change.Isolation,
	})
	return adapter.ChangeResult{
		Outcome: string(result.Outcome), Completed: result.Completed, Attempted: result.Attempted, AffectedRows: result.AffectedRows,
	}, err
}
