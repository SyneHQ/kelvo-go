package nativereader

import (
	"context"
	"math"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
)

type statementWriter interface {
	ApplyStatement(context.Context, string) (*int64, error)
}
type statementValidator interface{ ValidateStatement(string) error }

func (s *Session) Execute(ctx context.Context, change adapter.Change) (adapter.ChangeResult, error) {
	result := adapter.ChangeResult{Outcome: "failed"}
	if s == nil || ctx == nil || !adapter.NativeWriter(s.spec.Engine) || change.Transaction || change.Role != "" || change.Isolation != "" || len(change.Parameters) != 0 && len(change.Parameters) != len(change.Statements) {
		return result, adapter.ErrUnsupported
	}
	for _, parameters := range change.Parameters {
		if len(parameters) != 0 {
			return result, adapter.ErrUnsupported
		}
	}
	if err := sqlsession.ValidateAutocommit(change.Statements); err != nil {
		return result, adapter.ErrInvalid
	}
	l, err := s.limits(ctx, 1, 1<<20)
	if err != nil {
		return result, err
	}
	e, err := s.open(l)
	if err != nil {
		return result, err
	}
	defer e.Close()
	writer, ok := e.(statementWriter)
	if !ok {
		return result, adapter.ErrUnsupported
	}
	if validator, ok := e.(statementValidator); ok {
		for _, sql := range change.Statements {
			if err := validator.ValidateStatement(sql); err != nil {
				return result, err
			}
		}
	}
	var affected int64
	known := true
	for i, sql := range change.Statements {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.Attempted = i + 1
		count, err := writer.ApplyStatement(ctx, sql)
		if err != nil {
			result.Outcome = "unknown"
			return result, err
		}
		result.Completed++
		if count == nil || *count < 0 || *count > math.MaxInt64-affected {
			known = false
		} else {
			affected += *count
		}
	}
	result.Outcome = "succeeded"
	if known {
		result.AffectedRows = &affected
	}
	return result, nil
}
