package cassandra

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/gocql/gocql"
)

type changeBackend interface {
	Execute(context.Context, string, []any) error
}

// Execute accepts independent autocommit statements. A disconnected or timed
// out write is unknown and must not be replayed, including counter updates.
func (s *Session) Execute(ctx context.Context, change adapter.Change) (adapter.ChangeResult, error) {
	result := adapter.ChangeResult{Outcome: "failed"}
	if s == nil || ctx == nil || len(change.Statements) == 0 || len(change.Statements) > 100 || len(change.Parameters) != len(change.Statements) {
		return result, adapter.ErrInvalid
	}
	if change.Transaction || change.Role != "" || change.Isolation != "" {
		return result, adapter.ErrUnsupported
	}
	bound := make([][]any, len(change.Statements))
	total := 0
	for i, statement := range change.Statements {
		total += len(statement)
		if total > 1<<20 || len(change.Parameters[i]) > 1000 || !changeCQL(statement, len(change.Parameters[i])) {
			return result, adapter.ErrUnsupported
		}
		var err error
		bound[i], err = parameterValues(change.Parameters[i])
		if err != nil {
			return result, err
		}
		for _, value := range bound[i] {
			if stamp, ok := value.(time.Time); ok && stamp.Nanosecond()%int(time.Millisecond) != 0 {
				return result, adapter.ErrUnsupported
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	backend, ok := s.backend.(changeBackend)
	if !ok {
		return result, adapter.ErrUnsupported
	}
	for i, statement := range change.Statements {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.Attempted++
		if err := backend.Execute(ctx, statement, bound[i]); err != nil {
			result.Outcome = "unknown"
			var rejection gocql.RequestError
			if errors.As(err, &rejection) {
				switch rejection.Code() {
				case gocql.ErrCodeSyntax, gocql.ErrCodeUnauthorized, gocql.ErrCodeInvalid, gocql.ErrCodeAlreadyExists, gocql.ErrCodeCredentials:
					result.Outcome = "failed"
				}
			}
			return result, errors.New("CQL statement failed")
		}
		result.Completed++
	}
	result.Outcome = "succeeded"
	return result, nil // CQL acknowledgements do not provide affected-row counts.
}

func (b liveBackend) Execute(ctx context.Context, statement string, values []any) error {
	return b.session.Query(statement, values...).WithContext(ctx).
		RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 0}).Idempotent(false).
		SetSpeculativeExecutionPolicy(gocql.NonSpeculativeExecution{}).Exec()
}

func changeCQL(statement string, parameters int) bool {
	statement = strings.TrimSpace(statement)
	if len(statement) == 0 || len(statement) > 1<<20 {
		return false
	}
	statement = strings.TrimSuffix(statement, ";")
	var words []string
	markers := 0
	var quote byte
	for i := 0; i < len(statement); i++ {
		ch := statement[i]
		if ch == 0 || ch == '\\' {
			return false
		}
		if quote != 0 {
			if ch == quote {
				if i+1 < len(statement) && statement[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == ';' || i+1 < len(statement) && (statement[i:i+2] == "--" || statement[i:i+2] == "//" || statement[i:i+2] == "/*") {
			return false
		}
		if ch == '?' {
			markers++
		}
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_' {
			start := i
			for i+1 < len(statement) && (statement[i+1] >= 'a' && statement[i+1] <= 'z' || statement[i+1] >= 'A' && statement[i+1] <= 'Z' || statement[i+1] >= '0' && statement[i+1] <= '9' || statement[i+1] == '_') {
				i++
			}
			words = append(words, strings.ToUpper(statement[start:i+1]))
		}
	}
	if quote != 0 || markers != parameters || len(words) < 2 {
		return false
	}
	switch words[0] {
	case "INSERT", "UPDATE", "DELETE":
		// Conditional DML returns [applied]; Exec cannot faithfully return it.
		for _, word := range words[1:] {
			if word == "IF" {
				return false
			}
		}
		return true
	case "TRUNCATE":
		return true
	case "CREATE", "ALTER", "DROP":
		switch words[1] {
		case "TABLE", "INDEX", "TYPE":
			return true
		case "MATERIALIZED":
			return len(words) > 2 && words[2] == "VIEW"
		}
	}
	return false
}
