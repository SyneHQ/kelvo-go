package runtime

import (
	"errors"
	"strings"

	"github.com/SYNEHQ/kelvo-go/operations"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

// sourceError returns the database's own error for a statement it refused.
// Callers use it only after the session opened, so connection and login
// failures never reach it; the class checks also drop server failures that
// can name hosts, roles or shutdowns rather than the caller's statement.
func sourceError(err error, statement *int) *operations.SourceError {
	var out operations.SourceError
	var pg *pgconn.PgError
	var my *mysqldriver.MySQLError
	switch {
	case errors.As(err, &pg):
		if pg.Severity == "FATAL" || pg.Severity == "PANIC" || connectionClass(pg.Code) {
			return nil
		}
		out = operations.SourceError{SQLState: pg.Code, Message: pg.Message, Detail: pg.Detail, Hint: pg.Hint,
			Schema: pg.SchemaName, Table: pg.TableName, Column: pg.ColumnName, Constraint: pg.ConstraintName}
	case errors.As(err, &my):
		state := string(my.SQLState[:])
		if connectionClass(state) {
			return nil
		}
		out = operations.SourceError{SQLState: state, Message: my.Message}
	default:
		return nil
	}
	for _, field := range []*string{&out.Message, &out.Detail, &out.Hint, &out.Schema, &out.Table, &out.Column, &out.Constraint} {
		*field = sourceText(*field)
	}
	if out.Message == "" {
		return nil
	}
	if !validSQLState(out.SQLState) {
		out.SQLState = ""
	}
	out.Statement = statement
	return &out
}

// connectionClass reports SQLSTATE classes about the connection, credentials
// or server lifecycle (08, 28, 57P) rather than the statement.
func connectionClass(state string) bool {
	return strings.HasPrefix(state, "08") || strings.HasPrefix(state, "28") || strings.HasPrefix(state, "57P")
}

func validSQLState(s string) bool {
	if len(s) != 5 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}

// sourceText makes driver text a valid receipt field: one line of UTF-8,
// trimmed to MaxSourceErrorText bytes on a rune boundary.
func sourceText(s string) string {
	s = strings.ToValidUTF8(s, "�")
	s = strings.Join(strings.FieldsFunc(s, func(r rune) bool { return r == 0 || r == '\r' || r == '\n' || r == '\t' }), " ")
	s = strings.TrimSpace(s)
	if len(s) > operations.MaxSourceErrorText {
		cut := operations.MaxSourceErrorText
		for cut > 0 && s[cut]&0xC0 == 0x80 {
			cut--
		}
		s = s[:cut]
	}
	return s
}
