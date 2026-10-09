// Package sqlsession runs approved changes on a dedicated SQL connection.
// The caller owns authorization and records dispatch before calling Execute.
package sqlsession

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
)

type Options struct {
	Transaction bool
	Role        string
	Isolation   string
}

type Statement struct {
	SQL        string
	Parameters []any
}

type Outcome string

const (
	Succeeded Outcome = "succeeded"
	Failed    Outcome = "failed"
	Unknown   Outcome = "unknown"
)

// Result reports committed statements only. Unknown means the source must be
// reconciled; retrying the request can repeat a write. AffectedRows is nil when
// any driver cannot report it or when the final effect is uncertain.
type Result struct {
	Outcome      Outcome
	Completed    int
	Attempted    int
	AffectedRows *int64
}

var (
	roleIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$#.-]{0,127}$`)
	controlCommand = regexp.MustCompile(`(?i)^\s*(begin\b|start\b|commit\b|rollback\b|abort\b|savepoint\b|release\b|prepare\b|deallocate\b|discard\b|set\b|reset\b|use\b|execute\b|exec\b|call\b|do\b|end\b)`)
	dmlStatement   = regexp.MustCompile(`(?i)^\s*(insert|update|delete|merge)\b`)
)

func Validate(engine string, statements []string, options Options) error {
	switch engine {
	case "postgresql", "mysql", "mariadb", "sqlserver", "oracle", "snowflake", "clickhouse", "d1", "databricks":
	default:
		return errors.New("unsupported change engine")
	}
	if _, err := isolationLevel(engine, options); err != nil {
		return err
	}
	if len(statements) == 0 || len(statements) > 100 {
		return errors.New("supply between 1 and 100 statements")
	}
	if options.Role != "" && !roleIdentifier.MatchString(options.Role) {
		return errors.New("invalid database role identifier")
	}
	if (engine == "clickhouse" || engine == "d1" || engine == "databricks") && options.Transaction {
		return errors.New("selected change protocol does not support transactions")
	}
	total := 0
	for _, statement := range statements {
		total += len(statement)
		if total > 1<<20 {
			return errors.New("change statements exceed 1 MiB")
		}
		code, err := statementCode(statement)
		if err != nil {
			return err
		}
		if strings.TrimSpace(code) == "" || controlCommand.MatchString(code) {
			return errors.New("empty, session-control or procedural SQL requires a separate operation")
		}
		if options.Transaction && mysqlTransaction(engine) {
			if err := validateMySQLTransaction(statement); err != nil {
				return err
			}
		}
		if options.Transaction && (engine == "mysql" || engine == "mariadb" || engine == "oracle" || engine == "snowflake") && !dmlStatement.MatchString(code) {
			return errors.New("transaction mode requires DML; DDL can commit implicitly")
		}
	}
	return nil
}

// Execute consumes pool after acquiring its connection. The caller must supply
// an exclusive operation-owned pool and must not reuse it. The held connection
// remains usable until cleanup; queued/new borrowers are rejected. A changed
// role or session setting cannot pass to another operation. Writes are not retried.
func Execute(ctx context.Context, pool *sql.DB, engine string, statements []string, options Options) (Result, error) {
	bound := make([]Statement, len(statements))
	for i, statement := range statements {
		bound[i].SQL = statement
	}
	return ExecuteStatements(ctx, pool, engine, bound, options)
}

// ExecuteStatements consumes an exclusive operation-owned pool, as Execute does.
func ExecuteStatements(ctx context.Context, pool *sql.DB, engine string, statements []Statement, options Options) (Result, error) {
	result := Result{Outcome: Failed}
	text := make([]string, len(statements))
	for i, statement := range statements {
		text[i] = statement.SQL
	}
	if err := Validate(engine, text, options); err != nil {
		return result, err
	}
	if pool == nil {
		return result, errors.New("connector has no SQL execution session")
	}
	conn, err := pool.Conn(ctx)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	// Consume the operation-owned pool after acquisition. The held connection
	// remains usable, but queued/new borrowers cannot receive this session.
	// database/sql owns rollback and physical driver release; do not use Raw
	// after cancellation can close the connection.
	if err := pool.Close(); err != nil {
		return result, err
	}
	if options.Role != "" {
		command := `SET ROLE "` + options.Role + `"`
		switch engine {
		case "mysql", "mariadb":
			command = "SET ROLE `" + options.Role + "`"
		case "sqlserver":
			command = "EXECUTE AS USER = '" + options.Role + "'"
		case "snowflake":
			command = `USE ROLE "` + options.Role + `"`
		}
		if _, err = conn.ExecContext(ctx, command); err != nil {
			return result, fmt.Errorf("activate database role: %w", err)
		}
	}
	if options.Transaction && mysqlTransaction(engine) {
		var count int
		err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' AND (engine IS NULL OR engine <> 'InnoDB')").Scan(&count)
		if err != nil || count > 0 {
			return result, errors.New("transaction mode requires all target-database tables to use InnoDB")
		}
	}
	var tx *sql.Tx
	if options.Transaction {
		level, _ := isolationLevel(engine, options)
		tx, err = conn.BeginTx(ctx, &sql.TxOptions{Isolation: level})
		if err != nil {
			return result, err
		}
		defer tx.Rollback()
	}
	var affected int64
	knownRows := true
	for index, statement := range statements {
		if err = ctx.Err(); err != nil {
			if tx != nil {
				rollbackErr := tx.Rollback()
				if rollbackErr != nil || mysqlTransaction(engine) && result.Attempted > 0 {
					result.Outcome = Unknown
				}
			}
			return result, err
		}
		result.Attempted = index + 1
		var sqlResult sql.Result
		if tx != nil {
			sqlResult, err = tx.ExecContext(ctx, statement.SQL, statement.Parameters...)
		} else {
			sqlResult, err = conn.ExecContext(ctx, statement.SQL, statement.Parameters...)
		}
		if err != nil {
			// Even a timeout can occur after the server committed an autocommit
			// statement. MySQL triggers, views or routines can reach a different
			// storage engine; a successful rollback cannot prove their effects
			// absent, even when the selected database's base tables use InnoDB.
			result.Outcome = Unknown
			if tx != nil && tx.Rollback() == nil && !mysqlTransaction(engine) {
				result.Outcome = Failed
			}
			return result, fmt.Errorf("statement %d: %w", index+1, err)
		}
		if tx == nil {
			result.Completed++
		}
		rows, rowsErr := sqlResult.RowsAffected()
		if rowsErr != nil || rows < 0 || rows > math.MaxInt64-affected {
			knownRows = false
		} else {
			affected += rows
		}
	}
	if tx != nil {
		if err = tx.Commit(); err != nil {
			result.Outcome = Unknown
			return result, fmt.Errorf("commit outcome requires reconciliation: %w", err)
		}
		result.Completed = len(statements)
	}
	result.Outcome = Succeeded
	if knownRows {
		result.AffectedRows = &affected
	}
	return result, nil
}

// IsolationLevels advertises only modes implemented by this driver's transaction API.
func IsolationLevels(engine string) []string {
	switch engine {
	case "postgresql", "mysql", "mariadb", "sqlserver":
		return []string{"default", "read_uncommitted", "read_committed", "repeatable_read", "serializable"}
	case "oracle":
		return []string{"default", "read_committed", "serializable"}
	case "snowflake":
		return []string{"default", "read_committed"}
	}
	return nil
}
func isolationLevel(engine string, options Options) (sql.IsolationLevel, error) {
	if options.Isolation == "" {
		return sql.LevelDefault, nil
	}
	if !options.Transaction {
		return 0, errors.New("isolation requires transaction mode")
	}
	supported := false
	for _, level := range IsolationLevels(engine) {
		supported = supported || level == options.Isolation
	}
	if !supported {
		return 0, errors.New("unsupported transaction isolation")
	}
	switch options.Isolation {
	case "default":
		return sql.LevelDefault, nil
	case "read_uncommitted":
		return sql.LevelReadUncommitted, nil
	case "read_committed":
		return sql.LevelReadCommitted, nil
	case "repeatable_read":
		return sql.LevelRepeatableRead, nil
	case "serializable":
		return sql.LevelSerializable, nil
	}
	return 0, errors.New("unsupported transaction isolation")
}
