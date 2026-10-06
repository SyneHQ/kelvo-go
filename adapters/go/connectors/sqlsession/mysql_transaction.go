package sqlsession

import (
	"errors"

	"github.com/xwb1989/sqlparser"
)

var errMySQLTransaction = errors.New("transaction mode requires parsed INSERT, UPDATE or DELETE with unqualified tables and functions")

func mysqlTransaction(engine string) bool { return engine == "mysql" || engine == "mariadb" }

// Inspect the entire statement, including comma joins, targets and subqueries.
// A keyword regexp misses relations that are not immediately after FROM/JOIN.
// This bounds explicit references; source triggers/views/routines still govern
// implicit effects, so failed attempted mutations remain outcome-unknown.
func validateMySQLTransaction(statement string) error {
	parsed, err := sqlparser.Parse(statement)
	if err != nil {
		return errMySQLTransaction
	}
	switch parsed.(type) {
	case *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete:
	default:
		return errMySQLTransaction
	}
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch value := node.(type) {
		case sqlparser.TableName:
			if !value.Qualifier.IsEmpty() {
				return false, errMySQLTransaction
			}
		case *sqlparser.FuncExpr:
			if !value.Qualifier.IsEmpty() {
				return false, errMySQLTransaction
			}
		}
		return true, nil
	}, parsed)
}
