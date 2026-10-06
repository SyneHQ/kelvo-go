package sqlsession

import (
	"errors"
	"regexp"
	"strings"
)

var nativeReturning = regexp.MustCompile(`(?i)\breturning\b`)

// ValidateAutocommit checks every statement slot before a native protocol sends
// the first mutation. It does not grant authority or emulate transactions.
func ValidateAutocommit(statements []string) error {
	if len(statements) < 1 || len(statements) > 100 {
		return errors.New("supply between 1 and 100 statements")
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
		fields := strings.Fields(strings.ToUpper(code))
		if len(fields) == 0 {
			return errors.New("empty statement")
		}
		switch fields[0] {
		case "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE", "REPLACE", "UPSERT":
		default:
			return errors.New("unsupported native mutation statement")
		}
		if nativeReturning.MatchString(code) {
			return errors.New("result-producing statement requires a separate operation")
		}
	}
	return nil
}
