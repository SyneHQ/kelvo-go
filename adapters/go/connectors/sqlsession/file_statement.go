package sqlsession

import (
	"errors"
	"strings"
	"unicode"
)

var errFileStatement = errors.New("unsupported file mutation statement")

// FileStatement validates one mutation slot. Engine authorization additionally
// denies filesystem, extension and session controls while the SQL is prepared.
func FileStatement(statement string) error {
	code, err := statementCode(statement)
	if err != nil {
		return err
	}
	fields := strings.Fields(strings.ToUpper(code))
	if len(fields) == 0 {
		return errFileStatement
	}
	switch fields[0] {
	case "INSERT", "UPDATE", "DELETE", "REPLACE", "MERGE", "CREATE", "ALTER", "DROP", "WITH":
	default:
		return errFileStatement
	}
	for _, token := range strings.FieldsFunc(strings.ToUpper(code), func(r rune) bool { return !unicode.IsLetter(r) && r != '_' }) {
		switch token {
		case "ATTACH", "DETACH", "PRAGMA", "VACUUM", "COPY", "LOAD", "INSTALL", "EXPORT", "IMPORT", "CALL", "EXEC", "EXECUTE", "RESET", "USE", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "SECRET", "SEQUENCE":
			return errFileStatement
		}
	}
	return nil
}
