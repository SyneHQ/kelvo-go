package sqlsession

import (
	"errors"
	"strings"
)

// statementCode is a conservative slot validator, not a SQL authorization
// parser. Approval and source permissions remain mandatory. Ambiguous escapes
// and procedural bodies must use a separately reviewed migration operation.
func statementCode(input string) (string, error) {
	var code strings.Builder
	ended := false
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if ch == 0 {
			return "", errors.New("SQL contains NUL")
		}
		if ch == '-' && i+1 < len(input) && input[i+1] == '-' {
			for i < len(input) && input[i] != '\n' {
				i++
			}
			code.WriteByte(' ')
			continue
		}
		if ch == '#' {
			return "", errors.New("dialect-dependent hash comments require migrations")
		}
		if ch == '/' && i+1 < len(input) && input[i+1] == '*' {
			depth := 1
			i += 2
			for ; i < len(input) && depth > 0; i++ {
				if input[i] == '!' || input[i] == '+' {
					return "", errors.New("executable comments and optimizer hints require migrations")
				}
				if i+1 < len(input) && input[i] == '/' && input[i+1] == '*' {
					depth++
					i++
				} else if i+1 < len(input) && input[i] == '*' && input[i+1] == '/' {
					depth--
					i++
				}
			}
			if depth != 0 {
				return "", errors.New("unterminated SQL comment")
			}
			i--
			code.WriteByte(' ')
			continue
		}
		if ch == ';' {
			if ended {
				return "", errors.New("multiple SQL commands in one statement")
			}
			ended = true
			continue
		}
		if ended {
			if ch != ' ' && ch != '\n' && ch != '\r' && ch != '\t' {
				return "", errors.New("multiple SQL commands in one statement")
			}
			continue
		}
		if ch == '$' {
			for j := i + 1; j < len(input); j++ {
				if input[j] == '$' {
					return "", errors.New("dollar-quoted bodies require migrations")
				}
				if !(input[j] == '_' || input[j] >= 'a' && input[j] <= 'z' || input[j] >= 'A' && input[j] <= 'Z' || input[j] >= '0' && input[j] <= '9') {
					break
				}
			}
		}
		if ch == '\'' || ch == '"' || ch == '`' || ch == '[' {
			end := ch
			if ch == '[' {
				end = ']'
			}
			closed := false
			for i++; i < len(input); i++ {
				if input[i] == '\\' || input[i] == 0 {
					return "", errors.New("ambiguous SQL escapes require parameters or migrations")
				}
				if input[i] == end {
					if i+1 < len(input) && input[i+1] == end {
						i++
						continue
					}
					closed = true
					break
				}
			}
			if !closed {
				return "", errors.New("unterminated SQL quote")
			}
			code.WriteString(" identifier ")
			continue
		}
		code.WriteByte(ch)
	}
	return code.String(), nil
}
