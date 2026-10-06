package cloudapi

import (
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// DatabricksStatement maps positional markers to provider names, never values.
// Canonical :p1 ... :pN markers are also accepted. Quoted text and comments are
// copied verbatim and cannot consume a parameter.
func DatabricksStatement(sql string, count int) (string, error) {
	if count == 0 {
		return sql, nil
	}
	if count < 0 || count > 1000 {
		return "", operations.ErrInvalid
	}
	var out strings.Builder
	seen := make([]bool, count)
	position, named := 0, false
	for i := 0; i < len(sql); {
		start := i
		switch {
		case sql[i] == '\'' || sql[i] == '"' || sql[i] == '`':
			quote := sql[i]
			i++
			closed := false
			for i < len(sql) {
				if sql[i] == '\\' {
					i += 2
					continue
				}
				if sql[i] == quote {
					i++
					if i < len(sql) && sql[i] == quote {
						i++
						continue
					}
					closed = true
					break
				}
				i++
			}
			if !closed || i > len(sql) {
				return "", operations.ErrInvalid
			}
		case strings.HasPrefix(sql[i:], "--"):
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case strings.HasPrefix(sql[i:], "/*"):
			i += 2
			depth := 1
			for i < len(sql) && depth > 0 {
				if strings.HasPrefix(sql[i:], "/*") {
					depth++
					i += 2
				} else if strings.HasPrefix(sql[i:], "*/") {
					depth--
					i += 2
				} else {
					i++
				}
			}
			if depth != 0 {
				return "", operations.ErrInvalid
			}
		case sql[i] == '?':
			if named || position == count {
				return "", operations.ErrInvalid
			}
			seen[position] = true
			position++
			i++
			out.WriteString(":p" + strconv.Itoa(position))
			continue
		case sql[i] == ':' && i+1 < len(sql) && sql[i+1] != ':' && (i == 0 || sql[i-1] != ':'):
			if position > 0 {
				return "", operations.ErrInvalid
			}
			i++
			for i < len(sql) && (sql[i] >= 'a' && sql[i] <= 'z' || sql[i] >= 'A' && sql[i] <= 'Z' || sql[i] >= '0' && sql[i] <= '9' || sql[i] == '_') {
				i++
			}
			marker := sql[start+1 : i]
			if len(marker) < 2 || marker[0] != 'p' {
				return "", operations.ErrInvalid
			}
			n, err := strconv.Atoi(marker[1:])
			if err != nil || n < 1 || n > count || marker != "p"+strconv.Itoa(n) {
				return "", operations.ErrInvalid
			}
			seen[n-1] = true
			named = true
		default:
			i++
		}
		out.WriteString(sql[start:i])
	}
	for _, used := range seen {
		if !used {
			return "", operations.ErrInvalid
		}
	}
	return out.String(), nil
}
