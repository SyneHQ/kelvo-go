// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package sqlguard implements the native adapters' conservative read-query
// syntax envelope. Database permissions remain the authorization boundary.
package sqlguard

import (
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"strings"
)

// ReadOnly rejects multiple statements and write/session/administrative syntax.
// Ambiguous dialect quoting is rejected, not guessed. Pass literal values with
// bound parameters. This is a syntax restriction, not a complete SQL parser or
// a replacement for database-enforced read-only credentials.
func ReadOnly(sql string) (string, error) { return ReadOnlyWithOptions(sql, false) }

// ReadOnlyWithOptions permits only PostgreSQL-style $1 positional parameters when enabled.
func ReadOnlyWithOptions(sql string, allowDollarParams bool) (string, error) {
	deny := func() (string, error) {
		return "", query.NewError("INVALID_ARGUMENT", "Native queries require one read-only SELECT or WITH statement")
	}
	if len(sql) == 0 || len(sql) > 64<<10 {
		return deny()
	}
	var tokens []string
	end := -1
	for i := 0; i < len(sql); {
		c := sql[i]
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			i++
			continue
		}
		if c == 0 || c == '\\' {
			return deny()
		}
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i += 2
			if i < len(sql) && sql[i] == '!' {
				return deny()
			}
			closed := false
			for i+1 < len(sql) {
				if sql[i] == '/' && sql[i+1] == '*' {
					return deny()
				}
				if sql[i] == '*' && sql[i+1] == '/' {
					i += 2
					closed = true
					break
				}
				i++
			}
			if !closed {
				return deny()
			}
			continue
		}
		if end >= 0 {
			return deny()
		}
		if c == ';' {
			end = i
			i++
			continue
		}
		if c == '\'' || c == '"' || c == '`' || c == '[' {
			close := c
			if c == '[' {
				close = ']'
			}
			i++
			closed := false
			for i < len(sql) {
				if sql[i] == '\\' || sql[i] == 0 {
					return deny()
				}
				if sql[i] == close {
					if i+1 < len(sql) && sql[i+1] == close {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return deny()
			}
			continue
		}
		if c == '$' {
			if !allowDollarParams || i+1 >= len(sql) || sql[i+1] < '1' || sql[i+1] > '9' {
				return deny()
			}
			i += 2
			for i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
				i++
			}
			if i < len(sql) && (alpha(sql[i]) || sql[i] == '_') {
				return deny()
			}
			continue
		} // reject dollar-quoted bodies
		if alpha(c) {
			start := i
			i++
			for i < len(sql) && (alpha(sql[i]) || sql[i] >= '0' && sql[i] <= '9' || sql[i] == '_') {
				i++
			}
			tokens = append(tokens, strings.ToUpper(sql[start:i]))
			continue
		}
		i++
	}
	if len(tokens) == 0 || (tokens[0] != "SELECT" && tokens[0] != "WITH") {
		return deny()
	}
	for _, token := range tokens {
		switch token {
		case "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE", "COPY", "UNLOAD", "LOAD", "EXPORT", "IMPORT", "CALL", "EXEC", "EXECUTE", "GRANT", "REVOKE", "ATTACH", "DETACH", "PRAGMA", "VACUUM", "REINDEX", "SET", "RESET", "USE", "INTO", "PUT", "REMOVE", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "LOCK", "UNLOCK":
			return deny()
		}
	}
	if end >= 0 {
		sql = sql[:end]
	}
	return strings.TrimSpace(sql), nil
}
func alpha(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }
