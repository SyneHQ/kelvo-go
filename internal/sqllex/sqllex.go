// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package sqllex splits PostgreSQL/DuckDB SQL into code, identifier, literal
// and comment segments for routing, rewriting and caching. It is not a
// security boundary: nothing that authorizes or denies SQL may depend on it.
package sqllex

import (
	"errors"
	"regexp"
	"strings"
)

// Kind classifies a segment.
type Kind int

const (
	Code       Kind = iota // keywords, operators, unquoted identifiers
	Identifier             // "double-quoted" identifier, quotes included
	Literal                // '...', E'...', $tag$...$tag$
	Comment                // -- line or /* nested block */
)

// Segment is a contiguous run of the input; concatenating Text restores it.
type Segment struct {
	Text string
	Kind Kind
}

// ErrUnterminated reports a literal, identifier or comment without its end.
var ErrUnterminated = errors.New("unterminated SQL literal, identifier or comment")

// Tags follow DuckDB: ASCII letters, digits (not first), underscore and any
// non-ASCII character.
var dollarTag = regexp.MustCompile(`^\$(?:[A-Za-z_[:^ascii:]][A-Za-z0-9_[:^ascii:]]*)?\$`)

func isWord(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// Lex splits sql. It fails closed: any unterminated construct is an error.
func Lex(sql string) ([]Segment, error) {
	var out []Segment
	start := 0
	emit := func(end int, kind Kind) {
		if end > start {
			out = append(out, Segment{sql[start:end], kind})
		}
		start = end
	}
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '\'' || c == '"':
			emit(i, Code)
			escapes := c == '\'' && i > 0 && (sql[i-1] == 'e' || sql[i-1] == 'E') && (i < 2 || !isWord(sql[i-2]))
			j, closed := i+1, false
			for j < len(sql) {
				if escapes && sql[j] == '\\' {
					j += 2
					continue
				}
				if sql[j] == c {
					if j+1 < len(sql) && sql[j+1] == c {
						j += 2
						continue
					}
					j, closed = j+1, true
					break
				}
				j++
			}
			if !closed {
				return nil, ErrUnterminated
			}
			i = j
			if c == '"' {
				emit(i, Identifier)
			} else {
				emit(i, Literal)
			}
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			emit(i, Code)
			if j := strings.IndexAny(sql[i:], "\n\r"); j < 0 {
				i = len(sql)
			} else {
				i += j
			}
			emit(i, Comment)
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			emit(i, Code)
			depth, j := 0, i
			for j < len(sql) {
				if strings.HasPrefix(sql[j:], "/*") {
					depth, j = depth+1, j+2
				} else if strings.HasPrefix(sql[j:], "*/") {
					depth, j = depth-1, j+2
					if depth == 0 {
						break
					}
				} else {
					j++
				}
			}
			if depth != 0 {
				return nil, ErrUnterminated
			}
			i = j
			emit(i, Comment)
		case c == '$' && (i == 0 || !isWord(sql[i-1])):
			tag := dollarTag.FindString(sql[i:])
			if tag == "" {
				i++
				continue
			}
			emit(i, Code)
			end := strings.Index(sql[i+len(tag):], tag)
			if end < 0 {
				return nil, ErrUnterminated
			}
			i += 2*len(tag) + end
			emit(i, Literal)
		default:
			i++
		}
	}
	emit(len(sql), Code)
	return out, nil
}

// Words returns lower-cased word tokens from code segments, and from quoted
// identifiers when identifiers is true (a quoted name still calls a function).
func Words(segments []Segment, identifiers bool) []string {
	var out []string
	for _, s := range segments {
		if s.Kind == Code || identifiers && s.Kind == Identifier {
			out = append(out, strings.FieldsFunc(strings.ToLower(s.Text), func(r rune) bool {
				return !(r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
			})...)
		}
	}
	return out
}

// Statements splits on semicolons in code and drops empty or comment-only parts.
func Statements(segments []Segment) []string {
	var out []string
	var cur strings.Builder
	meaningful := false
	flush := func() {
		if meaningful {
			out = append(out, strings.TrimSpace(cur.String()))
		}
		cur.Reset()
		meaningful = false
	}
	for _, s := range segments {
		if s.Kind != Code {
			cur.WriteString(s.Text)
			meaningful = meaningful || s.Kind != Comment
			continue
		}
		for i, p := range strings.Split(s.Text, ";") {
			if i > 0 {
				flush()
			}
			cur.WriteString(p)
			meaningful = meaningful || strings.TrimSpace(p) != ""
		}
	}
	flush()
	return out
}
