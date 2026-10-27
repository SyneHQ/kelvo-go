// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqllex

import (
	"slices"
	"strings"
	"testing"
)

func TestLexWordsAndStatements(t *testing.T) {
	for _, tc := range []struct {
		sql        string
		words      string // code words, then quoted-identifier words after |
		statements int
		err        bool
	}{
		{sql: "SELECT 'a;DROP TABLE x'", words: "select|select", statements: 1},
		{sql: "SELECT 'it''s'; DROP TABLE x", words: "select drop table x|select drop table x", statements: 2},
		{sql: "SELECT ''''; DROP TABLE x", words: "select drop table x|select drop table x", statements: 2},
		{sql: `SELECT E'\''; DROP TABLE x`, words: "select e drop table x|select e drop table x", statements: 2},
		{sql: `SELECT E'\\'; DROP TABLE x`, words: "select e drop table x|select e drop table x", statements: 2},
		{sql: "SELECT 'a\\'; DROP TABLE x", words: "select drop table x|select drop table x", statements: 2},
		{sql: "SELECT $$DROP; x$$", words: "select|select", statements: 1},
		{sql: "SELECT $t$ $$ DROP $t$; DROP", words: "select drop|select drop", statements: 2},
		{sql: "SELECT $1, $2", words: "select 1 2|select 1 2", statements: 1},
		{sql: "SELECT 1 -- ; DROP TABLE x", words: "select 1|select 1", statements: 1},
		{sql: "SELECT /* a /* b */ ; DROP */ 1", words: "select 1|select 1", statements: 1},
		{sql: `SELECT "read_csv"('x')`, words: "select|select read_csv", statements: 1},
		{sql: `SELECT "a""b;c" FROM t`, words: "select from t|select a b c from t", statements: 1},
		{sql: "SELECT 'héllo ünïcode'; SELECT 'ö'", words: "select select|select select", statements: 2},
		{sql: "SELECT ;;; ", words: "select|select", statements: 1},
		{sql: "-- only\n/* c */", words: "|", statements: 0},
		{sql: "SELECT * FROM --\rread_csv('/p')", words: "select from read_csv|select from read_csv", statements: 1},
		{sql: "SELECT $é$'$é$ AS x FROM t --'", words: "select as x from t|select as x from t", statements: 1},
		{sql: "SELECT $日本$;$日本$", words: "select|select", statements: 1},
		{sql: "SELECT 'open", err: true},
		{sql: `SELECT "open`, err: true},
		{sql: "SELECT E'\\'", err: true},
		{sql: "SELECT /* /* */", err: true},
		{sql: "SELECT $q$ never", err: true},
	} {
		segs, err := Lex(tc.sql)
		if tc.err {
			if err == nil {
				t.Errorf("%q: expected lexer error", tc.sql)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%q: %v", tc.sql, err)
		}
		var b strings.Builder
		for _, s := range segs {
			b.WriteString(s.Text)
		}
		if b.String() != tc.sql {
			t.Fatalf("%q: segments do not round-trip", tc.sql)
		}
		got := strings.Join(Words(segs, false), " ") + "|" + strings.Join(Words(segs, true), " ")
		if got != tc.words {
			t.Errorf("%q: words %q, want %q", tc.sql, got, tc.words)
		}
		if n := len(Statements(segs)); n != tc.statements {
			t.Errorf("%q: %d statements, want %d", tc.sql, n, tc.statements)
		}
	}
	if !slices.Contains(Words(mustLex(t, "xéread_csv"), false), "read_csv") {
		t.Error("non-ASCII bytes must split tokens like the original guard")
	}
}

func mustLex(t *testing.T, sql string) []Segment {
	s, err := Lex(sql)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
