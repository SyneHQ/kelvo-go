// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlsession

import "testing"

func TestAutocommitRejectsAmbiguousSlotsAndResultClauses(t *testing.T) {
	for _, sql := range []string{"BEGIN", "SET ROLE owner", "UPDATE t SET n=1; DELETE FROM t", "INSERT INTO t VALUES (1) RETURNING id", "WITH x AS (DELETE FROM t) SELECT * FROM x", "CREATE PROCEDURE p AS $$ DELETE FROM t $$", "/*! INSERT INTO t VALUES (1) */", "INSERT INTO t VALUES ('unterminated)", "INSERT INTO t VALUES ('a\\'; DELETE FROM t)"} {
		if ValidateAutocommit([]string{"UPDATE t SET n=1", sql}) == nil {
			t.Fatalf("accepted %q", sql)
		}
	}
	for _, sql := range []string{"-- comment\nUPDATE t SET n=1;", "INSERT INTO t VALUES ('returning; BEGIN')", "CREATE TABLE t (id INT)", "DELETE FROM t WHERE id=1 /* trailing */"} {
		if err := ValidateAutocommit([]string{sql}); err != nil {
			t.Fatalf("rejected %q: %v", sql, err)
		}
	}
}
