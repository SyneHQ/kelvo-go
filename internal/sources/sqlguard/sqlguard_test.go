// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlguard

import "testing"

func TestReadEnvelope(t *testing.T) {
	for _, sql := range []string{"SELECT 1", " SELECT 'DELETE;UPDATE', \"drop\" FROM t; -- comment", "WITH q AS (SELECT 1 n) SELECT n FROM q", "SELECT [delete], `update` FROM t", "SELECT 'it''s safe'", "/* context */ SELECT 1"} {
		if _, err := ReadOnly(sql); err != nil {
			t.Errorf("rejected %q: %v", sql, err)
		}
	}
	for _, sql := range []string{"DELETE FROM t", "SELECT 1; DELETE FROM t", "SELECT 1 INTO t", "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x", "SELECT 1; SELECT 2", "PRAGMA table_info(t)", "SELECT 'unterminated", "SELECT 1 /* unclosed", "SELECT 1 /*! DELETE FROM t */", "SELECT 1 /* /* */ DELETE FROM t */", "SELECT $$x$$", "SELECT 'x\\' + '; DELETE FROM t --'", "SELECT 1; /* ok */ ;"} {
		if _, err := ReadOnly(sql); err == nil {
			t.Errorf("accepted %q", sql)
		}
	}
}

func TestReadOnlyDollarParameters(t *testing.T) {
	for _, q := range []string{"SELECT * FROM t WHERE id=$1", "SELECT $12"} {
		if _, e := ReadOnlyWithOptions(q, true); e != nil {
			t.Fatal(q, e)
		}
	}
	for _, q := range []string{"SELECT $0", "SELECT $1x", "SELECT $$x$$", "SELECT $tag$x$tag$", "SELECT $1; SELECT 2"} {
		if _, e := ReadOnlyWithOptions(q, true); e == nil {
			t.Fatal(q)
		}
	}
}

func TestRejectExecutableOptimizerHints(t *testing.T) {
	for _, statement := range []string{
		"SELECT /*+ MAX_EXECUTION_TIME(3600000) */ SLEEP(5)",
		"SELECT /*+ SET_VAR(max_execution_time=0) */ 1",
		"WITH q AS (SELECT /*+ MAX_EXECUTION_TIME(0) */ 1) SELECT * FROM q",
		"SELECT #'\n/*+ MAX_EXECUTION_TIME(3600000) */ SLEEP(5) #'",
	} {
		if _, err := ReadOnly(statement); err == nil {
			t.Fatal("accepted an executable optimizer hint")
		}
	}
	if _, err := ReadOnly("SELECT '/*+ MAX_EXECUTION_TIME(0) */' AS example"); err != nil {
		t.Fatal("rejected inert quoted content")
	}
}
