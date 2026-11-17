// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mysql

import (
	"fmt"
	"testing"

	driver "github.com/go-sql-driver/mysql"
)

func TestMySQLStatementErrorsCarryServerText(t *testing.T) {
	for number, want := range map[uint16]string{
		1064: "MySQL error 1064: You have an error in your SQL syntax; check the manual near 'FORM t' at line 1",
		1054: "MySQL error 1054: Unknown column 'nope' in 'field list'",
		1045: "", 1142: "", 1044: "", 1049: "", 1040: "", 3024: "",
	} {
		msg := map[uint16]string{1064: "You have an error in your SQL syntax; check the manual\nnear 'FORM t' at line 1", 1054: "Unknown column 'nope' in 'field list'"}[number]
		if msg == "" {
			msg = "Access denied for user 'private'@'10.0.0.5'"
		}
		err := fmt.Errorf("wrapped: %w", &driver.MySQLError{Number: number, Message: msg})
		if got := sourceErrorDetail("mysql", err); got != want {
			t.Fatalf("%d: got %q, want %q", number, got, want)
		}
	}
	if sourceErrorDetail("sqlserver", &driver.MySQLError{Number: 1064, Message: "x"}) != "" {
		t.Fatal("non-MySQL kind passed text")
	}
}
