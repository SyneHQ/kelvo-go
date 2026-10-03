// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	driver "github.com/go-sql-driver/mysql"
)

func mysqlTestState(value string) (state [5]byte) { copy(state[:], value); return state }
func TestMySQLTypedErrorCodesAcrossCompatibleFamilies(t *testing.T) {
	cases := []struct {
		number      uint16
		state, code string
	}{
		{1045, "28000", "UNAUTHENTICATED"},
		{1044, "42000", "PERMISSION_DENIED"}, {1142, "42000", "PERMISSION_DENIED"}, {1143, "42000", "PERMISSION_DENIED"}, {1227, "42000", "PERMISSION_DENIED"},
		{1046, "3D000", "CONFIGURATION_ERROR"}, {1049, "42000", "CONFIGURATION_ERROR"},
		{1054, "42S22", "INVALID_ARGUMENT"}, {1064, "42000", "INVALID_ARGUMENT"}, {1065, "42000", "INVALID_ARGUMENT"}, {1146, "42S02", "INVALID_ARGUMENT"},
		{1040, "08004", "UNAVAILABLE"}, {1053, "08S01", "UNAVAILABLE"}, {1205, "HY000", "UNAVAILABLE"}, {1213, "40001", "UNAVAILABLE"}, {1317, "70100", "UNAVAILABLE"},
	}
	const secret = "private_password_private_database_private_query"
	for _, kind := range []string{"mysql", "mariadb"} {
		for _, test := range cases {
			t.Run(fmt.Sprintf("%s_%d", kind, test.number), func(t *testing.T) {
				for _, state := range [][5]byte{mysqlTestState(test.state), {}} {
					server := &driver.MySQLError{Number: test.number, SQLState: state, Message: secret}
					for _, err := range []error{server, fmt.Errorf("private wrapper: %w", server), errors.Join(errors.New(secret), server)} {
						got := sourceErrorCode(kind, err)
						if got != test.code {
							t.Fatalf("mapped code %q, want %q", got, test.code)
						}
						if strings.Contains(got, secret) {
							t.Fatal("server message escaped the mapper")
						}
					}
				}
			})
		}
	}
}
func TestMySQLTimeoutCodeDoesNotCollideWithMariaDB(t *testing.T) {
	server := &driver.MySQLError{Number: 3024, SQLState: mysqlTestState("HY000"), Message: "private statement"}
	if got := sourceErrorCode("mysql", server); got != "DEADLINE_EXCEEDED" {
		t.Fatalf("MySQL timeout: %q", got)
	}
	if got := sourceErrorCode("mariadb", server); got != "" {
		t.Fatalf("MariaDB reserved code misclassified: %q", got)
	}
	// Generic interruption does not prove a deadline or client cancellation.
	for _, kind := range []string{"mysql", "mariadb"} {
		interrupted := &driver.MySQLError{Number: 1317, SQLState: mysqlTestState("70100")}
		if got := sourceErrorCode(kind, interrupted); got != "UNAVAILABLE" {
			t.Fatalf("generic interruption: %q", got)
		}
	}
}

type mysqlCodeText struct{}

func (mysqlCodeText) Error() string { panic("classification must not read arbitrary error text") }

func TestMySQLUnknownAndContradictoryProtocolCodesRemainUnknown(t *testing.T) {
	var typedNil *driver.MySQLError
	for _, err := range []error{nil, typedNil, context.Canceled, context.DeadlineExceeded, errors.New("Error 1045 (28000): Access denied private_password"), mysqlCodeText{}} {
		if got := sourceErrorCode("mysql", err); got != "" {
			t.Fatalf("untyped error was classified: %q", got)
		}
	}
	for _, number := range []uint16{0, 1043, 1055, 1062, 1129, 1130, 1147, 1226, 1698, 1969, 9999} {
		if got := sourceErrorCode("mysql", &driver.MySQLError{Number: number, SQLState: mysqlTestState("42000"), Message: "Access denied private_password"}); got != "" {
			t.Fatalf("unlisted server number was classified: %q", got)
		}
	}
	for _, state := range []string{"42000", "HY000", "28p00", "ABCDE"} {
		if got := sourceErrorCode("mysql", &driver.MySQLError{Number: 1045, SQLState: mysqlTestState(state)}); got != "" {
			t.Fatalf("conflicting SQLSTATE was classified: %q", got)
		}
	}
	if got := sourceErrorCode("unknown-family", &driver.MySQLError{Number: 1045}); got != "" {
		t.Fatalf("unknown protocol family was classified: %q", got)
	}
	joined := errors.Join(&driver.MySQLError{Number: 9999}, &driver.MySQLError{Number: 1045})
	if got := sourceErrorCode("mysql", joined); got != "" {
		t.Fatalf("unknown first typed cause was overridden: %q", got)
	}
}
