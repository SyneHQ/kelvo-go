// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresTypedErrorCodes(t *testing.T) {
	cases := map[string]string{
		"28000": "UNAUTHENTICATED", "28P01": "UNAUTHENTICATED", "42501": "PERMISSION_DENIED",
		"3D000": "CONFIGURATION_ERROR", "3F000": "CONFIGURATION_ERROR",
		"42601": "INVALID_ARGUMENT", "42703": "INVALID_ARGUMENT", "42883": "INVALID_ARGUMENT", "42P01": "INVALID_ARGUMENT", "42P02": "INVALID_ARGUMENT",
		"08000": "UNAVAILABLE", "08001": "UNAVAILABLE", "08003": "UNAVAILABLE", "08006": "UNAVAILABLE",
		"40001": "UNAVAILABLE", "40P01": "UNAVAILABLE", "53300": "UNAVAILABLE", "55P03": "UNAVAILABLE",
		"57014": "UNAVAILABLE", "57P01": "UNAVAILABLE", "57P02": "UNAVAILABLE", "57P03": "UNAVAILABLE",
	}
	const secret = "private_password_private_database_private_query"
	for state, want := range cases {
		t.Run(state, func(t *testing.T) {
			server := &pgconn.PgError{Code: state, Message: secret, Detail: secret, Hint: secret, SchemaName: secret, TableName: secret, ColumnName: secret, InternalQuery: secret}
			for _, err := range []error{server, fmt.Errorf("private wrapper: %w", server), errors.Join(errors.New(secret), fmt.Errorf("nested: %w", server))} {
				got := sourceErrorCode(err)
				if got != want {
					t.Fatalf("mapped code %q, want %q", got, want)
				}
				if strings.Contains(got, secret) || strings.Contains(got, state) {
					t.Fatal("server diagnostics escaped the mapper")
				}
			}
		})
	}
}

type postgresCodeText struct{}

func (postgresCodeText) Error() string    { panic("classification must not read arbitrary error text") }
func (postgresCodeText) SQLState() string { return "42501" }

func TestPostgresUnknownAndLookalikeErrorsRemainUnknown(t *testing.T) {
	for _, state := range []string{"", "42000", "42502", "42804", "23505", "0A000", "08004", "08P01", "53400", "57P04", "XX000", "28P02", "28p01", "42501 private_password"} {
		if got := sourceErrorCode(&pgconn.PgError{Code: state, Message: "42501 permission denied private_password"}); got != "" {
			t.Fatalf("unlisted SQLSTATE was classified: %q", got)
		}
	}
	var typedNil *pgconn.PgError
	for _, err := range []error{nil, typedNil, context.Canceled, context.DeadlineExceeded, errors.New("ERROR 28P01: invalid password"), postgresCodeText{}} {
		if got := sourceErrorCode(err); got != "" {
			t.Fatalf("untyped error was classified: %q", got)
		}
	}
	// An ambiguous joined cause is not searched until a preferred code appears.
	joined := errors.Join(&pgconn.PgError{Code: "XX000"}, &pgconn.PgError{Code: "42501"})
	if got := sourceErrorCode(joined); got != "" {
		t.Fatalf("unknown first typed cause was overridden: %q", got)
	}
}
