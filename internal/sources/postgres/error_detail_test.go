// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresStatementErrorsCarryServerText(t *testing.T) {
	cases := map[string]struct {
		err  *pgconn.PgError
		want string
	}{
		"42883": {&pgconn.PgError{Code: "42883", Message: "operator does not exist: text = uuid", Hint: "No operator matches the given name and argument types. You might need to add explicit type casts.", Position: 104},
			"PostgreSQL error 42883: operator does not exist: text = uuid Hint: No operator matches the given name and argument types. You might need to add explicit type casts. (at character 104)"},
		"42804":     {&pgconn.PgError{Code: "42804", Message: "UNION types text and uuid cannot be matched"}, "PostgreSQL error 42804: UNION types text and uuid cannot be matched"},
		"22P02":     {&pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type uuid: "x"`, Position: 9}, `PostgreSQL error 22P02: invalid input syntax for type uuid: "x" (at character 9)`},
		"42P01":     {&pgconn.PgError{Code: "42P01", Message: `relation "public.nope" does not exist`}, `PostgreSQL error 42P01: relation "public.nope" does not exist`},
		"42601":     {&pgconn.PgError{Code: "42601", Message: "syntax error at or near\n\t\"FORM\""}, `PostgreSQL error 42601: syntax error at or near "FORM"`},
		"0A000":     {&pgconn.PgError{Code: "0A000", Message: "LATERAL not supported"}, "PostgreSQL error 0A000: LATERAL not supported"},
		"21000":     {&pgconn.PgError{Code: "21000", Message: "more than one row returned by a subquery used as an expression"}, "PostgreSQL error 21000: more than one row returned by a subquery used as an expression"},
		"no detail": {&pgconn.PgError{Code: "42703", Message: ""}, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := sourceErrorDetail(fmt.Errorf("wrapped: %w", tc.err)); got != tc.want {
				t.Fatalf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
	long := sourceErrorDetail(&pgconn.PgError{Code: "42601", Message: strings.Repeat("é", 600)})
	if utf8.RuneCountInString(long) != 500 || !strings.HasSuffix(long, "…") {
		t.Fatalf("not truncated to 500 runes: %d", utf8.RuneCountInString(long))
	}
}

func TestPostgresNonStatementErrorsKeepNoServerText(t *testing.T) {
	const secret = "role private_user at host 10.0.0.5"
	for state, code := range map[string]string{"42501": "PERMISSION_DENIED", "28P01": "UNAUTHENTICATED", "08006": "UNAVAILABLE", "57014": "UNAVAILABLE", "3D000": "CONFIGURATION_ERROR", "53300": "UNAVAILABLE"} {
		err := &pgconn.PgError{Code: state, Message: secret, Hint: secret}
		if got := sourceErrorDetail(err); got != "" {
			t.Fatalf("%s leaked server text: %q", state, got)
		}
		if got := sourceErrorCode(err); got != code {
			t.Fatalf("%s code %q, want %q", state, got, code)
		}
	}
}
