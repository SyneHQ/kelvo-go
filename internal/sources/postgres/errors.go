// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// sourceErrorCode reads only typed SQLSTATE, never server text or object names.
// The exact PostgreSQL protocol codes also apply to compatible family adapters.
// Unknown codes keep the caller's conservative generic failure classification.
// Reference: https://www.postgresql.org/docs/current/errcodes-appendix.html
func sourceErrorCode(err error) string {
	var server *pgconn.PgError
	if !errors.As(err, &server) || server == nil {
		return ""
	}
	switch server.Code {
	case "28000", "28P01": // invalid_authorization_specification, invalid_password
		return "UNAUTHENTICATED"
	case "42501": // insufficient_privilege, not the entire syntax/access class
		return "PERMISSION_DENIED"
	case "3D000", "3F000": // invalid_catalog_name, invalid_schema_name
		return "CONFIGURATION_ERROR"
	case "42601", "42703", "42883", "42P01", "42P02":
		// syntax_error, undefined_column/function/table/parameter
		return "INVALID_ARGUMENT"
	case "08000", "08001", "08003", "08006", "40001", "40P01", "53300", "55P03", "57P01", "57P02", "57P03":
		// Connection failure, serialization/deadlock, exhausted connection slots,
		// unavailable lock, shutdown or a server not accepting connections yet.
		return "UNAVAILABLE"
	case "57014":
		// query_canceled alone does not distinguish statement_timeout from an
		// administrative cancel. sqlnative handles actual caller deadlines first.
		return "UNAVAILABLE"
	}
	return ""
}
