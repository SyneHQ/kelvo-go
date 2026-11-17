// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mysql

import (
	"errors"
	"fmt"

	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlnative"

	driver "github.com/go-sql-driver/mysql"
)

// sourceErrorCode recognizes exact server numbers, optionally cross-checking
// SQLSTATE when the server supplied it. Neither error messages nor the broad
// SQLSTATE 42000 class are enough to classify an error as an access denial.
// References:
// https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html
// https://mariadb.com/docs/server/reference/error-codes/mariadb-error-codes-3000-to-3099/e3024
func sourceErrorCode(kind string, err error) string {
	if kind != "mysql" && kind != "mariadb" {
		return ""
	}
	var server *driver.MySQLError
	if !errors.As(err, &server) || server == nil {
		return ""
	}
	code, state := "", ""
	switch server.Number {
	case 1045: // ER_ACCESS_DENIED_ERROR
		code, state = "UNAUTHENTICATED", "28000"
	case 1044, 1142, 1143, 1227: // DB/table/column/specific privilege denied
		code, state = "PERMISSION_DENIED", "42000"
	case 1046: // ER_NO_DB_ERROR
		code, state = "CONFIGURATION_ERROR", "3D000"
	case 1049: // ER_BAD_DB_ERROR
		code, state = "CONFIGURATION_ERROR", "42000"
	case 1054: // ER_BAD_FIELD_ERROR
		code, state = "INVALID_ARGUMENT", "42S22"
	case 1064, 1065: // ER_PARSE_ERROR, ER_EMPTY_QUERY
		code, state = "INVALID_ARGUMENT", "42000"
	case 1146: // ER_NO_SUCH_TABLE
		code, state = "INVALID_ARGUMENT", "42S02"
	case 1040: // ER_CON_COUNT_ERROR
		code, state = "UNAVAILABLE", "08004"
	case 1053: // ER_SERVER_SHUTDOWN
		code, state = "UNAVAILABLE", "08S01"
	case 1205: // ER_LOCK_WAIT_TIMEOUT
		code, state = "UNAVAILABLE", "HY000"
	case 1213: // ER_LOCK_DEADLOCK
		code, state = "UNAVAILABLE", "40001"
	case 1317: // ER_QUERY_INTERRUPTED; not necessarily a deadline
		code, state = "UNAVAILABLE", "70100"
	case 3024:
		// MySQL ER_QUERY_TIMEOUT. MariaDB reserves this same numeric code for a
		// different meaning, so it must not inherit this timeout classification.
		if kind != "mysql" {
			return ""
		}
		code, state = "DEADLINE_EXCEEDED", "HY000"
	default:
		return ""
	}
	// Older server responses can omit SQLSTATE. Conflicting supplied protocol
	// metadata is left unknown rather than guessed from a numeric collision.
	if server.SQLState != [5]byte{} && string(server.SQLState[:]) != state {
		return ""
	}
	return code
}

// sourceErrorDetail returns the server's own text for errors the caller's SQL
// caused; "" otherwise. Privilege (1044/1142/1143/1227), auth, connection and
// configuration numbers are excluded: their text names users and hosts.
func sourceErrorDetail(kind string, err error) string {
	if kind != "mysql" && kind != "mariadb" {
		return ""
	}
	var server *driver.MySQLError
	if !errors.As(err, &server) || server == nil || server.Message == "" {
		return ""
	}
	switch server.Number {
	case 1052, // ER_NON_UNIQ_ERROR ambiguous column
		1054,       // ER_BAD_FIELD_ERROR
		1055,       // ER_WRONG_FIELD_WITH_GROUP
		1056,       // ER_WRONG_GROUP_FIELD
		1060,       // ER_DUP_FIELDNAME
		1064,       // ER_PARSE_ERROR
		1066,       // ER_NONUNIQ_TABLE
		1111,       // ER_INVALID_GROUP_FUNC_USE
		1146,       // ER_NO_SUCH_TABLE
		1149,       // ER_SYNTAX_ERROR
		1166,       // ER_WRONG_COLUMN_NAME
		1222,       // ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT
		1241,       // ER_OPERAND_COLUMNS
		1242,       // ER_SUBQUERY_NO_1_ROW
		1247,       // ER_ILLEGAL_REFERENCE
		1248,       // ER_DERIVED_MUST_HAVE_ALIAS
		1292,       // ER_TRUNCATED_WRONG_VALUE
		1305,       // ER_SP_DOES_NOT_EXIST (unknown function)
		1366,       // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD
		1367,       // ER_ILLEGAL_VALUE_FOR_TYPE
		1582, 1583, // ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, ER_WRONG_PARAMETERS_TO_NATIVE_FCT
		1630, // ER_FUNC_INEXISTENT_NAME_COLLISION
		3065: // ER_FIELD_IN_ORDER_NOT_SELECT
	default:
		return ""
	}
	return sqlnative.DetailText(fmt.Sprintf("MySQL error %d: %s", server.Number, server.Message))
}
