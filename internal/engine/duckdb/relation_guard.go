//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// deniedFunctions are file, network, SQL-string, extension, secret, setting,
// profiling, logging and catalog-definition entry points. They are matched
// against the parsed statement's function and table-function names, not raw
// text, so a column or alias that merely shares a name is never affected.
var deniedFunctions = map[string]struct{}{
	"glob": {}, "query": {}, "query_table": {}, "load_extension": {}, "getenv": {},
	"sniff_csv": {}, "csv_scan": {}, "json_scan": {}, "parquet_scan": {}, "arrow_scan": {}, "arrow_scan_dumb": {},
	"duckdb_secrets": {}, "which_secret": {}, "duckdb_settings": {}, "current_setting": {}, "set_config": {},
	"duckdb_views": {}, "duckdb_tables": {}, "duckdb_functions": {}, "duckdb_databases": {}, "duckdb_extensions": {},
	"duckdb_temporary_files": {}, "duckdb_logs": {}, "duckdb_logs_parsed": {}, "duckdb_log_contexts": {}, "write_log": {},
	"duckdb_prepared_statements": {}, "duckdb_dependencies": {}, "duckdb_optimizers": {}, "duckdb_memory": {},
	"duckdb_external_file_cache": {}, "duckdb_approx_database_count": {},
	"checkpoint": {}, "force_checkpoint": {}, "json_execute_serialized_sql": {},
	"enable_logging": {}, "disable_logging": {}, "enable_profiling": {}, "disable_profiling": {},
	"enable_peg_parser": {}, "disable_peg_parser": {}, "truncate_duckdb_logs": {}, "pragma_table_info": {},
	"iceberg_scan": {}, "iceberg_metadata": {}, "iceberg_snapshots": {}, "delta_scan": {}, "st_read": {}, "st_read_meta": {},
	"http_get": {}, "http_post": {}, "http_head": {},
}

// deniedPrefixes cover whole families: readers, database scanners, parquet
// metadata, HTTP helpers, pragma table functions and the private Arrow scan.
var deniedPrefixes = []string{
	"read_", "postgres_", "mysql_", "sqlite_", "parquet_", "http_", "pragma_", "iceberg_", "delta_",
	"ducklake_", "odbc_", "st_read", "kelvo_arrow_scan_", "excel", "xlsx",
}

// deniedRelations are catalog relations referenced as base tables (not table
// functions) that reveal a source's SQL, file path or presigned URL. The
// function denylist covers their table-function forms; these cover the views.
var deniedRelations = map[string]struct{}{
	"pg_views": {}, "duckdb_views": {}, "duckdb_tables": {}, "duckdb_functions": {}, "duckdb_macros": {},
	"duckdb_secrets": {}, "duckdb_settings": {}, "duckdb_databases": {}, "duckdb_logs": {}, "duckdb_optimizers": {},
	"pg_proc": {}, "sqlite_master": {}, "sqlite_schema": {}, "pg_settings": {},
}

func deniedFunction(name string) bool {
	name = strings.ToLower(name)
	if _, found := deniedFunctions[name]; found {
		return true
	}
	for _, prefix := range deniedPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// deniedCallRE catches a denied name immediately followed by `(`, allowing
// block comments between. Defense in depth beside the parsed-plan check, which
// is authoritative; this needs no lexer agreement because a false positive
// only denies, never allows.
var deniedCallRE = func() *regexp.Regexp {
	names := make([]string, 0, len(deniedFunctions))
	for n := range deniedFunctions {
		names = append(names, regexp.QuoteMeta(n))
	}
	names = append(names, `read_[a-z0-9_]*`, `postgres_[a-z0-9_]*`, `mysql_[a-z0-9_]*`, `sqlite_[a-z0-9_]*`,
		`parquet_[a-z0-9_]*`, `pragma_[a-z0-9_]*`, `kelvo_arrow_scan_[a-z0-9_]*`, `iceberg_[a-z0-9_]*`, `delta_[a-z0-9_]*`)
	return regexp.MustCompile(`(?is)\b(?:` + strings.Join(names, "|") + `)\s*(?:/\*.*?\*/\s*)*\(`)
}()

// preflightDenied runs before preparation. It blocks EXPORT (which creates its
// directory while preparing) and PRAGMA (classified as SELECT), which the
// parsed-plan check below cannot cover, plus any denied name written as a call.
func preflightDenied(sqlText string) bool {
	lowered := strings.ToLower(sqlText)
	for _, token := range strings.FieldsFunc(lowered, func(r rune) bool { return !(r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9') }) {
		if token == "pragma" || token == "export" {
			return true
		}
	}
	return deniedCallRE.MatchString(sqlText)
}

// enforceRelationAllowlist validates the parsed statement tree, not the text.
// Every table reference must be a plain identifier that is not a denied
// catalog relation (so a file path, glob or URL used as a table is rejected),
// and every function/table-function name must not be denied. It fails closed
// if the tree cannot be obtained. It runs after the single-SELECT check, on a
// connection already locked down, and json_serialize_sql only parses: it never
// opens a file or binds a relation.
func enforceRelationAllowlist(ctx context.Context, raw any, sqlText string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q, ok := raw.(driver.QueryerContext)
	if !ok {
		return errors.New("DuckDB parse introspection is unavailable")
	}
	tree, err := queryOneString(ctx, q, "SELECT CAST(json_serialize_sql('"+quoteLiteral(sqlText)+"') AS VARCHAR)")
	if err != nil {
		return query.NewError("PERMISSION_DENIED", "SQL could not be validated for read-only access")
	}
	var parsed struct {
		Error any             `json:"error"`
		Stmts json.RawMessage `json:"statements"`
	}
	if json.Unmarshal([]byte(tree), &parsed) != nil {
		return query.NewError("PERMISSION_DENIED", "SQL could not be validated for read-only access")
	}
	if b, ok := parsed.Error.(bool); !ok || b {
		return query.NewError("PERMISSION_DENIED", "SQL could not be validated for read-only access")
	}
	var node any
	if json.Unmarshal(parsed.Stmts, &node) != nil {
		return query.NewError("PERMISSION_DENIED", "SQL could not be validated for read-only access")
	}
	return walkPlan(node)
}

func walkPlan(v any) error {
	switch t := v.(type) {
	case map[string]any:
		if kind, _ := t["type"].(string); kind == "BASE_TABLE" {
			table, _ := t["table_name"].(string)
			schema, _ := t["schema_name"].(string)
			if err := checkBaseTable(schema, table); err != nil {
				return err
			}
		}
		if name, ok := t["function_name"].(string); ok && deniedFunction(name) {
			return query.NewError("PERMISSION_DENIED", "Query uses a capability unavailable to this execution")
		}
		for _, child := range t {
			if err := walkPlan(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range t {
			if err := walkPlan(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkBaseTable(schema, table string) error {
	// A registered source view, CTE or catalog relation is always a plain
	// identifier. A file path, glob or presigned URL used as a table name is
	// not, so reject it outright: it would read the file directly, bypassing
	// the registered relation and its row/column policy.
	if !catalog.ValidID(table) {
		return query.NewError("PERMISSION_DENIED", "Query references a relation unavailable to this execution")
	}
	low := strings.ToLower(table)
	if _, bad := deniedRelations[low]; bad {
		return query.NewError("PERMISSION_DENIED", "Query references a relation unavailable to this execution")
	}
	if strings.ToLower(schema) == "information_schema" && low == "views" {
		return query.NewError("PERMISSION_DENIED", "Query references a relation unavailable to this execution")
	}
	return nil
}

func queryOneString(ctx context.Context, q driver.QueryerContext, sql string) (string, error) {
	rows, err := q.QueryContext(ctx, sql, nil)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	dest := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(dest); err != nil {
		return "", err
	}
	switch v := dest[0].(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", io.ErrUnexpectedEOF
	}
}
