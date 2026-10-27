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
	"http_get": {}, "http_post": {}, "http_head": {}, "duckdb_table_sample": {},
}

// deniedPrefixes cover whole families: readers, database scanners, parquet
// metadata, HTTP helpers, pragma table functions and the private Arrow scan.
var deniedPrefixes = []string{
	"read_", "postgres_", "mysql_", "sqlite_", "parquet_", "http_", "pragma_", "iceberg_", "delta_",
	"ducklake_", "odbc_", "st_read", "kelvo_arrow_scan_", "excel", "xlsx",
	// pg_get_viewdef and siblings return object definitions, which embed
	// source paths and presigned URLs.
	"pg_get_",
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
		`parquet_[a-z0-9_]*`, `pragma_[a-z0-9_]*`, `pg_get_[a-z0-9_]*`, `kelvo_arrow_scan_[a-z0-9_]*`, `iceberg_[a-z0-9_]*`, `delta_[a-z0-9_]*`)
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
// Every table reference that is not a CTE in scope must resolve to a relation
// that already exists in this connection's catalog, using the same lookup
// locations DuckDB's binder uses. A name that does not resolve would fall back
// to a replacement scan, which reads it as a file path, so it is denied
// regardless of quoting. Denied catalog relations and denied functions are
// rejected on top. The catalog is read by engine-authored SQL before the
// user's statement runs; json_serialize_sql only parses. Fails closed.
func enforceRelationAllowlist(ctx context.Context, raw any, sqlText string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q, ok := raw.(driver.QueryerContext)
	if !ok {
		return errors.New("DuckDB parse introspection is unavailable")
	}
	unvalidated := query.NewError("PERMISSION_DENIED", "SQL could not be validated for read-only access")
	index, err := loadRelationIndex(ctx, q)
	if err != nil {
		return unvalidated
	}
	tree, err := queryOneString(ctx, q, "SELECT CAST(json_serialize_sql('"+quoteLiteral(sqlText)+"') AS VARCHAR)")
	if err != nil {
		return unvalidated
	}
	var parsed struct {
		Error any             `json:"error"`
		Stmts json.RawMessage `json:"statements"`
	}
	if json.Unmarshal([]byte(tree), &parsed) != nil {
		return unvalidated
	}
	if b, ok := parsed.Error.(bool); !ok || b {
		return unvalidated
	}
	var node any
	if json.Unmarshal(parsed.Stmts, &node) != nil {
		return unvalidated
	}
	return walkPlan(node, index, nil)
}

// relationIndex is the connection's existing relations plus DuckDB's lookup
// locations. Lookups are case-insensitive, as DuckDB's are by default.
type relationIndex struct {
	relations     map[[3]string]struct{} // database, schema, name
	defaultSchema map[string]string      // database -> schema used for "db.name"
	search        [][2]string            // locations for unqualified names
	current       string
}

func loadRelationIndex(ctx context.Context, q driver.QueryerContext) (*relationIndex, error) {
	index := &relationIndex{relations: map[[3]string]struct{}{}, defaultSchema: map[string]string{}}
	rows, err := queryStrings(ctx, q, "SELECT database_name, schema_name, table_name FROM duckdb_tables() UNION ALL SELECT database_name, schema_name, view_name FROM duckdb_views()", 3)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		index.relations[[3]string{strings.ToLower(r[0]), strings.ToLower(r[1]), strings.ToLower(r[2])}] = struct{}{}
	}
	databases, err := queryStrings(ctx, q, "SELECT database_name, type FROM duckdb_databases()", 2)
	if err != nil {
		return nil, err
	}
	for _, d := range databases {
		switch strings.ToLower(d[1]) {
		case "duckdb", "sqlite":
			index.defaultSchema[strings.ToLower(d[0])] = "main"
		case "postgres", "postgres_scanner":
			index.defaultSchema[strings.ToLower(d[0])] = "public"
		}
	}
	current, err := queryStrings(ctx, q, "SELECT current_database(), current_schema()", 2)
	if err != nil || len(current) != 1 {
		return nil, errors.New("current catalog is unavailable")
	}
	db, schema := strings.ToLower(current[0][0]), strings.ToLower(current[0][1])
	index.current = db
	// A subset of DuckDB's default search path: a name found here binds to an
	// existing relation and never reaches a replacement scan.
	index.search = [][2]string{{"temp", "main"}, {db, schema}, {"system", "main"}, {"system", "pg_catalog"}}
	return index, nil
}

func (x *relationIndex) has(db, schema, name string) bool {
	_, ok := x.relations[[3]string{db, schema, name}]
	return ok
}

// resolves mirrors the binder: db.schema.name exactly; "a.name" as catalog a
// with its default schema, or as schema a in the current, temp or system
// catalog; an unqualified name along the search path.
func (x *relationIndex) resolves(catalogName, schema, name string) bool {
	c, s, n := strings.ToLower(catalogName), strings.ToLower(schema), strings.ToLower(name)
	switch {
	case n == "":
		return false
	case c != "":
		return x.has(c, s, n)
	case s != "":
		if def, ok := x.defaultSchema[s]; ok && x.has(s, def, n) {
			return true
		}
		for _, db := range []string{x.current, "temp", "system"} {
			if x.has(db, s, n) {
				return true
			}
		}
		return false
	}
	for _, loc := range x.search {
		if x.has(loc[0], loc[1], n) {
			return true
		}
	}
	return false
}

func walkPlan(v any, index *relationIndex, ctes map[string]struct{}) error {
	switch t := v.(type) {
	case map[string]any:
		// CTEs are visible to later CTEs of the same WITH, to themselves
		// (recursive), and to the query body, but not outside this node.
		if cm, ok := t["cte_map"].(map[string]any); ok {
			scope := make(map[string]struct{}, len(ctes)+4)
			for k := range ctes {
				scope[k] = struct{}{}
			}
			entries, _ := cm["map"].([]any)
			for _, e := range entries {
				entry, _ := e.(map[string]any)
				key, _ := entry["key"].(string)
				scope[strings.ToLower(key)] = struct{}{}
				if err := walkPlan(entry["value"], index, scope); err != nil {
					return err
				}
			}
			ctes = scope
		}
		if kind, _ := t["type"].(string); kind == "BASE_TABLE" {
			table, _ := t["table_name"].(string)
			schema, _ := t["schema_name"].(string)
			catalogName, _ := t["catalog_name"].(string)
			if err := checkBaseTable(index, ctes, catalogName, schema, table); err != nil {
				return err
			}
		}
		if name, ok := t["function_name"].(string); ok && deniedFunction(name) {
			return query.NewError("PERMISSION_DENIED", "Query uses a capability unavailable to this execution")
		}
		for key, child := range t {
			if key == "cte_map" {
				continue
			}
			if err := walkPlan(child, index, ctes); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range t {
			if err := walkPlan(child, index, ctes); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkBaseTable(index *relationIndex, ctes map[string]struct{}, catalogName, schema, table string) error {
	denied := query.NewError("PERMISSION_DENIED", "Query references a relation unavailable to this execution")
	low := strings.ToLower(table)
	if _, bad := deniedRelations[low]; bad {
		return denied
	}
	if strings.ToLower(schema) == "information_schema" && low == "views" {
		return denied
	}
	if catalogName == "" && schema == "" {
		if _, cte := ctes[low]; cte {
			return nil
		}
	}
	if !index.resolves(catalogName, schema, table) {
		return denied
	}
	return nil
}

func queryStrings(ctx context.Context, q driver.QueryerContext, sql string, columns int) ([][]string, error) {
	rows, err := q.QueryContext(ctx, sql, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]string
	dest := make([]driver.Value, len(rows.Columns()))
	if len(dest) != columns {
		return nil, errors.New("unexpected catalog shape")
	}
	for {
		if err := rows.Next(dest); err == io.EOF {
			return out, nil
		} else if err != nil {
			return nil, err
		}
		row := make([]string, columns)
		for i, v := range dest {
			switch tv := v.(type) {
			case string:
				row[i] = tv
			case []byte:
				row[i] = string(tv)
			case nil:
			default:
				return nil, errors.New("unexpected catalog value")
			}
		}
		out = append(out, row)
	}
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
