package relational

import (
	"context"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func (s *Session) Inspect(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	statement, parameters, err := s.metadataQuery(spec)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	if int64(spec.Limit) > limits.MaxRows {
		return adapter.QueryStats{}, adapter.ErrLimit
	}
	return s.read(ctx, statement, parameters, limits, sink)
}

// Metadata pagination uses a bounded numeric offset; each page is intentionally
// limited by Spec.Limit. Database/schema/table selections are values, never SQL.
func (s *Session) metadataQuery(spec operations.MetadataSpec) (string, []any, error) {
	if s == nil || s.Session == nil || spec.Limit < 1 || spec.Limit > 10000 || len(spec.Cursor) > 7 || spec.Target.Catalog != "" && spec.Target.Catalog != s.database || s.schema != "" && spec.Target.Schema != "" && spec.Target.Schema != s.schema {
		return "", nil, adapter.ErrInvalid
	}
	offset := int64(0)
	if spec.Cursor != "" {
		var err error
		offset, err = strconv.ParseInt(spec.Cursor, 10, 32)
		if err != nil || offset < 0 || offset > 1000000 || strconv.FormatInt(offset, 10) != spec.Cursor {
			return "", nil, adapter.ErrInvalid
		}
	}
	schema := spec.Target.Schema
	if schema == "" {
		schema = s.schema
	}
	name := spec.Target.Name
	var statement string
	var args []any
	if s.Engine == "postgresql" {
		switch spec.Object {
		case "catalogs", "databases":
			if name != "" {
				return "", nil, adapter.ErrUnsupported
			}
			statement = `SELECT datname::text AS catalog FROM pg_catalog.pg_database WHERE datallowconn AND datname=current_database() AND pg_catalog.has_database_privilege(oid,'CONNECT') ORDER BY datname`
		case "schemas":
			if name != "" {
				return "", nil, adapter.ErrInvalid
			}
			statement = `SELECT catalog_name::text AS catalog, schema_name::text AS schema_name FROM information_schema.schemata WHERE (?='' OR schema_name=?) ORDER BY schema_name`
			args = []any{schema, schema}
		case "tables":
			statement = `SELECT table_catalog::text AS catalog, table_schema::text AS schema_name, table_name::text AS name, table_type::text AS type FROM information_schema.tables WHERE (?='' OR table_schema=?) AND (?='' OR table_name=?) ORDER BY table_schema,table_name`
			args = []any{schema, schema, name, name}
		case "columns":
			statement = `SELECT table_schema::text AS schema_name, table_name::text AS table_name, column_name::text AS name, data_type::text AS type, ordinal_position::bigint AS position, is_nullable::text AS nullable, column_default::text AS default_value, numeric_precision::bigint AS numeric_precision, numeric_scale::bigint AS numeric_scale FROM information_schema.columns WHERE (?='' OR table_schema=?) AND (?='' OR table_name=?) ORDER BY table_schema,table_name,ordinal_position`
			args = []any{schema, schema, name, name}
		case "primary_keys":
			statement = `SELECT k.table_schema::text AS schema_name, k.table_name::text AS table_name, k.constraint_name::text AS name, k.column_name::text AS column_name, k.ordinal_position::bigint AS position FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k ON k.constraint_catalog=t.constraint_catalog AND k.constraint_schema=t.constraint_schema AND k.constraint_name=t.constraint_name WHERE t.constraint_type='PRIMARY KEY' AND (?='' OR k.table_schema=?) AND (?='' OR k.table_name=?) ORDER BY k.table_schema,k.table_name,k.constraint_name,k.ordinal_position`
			args = []any{schema, schema, name, name}
		case "foreign_keys", "relationships":
			statement = `SELECT k.table_schema::text AS schema_name, k.table_name::text AS table_name, k.constraint_name::text AS name, k.column_name::text AS column_name, k.ordinal_position::bigint AS position, u.table_schema::text AS referenced_schema, u.table_name::text AS referenced_table, u.column_name::text AS referenced_column FROM information_schema.key_column_usage k JOIN information_schema.referential_constraints r ON r.constraint_catalog=k.constraint_catalog AND r.constraint_schema=k.constraint_schema AND r.constraint_name=k.constraint_name JOIN information_schema.key_column_usage u ON u.constraint_catalog=r.unique_constraint_catalog AND u.constraint_schema=r.unique_constraint_schema AND u.constraint_name=r.unique_constraint_name AND u.ordinal_position=k.position_in_unique_constraint WHERE (?='' OR k.table_schema=?) AND (?='' OR k.table_name=?) ORDER BY k.table_schema,k.table_name,k.constraint_name,k.ordinal_position`
			args = []any{schema, schema, name, name}
		case "functions", "procedures":
			kind := "FUNCTION"
			if spec.Object == "procedures" {
				kind = "PROCEDURE"
			}
			statement = `SELECT routine_catalog::text AS catalog,routine_schema::text AS schema_name,routine_name::text AS name,routine_type::text AS type,data_type::text AS return_type,external_language::text AS language,routine_definition::text AS definition,specific_name::text AS specific_name FROM information_schema.routines WHERE routine_type=? AND (?='' OR routine_schema=?) AND (?='' OR routine_name=?) ORDER BY routine_schema,routine_name,specific_name`
			args = []any{kind, schema, schema, name, name}
		case "indexes":
			statement = `SELECT schemaname::text AS schema_name, tablename::text AS table_name, indexname::text AS name, indexdef::text AS definition FROM pg_catalog.pg_indexes WHERE (?='' OR schemaname=?) AND (?='' OR tablename=?) ORDER BY schemaname,tablename,indexname`
			args = []any{schema, schema, name, name}
		default:
			return "", nil, adapter.ErrUnsupported
		}
	} else if s.Engine == "mysql" || s.Engine == "mariadb" {
		if schema != "" && schema != s.database {
			return "", nil, adapter.ErrInvalid
		}
		switch spec.Object {
		case "catalogs", "databases":
			if name != "" {
				return "", nil, adapter.ErrInvalid
			}
			statement = `SELECT SCHEMA_NAME AS catalog FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=? ORDER BY SCHEMA_NAME`
			args = []any{s.database}
		case "schemas":
			if name != "" {
				return "", nil, adapter.ErrInvalid
			}
			statement = `SELECT SCHEMA_NAME AS catalog, SCHEMA_NAME AS schema_name FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=? ORDER BY SCHEMA_NAME`
			args = []any{s.database}
		case "tables":
			statement = `SELECT TABLE_SCHEMA AS catalog, TABLE_SCHEMA AS schema_name, TABLE_NAME AS name, TABLE_TYPE AS type FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND (?='' OR TABLE_NAME=?) ORDER BY TABLE_NAME`
			args = []any{s.database, name, name}
		case "columns":
			statement = `SELECT TABLE_SCHEMA AS schema_name,TABLE_NAME AS table_name,COLUMN_NAME AS name,COLUMN_TYPE AS type,CAST(ORDINAL_POSITION AS SIGNED) AS position,IS_NULLABLE AS nullable,COLUMN_DEFAULT AS default_value,CAST(NUMERIC_PRECISION AS SIGNED) AS numeric_precision,CAST(NUMERIC_SCALE AS SIGNED) AS numeric_scale FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND (?='' OR TABLE_NAME=?) ORDER BY TABLE_NAME,ORDINAL_POSITION`
			args = []any{s.database, name, name}
		case "primary_keys":
			statement = `SELECT TABLE_SCHEMA AS schema_name,TABLE_NAME AS table_name,CONSTRAINT_NAME AS name,COLUMN_NAME AS column_name,CAST(ORDINAL_POSITION AS SIGNED) AS position FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=? AND CONSTRAINT_NAME='PRIMARY' AND (?='' OR TABLE_NAME=?) ORDER BY TABLE_NAME,ORDINAL_POSITION`
			args = []any{s.database, name, name}
		case "foreign_keys", "relationships":
			statement = `SELECT TABLE_SCHEMA AS schema_name,TABLE_NAME AS table_name,CONSTRAINT_NAME AS name,COLUMN_NAME AS column_name,CAST(ORDINAL_POSITION AS SIGNED) AS position,REFERENCED_TABLE_SCHEMA AS referenced_schema,REFERENCED_TABLE_NAME AS referenced_table,REFERENCED_COLUMN_NAME AS referenced_column FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME IS NOT NULL AND (?='' OR TABLE_NAME=?) ORDER BY TABLE_NAME,CONSTRAINT_NAME,ORDINAL_POSITION`
			args = []any{s.database, name, name}
		case "functions", "procedures":
			kind := "FUNCTION"
			if spec.Object == "procedures" {
				kind = "PROCEDURE"
			}
			statement = `SELECT ROUTINE_SCHEMA AS catalog,ROUTINE_SCHEMA AS schema_name,ROUTINE_NAME AS name,ROUTINE_TYPE AS type,DTD_IDENTIFIER AS return_type,EXTERNAL_LANGUAGE AS language,ROUTINE_DEFINITION AS definition,SPECIFIC_NAME AS specific_name FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA=? AND ROUTINE_TYPE=? AND (?='' OR ROUTINE_NAME=?) ORDER BY ROUTINE_NAME,SPECIFIC_NAME`
			args = []any{s.database, kind, name, name}
		case "indexes":
			statement = `SELECT TABLE_SCHEMA AS schema_name,TABLE_NAME AS table_name,INDEX_NAME AS name,COLUMN_NAME AS column_name,CAST(SEQ_IN_INDEX AS SIGNED) AS position,CAST(NON_UNIQUE AS SIGNED) AS non_unique FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND (?='' OR TABLE_NAME=?) ORDER BY TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX`
			args = []any{s.database, name, name}
		default:
			return "", nil, adapter.ErrUnsupported
		}
	} else if s.Engine == "sqlserver" {
		var err error
		statement, args, err = sqlserverMetadata(spec.Object, schema, name)
		if err != nil {
			return "", nil, err
		}
	} else {
		return "", nil, adapter.ErrUnsupported
	}
	if s.Engine == "sqlserver" {
		statement += " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
		args = append(args, offset, spec.Limit)
	} else {
		statement += " LIMIT ? OFFSET ?"
		args = append(args, spec.Limit, offset)
	}
	if s.Engine == "postgresql" || s.Engine == "sqlserver" {
		var out strings.Builder
		index := 0
		for _, r := range statement {
			if r == '?' {
				index++
				if s.Engine == "sqlserver" {
					out.WriteString("@p")
				} else {
					out.WriteByte('$')
				}
				out.WriteString(strconv.Itoa(index))
			} else {
				out.WriteRune(r)
			}
		}
		statement = out.String()
	}
	return statement, args, nil
}
