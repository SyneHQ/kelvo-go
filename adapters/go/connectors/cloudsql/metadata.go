// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cloudsql

import (
	"context"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func (s *Session) Inspect(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	statement, err := s.metadataQuery(spec)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	if int64(spec.Limit) > limits.MaxRows {
		return adapter.QueryStats{}, adapter.ErrLimit
	}
	return s.Query(ctx, adapter.Query{Statement: statement, MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}, sink)
}

func sqlLiteral(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

func (s *Session) metadataQuery(spec operations.MetadataSpec) (string, error) {
	if s == nil || spec.Limit < 1 || spec.Limit > 10000 || s.database == "" || spec.Target.Catalog != "" && spec.Target.Catalog != s.database || strings.ContainsAny(spec.Target.Name+spec.Target.Schema, "\x00\r\n\\") {
		return "", adapter.ErrInvalid
	}
	offset := int64(0)
	if spec.Cursor != "" {
		var err error
		offset, err = strconv.ParseInt(spec.Cursor, 10, 32)
		if err != nil || offset < 0 || offset > 1000000 || strconv.FormatInt(offset, 10) != spec.Cursor {
			return "", adapter.ErrInvalid
		}
	}
	var statement string
	database := sqlLiteral(s.database)
	if s.source.Type == "d1" {
		if spec.Target.Schema != "" && spec.Target.Schema != "main" {
			return "", adapter.ErrInvalid
		}
		filter := ""
		if spec.Target.Name != "" {
			filter = " AND m.name=" + sqlLiteral(spec.Target.Name)
		}
		switch spec.Object {
		case "catalogs", "databases":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT " + database + " AS catalog"
		case "schemas":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT " + database + " AS catalog, 'main' AS schema_name"
		case "tables":
			statement = "SELECT " + database + " AS catalog,'main' AS schema_name,m.name,CASE m.type WHEN 'view' THEN 'VIEW' ELSE 'BASE TABLE' END AS type FROM sqlite_schema m WHERE m.type IN ('table','view') AND m.name NOT LIKE 'sqlite_%'" + filter + " ORDER BY m.name"
		case "columns":
			statement = "SELECT 'main' AS schema_name,m.name AS table_name,p.name,p.type,p.cid+1 AS position,CASE p.\"notnull\" WHEN 0 THEN 'YES' ELSE 'NO' END AS nullable,p.dflt_value AS default_value,NULL AS numeric_precision,NULL AS numeric_scale FROM sqlite_schema m JOIN pragma_table_info(m.name) p WHERE m.type IN ('table','view') AND m.name NOT LIKE 'sqlite_%'" + filter + " ORDER BY m.name,p.cid"
		default:
			return "", adapter.ErrUnsupported
		}
	} else if s.source.Type == "databricks" {
		if s.schema != "" && spec.Target.Schema != "" && spec.Target.Schema != s.schema {
			return "", adapter.ErrInvalid
		}
		schema := s.schema
		if spec.Target.Schema != "" {
			schema = spec.Target.Schema
		}
		filter := "table_catalog=" + database
		if schema != "" {
			filter += " AND table_schema=" + sqlLiteral(schema)
		}
		if spec.Target.Name != "" {
			filter += " AND table_name=" + sqlLiteral(spec.Target.Name)
		}
		switch spec.Object {
		case "catalogs", "databases":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT catalog_name AS catalog FROM information_schema.catalogs WHERE catalog_name=" + database
		case "schemas":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT catalog_name AS catalog,schema_name FROM information_schema.schemata WHERE catalog_name=" + database
			if schema != "" {
				statement += " AND schema_name=" + sqlLiteral(schema)
			}
			statement += " ORDER BY schema_name"
		case "tables":
			statement = "SELECT table_catalog AS catalog,table_schema AS schema_name,table_name AS name,table_type AS type FROM information_schema.tables WHERE " + filter + " ORDER BY table_schema,table_name"
		case "columns":
			statement = "SELECT table_schema AS schema_name,table_name,column_name AS name,full_data_type AS type,ordinal_position AS position,is_nullable AS nullable,column_default AS default_value,numeric_precision,numeric_scale FROM information_schema.columns WHERE " + filter + " ORDER BY table_schema,table_name,ordinal_position"
		default:
			return "", adapter.ErrUnsupported
		}
	} else {
		return "", adapter.ErrUnsupported
	}
	return statement + " LIMIT " + strconv.Itoa(spec.Limit) + " OFFSET " + strconv.FormatInt(offset, 10), nil
}
