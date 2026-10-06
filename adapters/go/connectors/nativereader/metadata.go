// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package nativereader

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cosmosdb"
	"github.com/SYNEHQ/kelvo-go/internal/sources/flightsql"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func (s *Session) Inspect(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	if spec.Limit < 1 || int64(spec.Limit) > limits.MaxRows || (adapter.Query{Statement: "metadata", MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}).Validate() != nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	if s == nil || ctx == nil || sink == nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	if s.spec.Database != "" && spec.Target.Catalog != "" && spec.Target.Catalog != s.spec.Database || s.spec.Schema != "" && spec.Target.Schema != "" && spec.Target.Schema != s.spec.Schema {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	// Protocol discovery must inherit the same saved namespace as generated
	// metadata SQL, even when the public request omits its redundant target.
	if s.spec.Database != "" {
		spec.Target.Catalog = s.spec.Database
	}
	if s.spec.Schema != "" {
		spec.Target.Schema = s.spec.Schema
	}
	switch s.spec.Engine {
	case "elasticsearch":
		return s.inspectElastic(ctx, spec, limits, sink)
	case "dynamodb":
		return s.inspectDynamo(ctx, spec, limits, sink)
	case "arrow_flight":
		l, err := s.limits(ctx, limits.MaxRows, limits.MaxBytes)
		if err != nil {
			return adapter.QueryStats{}, err
		}
		e, err := flightsql.NewResolved(catalog.Config{Sources: []catalog.Source{s.source}}, l, cloudapi.Credentials{URL: s.spec.URL, Token: s.spec.Token, TLS: s.tls})
		if err != nil {
			return adapter.QueryStats{}, err
		}
		defer e.Close()
		stats, err := e.Inspect(ctx, spec, splitSink{next: sink, rows: int64(limits.BatchRows)})
		return adapter.QueryStats{Rows: stats.Rows, Bytes: stats.Bytes, Elapsed: time.Duration(stats.DurationNS)}, err
	case "cosmosdb":
		l, err := s.limits(ctx, limits.MaxRows, limits.MaxBytes)
		if err != nil {
			return adapter.QueryStats{}, err
		}
		e, err := cosmosdb.NewDiscoveryResolved(catalog.Config{Sources: []catalog.Source{s.source}}, l, cloudapi.Credentials{URL: s.spec.URL, Token: s.spec.Token, TLS: s.tls})
		if err != nil {
			return adapter.QueryStats{}, err
		}
		defer e.Close()
		stats, err := e.Inspect(ctx, spec, splitSink{next: sink, rows: int64(limits.BatchRows)})
		return adapter.QueryStats{Rows: stats.Rows, Bytes: stats.Bytes, Elapsed: time.Duration(stats.DurationNS)}, err

	}
	sql, err := s.metadataQuery(spec)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	if int64(spec.Limit) > limits.MaxRows {
		return adapter.QueryStats{}, adapter.ErrLimit
	}
	return s.Query(ctx, adapter.Query{Statement: sql, MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}, &metadataAliasSink{next: sink})
}

// Catalog APIs in some engines uppercase unquoted aliases. Normalize only
// these adapter-owned metadata aliases; ordinary result column names retain
// their source spelling. The Arrow values and field metadata are untouched.
type metadataAliasSink struct {
	next   adapter.Sink
	schema *arrow.Schema
	source *arrow.Schema
}

func (s *metadataAliasSink) Schema(schema *arrow.Schema) error {
	if schema == nil || s.schema != nil {
		return adapter.ErrInvalid
	}
	fields := schema.Fields()
	seen := map[string]bool{}
	for i := range fields {
		name := strings.ToLower(fields[i].Name)
		switch name {
		case "catalog", "schema_name", "table_name", "name", "type", "position", "nullable", "default_value", "numeric_precision", "numeric_scale", "provider_project":
		default:
			return adapter.ErrInvalid
		}
		if seen[name] {
			return adapter.ErrInvalid
		}
		seen[name] = true
		fields[i].Name = name
	}
	meta := schema.Metadata()
	s.source = schema
	s.schema = arrow.NewSchema(fields, &meta)
	return s.next.Schema(s.schema)
}

func (s *metadataAliasSink) Write(batch arrow.RecordBatch) error {
	if s.schema == nil || batch == nil || !batch.Schema().Equal(s.source) || !batch.Schema().Metadata().Equal(s.source.Metadata()) {
		return adapter.ErrInvalid
	}
	out := array.NewRecordBatch(s.schema, batch.Columns(), batch.NumRows())
	defer out.Release()
	return s.next.Write(out)
}
func literal(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
func (s *Session) metadataQuery(spec operations.MetadataSpec) (string, error) {
	if s == nil || spec.ObjectKind != "" || s.spec.Database == "" || spec.Limit < 1 || spec.Limit > 10000 || spec.Target.Catalog != "" && spec.Target.Catalog != s.spec.Database || strings.ContainsAny(spec.Target.Name+spec.Target.Schema, "\x00\r\n\\") {
		return "", adapter.ErrInvalid
	}
	if s.spec.Schema != "" && spec.Target.Schema != "" && spec.Target.Schema != s.spec.Schema {
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
	schema := s.spec.Schema
	if spec.Target.Schema != "" {
		schema = spec.Target.Schema
	}
	db := literal(s.spec.Database)
	filter := ""
	statement := ""
	switch s.spec.Engine {
	case "trino", "presto", "athena", "snowflake":
		filter = "table_catalog=" + db
		if s.spec.Engine == "athena" {
			if schema != "" && schema != s.spec.Database {
				return "", adapter.ErrInvalid
			}
			filter = "table_schema=" + db
		} else if schema != "" {
			filter += " AND table_schema=" + literal(schema)
		}
		if spec.Target.Name != "" {
			filter += " AND table_name=" + literal(spec.Target.Name)
		}
		switch spec.Object {
		case "catalogs", "databases":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT " + db + " AS catalog"
		case "schemas":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT catalog_name AS catalog,schema_name FROM information_schema.schemata WHERE catalog_name=" + db
			if s.spec.Engine == "athena" {
				statement = "SELECT " + db + " AS catalog,schema_name FROM information_schema.schemata WHERE schema_name=" + db
			} else if schema != "" {
				statement += " AND schema_name=" + literal(schema)
			}
			statement += " ORDER BY schema_name"
		case "tables":
			statement = "SELECT table_catalog AS catalog,table_schema AS schema_name,table_name AS name,table_type AS type FROM information_schema.tables WHERE " + filter + " ORDER BY table_schema,table_name"
			if s.spec.Engine == "athena" {
				statement = "SELECT " + db + " AS catalog,table_schema AS schema_name,table_name AS name,table_type AS type FROM information_schema.tables WHERE " + filter + " ORDER BY table_schema,table_name"
			}
		case "columns":
			statement = "SELECT table_schema AS schema_name,table_name,column_name AS name,data_type AS type,ordinal_position AS position,is_nullable AS nullable,column_default AS default_value FROM information_schema.columns WHERE " + filter + " ORDER BY table_schema,table_name,ordinal_position"
		default:
			return "", adapter.ErrUnsupported
		}
	case "bigquery":
		if schema != "" && schema != s.spec.Database {
			return "", adapter.ErrInvalid
		}
		project := s.spec.Options["project"]
		base := "`" + project + "." + s.spec.Database + ".INFORMATION_SCHEMA."
		filter = "table_catalog=" + literal(project) + " AND table_schema=" + db
		if spec.Target.Name != "" {
			filter += " AND table_name=" + literal(spec.Target.Name)
		}
		switch spec.Object {
		case "catalogs", "databases":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT " + db + " AS catalog"
		case "schemas":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT " + db + " AS catalog," + db + " AS schema_name," + literal(project) + " AS provider_project"
		case "tables":
			statement = "SELECT " + db + " AS catalog,table_schema AS schema_name,table_name AS name,table_type AS type,table_catalog AS provider_project FROM " + base + "TABLES` WHERE " + filter + " ORDER BY table_name"
		case "columns":
			statement = "SELECT table_schema AS schema_name,table_name,column_name AS name,data_type AS type,ordinal_position AS position,is_nullable AS nullable,column_default AS default_value FROM " + base + "COLUMNS` WHERE " + filter + " ORDER BY table_name,ordinal_position"
		default:
			return "", adapter.ErrUnsupported
		}
	case "spanner":
		if schema != "" {
			return "", adapter.ErrUnsupported
		}
		filter = "TABLE_SCHEMA=''"
		if spec.Target.Name != "" {
			filter += " AND TABLE_NAME=" + literal(spec.Target.Name)
		}
		switch spec.Object {
		case "catalogs", "databases":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT " + db + " AS catalog"
		case "schemas":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT " + db + " AS catalog,'' AS schema_name"
		case "tables":
			statement = "SELECT " + db + " AS catalog,TABLE_SCHEMA AS schema_name,TABLE_NAME AS name,TABLE_TYPE AS type FROM INFORMATION_SCHEMA.TABLES WHERE " + filter + " ORDER BY TABLE_NAME"
		case "columns":
			statement = "SELECT TABLE_SCHEMA AS schema_name,TABLE_NAME AS table_name,COLUMN_NAME AS name,SPANNER_TYPE AS type,ORDINAL_POSITION AS position,IS_NULLABLE AS nullable FROM INFORMATION_SCHEMA.COLUMNS WHERE " + filter + " ORDER BY TABLE_NAME,ORDINAL_POSITION"
		default:
			return "", adapter.ErrUnsupported
		}
	case "exasol":
		if schema != "" && schema != s.spec.Database {
			return "", adapter.ErrInvalid
		}
		filter = "TABLE_SCHEMA=" + db
		if spec.Target.Name != "" {
			filter += " AND TABLE_NAME=" + literal(spec.Target.Name)
		}
		switch spec.Object {
		case "catalogs", "databases":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT " + db + " AS catalog"
		case "schemas":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT " + db + " AS catalog,SCHEMA_NAME AS schema_name FROM EXA_ALL_SCHEMAS WHERE SCHEMA_NAME=" + db
		case "tables":
			statement = "SELECT " + db + " AS catalog,TABLE_SCHEMA AS schema_name,TABLE_NAME AS name,'TABLE' AS type FROM EXA_ALL_TABLES WHERE " + filter + " ORDER BY TABLE_NAME"
		case "columns":
			filter = "COLUMN_SCHEMA=" + db
			if spec.Target.Name != "" {
				filter += " AND COLUMN_TABLE=" + literal(spec.Target.Name)
			}
			statement = "SELECT COLUMN_SCHEMA AS schema_name,COLUMN_TABLE AS table_name,COLUMN_NAME AS name,COLUMN_TYPE AS type,COLUMN_ORDINAL_POSITION AS position,COLUMN_IS_NULLABLE AS nullable,COLUMN_DEFAULT AS default_value FROM EXA_ALL_COLUMNS WHERE " + filter + " ORDER BY COLUMN_TABLE,COLUMN_ORDINAL_POSITION"
		default:
			return "", adapter.ErrUnsupported
		}
	case "ignite":
		filter = "CACHE_NAME=" + db
		if schema != "" {
			filter += " AND SCHEMA_NAME=" + literal(schema)
		}
		if spec.Target.Name != "" {
			filter += " AND TABLE_NAME=" + literal(spec.Target.Name)
		}
		switch spec.Object {
		case "catalogs", "databases":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT " + db + " AS catalog"
		case "schemas":
			if spec.Target.Name != "" {
				return "", adapter.ErrInvalid
			}
			statement = "SELECT DISTINCT " + db + " AS catalog,SCHEMA_NAME AS schema_name FROM SYS.TABLES WHERE " + filter + " ORDER BY SCHEMA_NAME"
		case "tables":
			statement = "SELECT " + db + " AS catalog,SCHEMA_NAME AS schema_name,TABLE_NAME AS name,'TABLE' AS type FROM SYS.TABLES WHERE " + filter + " ORDER BY SCHEMA_NAME,TABLE_NAME"
		default:
			return "", adapter.ErrUnsupported
		}
	default:
		return "", adapter.ErrUnsupported
	}
	return statement + " LIMIT " + strconv.Itoa(spec.Limit) + " OFFSET " + strconv.FormatInt(offset, 10), nil
}
