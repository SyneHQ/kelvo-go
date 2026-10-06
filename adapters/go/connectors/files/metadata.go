package files

import (
	"encoding/json"
	"strconv"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func metadataQuery(spec operations.MetadataSpec, name, format string) (string, []operations.Parameter, error) {
	if spec.Limit < 1 || spec.Limit > 10000 || spec.Target.Catalog != "" && spec.Target.Catalog != name {
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
	var statement string
	var values []any
	schema, table := spec.Target.Schema, spec.Target.Name
	filter := "table_catalog=current_database()"
	if format != "duckdb" {
		filter += " AND table_name=?"
		values = append(values, name)
	}
	switch spec.Object {
	case "catalogs", "databases":
		if table != "" || schema != "" {
			return "", nil, adapter.ErrInvalid
		}
		statement = "SELECT ?::VARCHAR AS catalog"
		values = []any{name}
	case "schemas":
		if table != "" {
			return "", nil, adapter.ErrInvalid
		}
		statement = "SELECT ?::VARCHAR AS catalog,schema_name FROM information_schema.schemata WHERE catalog_name=current_database() AND (?='' OR schema_name=?) ORDER BY schema_name"
		values = []any{name, schema, schema}
	case "tables":
		statement = "SELECT ?::VARCHAR AS catalog,table_schema AS schema_name,table_name AS name,table_type AS type FROM information_schema.tables WHERE " + filter + " AND (?='' OR table_schema=?) AND (?='' OR table_name=?) ORDER BY table_schema,table_name"
		values = append([]any{name}, values...)
		values = append(values, schema, schema, table, table)
	case "columns":
		statement = "SELECT table_schema AS schema_name,table_name,column_name AS name,data_type AS type,ordinal_position::BIGINT AS position,is_nullable AS nullable,column_default AS default_value,numeric_precision::BIGINT AS numeric_precision,numeric_scale::BIGINT AS numeric_scale FROM information_schema.columns WHERE " + filter + " AND (?='' OR table_schema=?) AND (?='' OR table_name=?) ORDER BY table_schema,table_name,ordinal_position"
		values = append(values, schema, schema, table, table)
	default:
		return "", nil, adapter.ErrUnsupported
	}
	statement += " LIMIT ? OFFSET ?"
	values = append(values, spec.Limit, offset)
	params := make([]operations.Parameter, len(values))
	for i, value := range values {
		kind := "int64"
		if _, ok := value.(string); ok {
			kind = "string"
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return "", nil, adapter.ErrInvalid
		}
		params[i] = operations.Parameter{Type: kind, Value: raw}
	}
	return statement, params, nil
}
