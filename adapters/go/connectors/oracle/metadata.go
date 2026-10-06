// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package oracle

import (
	"context"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

func (s *Session) Inspect(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	if spec.Object == "objects" {
		return s.inspectObjects(ctx, spec, limits, sink)
	}
	if spec.ObjectKind != "" {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	statement, parameters, numbers, err := s.metadataQuery(spec)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	if int64(spec.Limit) > limits.MaxRows {
		return adapter.QueryStats{}, adapter.ErrLimit
	}
	converted := &metadataSink{next: sink, numbers: numbers, maxBytes: limits.MaxBytes}
	stats, err := s.read(ctx, statement, parameters, limits, converted)
	stats.Bytes = converted.bytes
	return stats, err
}

func (s *Session) metadataQuery(spec operations.MetadataSpec) (string, []any, map[string]bool, error) {
	if s == nil || s.Session == nil || spec.Limit < 1 || spec.Limit > 10000 || spec.Target.Catalog != "" && spec.Target.Catalog != s.database || s.schema != "" && spec.Target.Schema != "" && spec.Target.Schema != s.schema || len(spec.Target.Schema) > 128 || len(spec.Target.Name) > 128 || !utf8.ValidString(spec.Target.Schema+spec.Target.Name) || strings.ContainsAny(spec.Target.Schema+spec.Target.Name, "\x00\r\n") {
		return "", nil, nil, adapter.ErrInvalid
	}
	offset := int64(0)
	if spec.Cursor != "" {
		var err error
		offset, err = strconv.ParseInt(spec.Cursor, 10, 32)
		if err != nil || offset < 0 || offset > 1000000 || strconv.FormatInt(offset, 10) != spec.Cursor {
			return "", nil, nil, adapter.ErrInvalid
		}
	}
	schema := spec.Target.Schema
	if schema == "" {
		schema = s.schema
	}
	name := spec.Target.Name
	var statement string
	var args []any
	numbers := map[string]bool{}
	switch spec.Object {
	case "catalogs", "databases":
		if name != "" {
			return "", nil, nil, adapter.ErrInvalid
		}
		statement = `SELECT CAST(? AS VARCHAR2(256)) AS "catalog" FROM dual ORDER BY 1`
		args = []any{s.database}
	case "schemas":
		if name != "" {
			return "", nil, nil, adapter.ErrInvalid
		}
		statement = `SELECT DISTINCT CAST(? AS VARCHAR2(256)) AS "catalog",owner AS "schema_name" FROM all_objects WHERE (? IS NULL OR owner=?) ORDER BY owner`
		args = []any{s.database, schema, schema}
	case "tables":
		statement = `SELECT CAST(? AS VARCHAR2(256)) AS "catalog",owner AS "schema_name",object_name AS "name",CASE object_type WHEN 'TABLE' THEN 'BASE TABLE' ELSE 'VIEW' END AS "type" FROM all_objects WHERE object_type IN ('TABLE','VIEW') AND (? IS NULL OR owner=?) AND (? IS NULL OR object_name=?) ORDER BY owner,object_name`
		args = []any{s.database, schema, schema, name, name}
	case "columns":
		// Textual integer metadata is explicitly converted to Arrow int64 below;
		// Oracle NUMBER metadata must not accidentally become JSON decimal strings.
		statement = `SELECT owner AS "schema_name",table_name AS "table_name",column_name AS "name",data_type AS "type",TO_CHAR(column_id,'FM9999999990') AS "position",CASE nullable WHEN 'Y' THEN 'YES' ELSE 'NO' END AS "nullable",data_default AS "default_value",TO_CHAR(data_precision,'FM9999999990') AS "numeric_precision",TO_CHAR(data_scale,'FM9999999990') AS "numeric_scale" FROM all_tab_columns WHERE (? IS NULL OR owner=?) AND (? IS NULL OR table_name=?) ORDER BY owner,table_name,column_id`
		args = []any{schema, schema, name, name}
		numbers = map[string]bool{"position": true, "numeric_precision": true, "numeric_scale": true}
	case "primary_keys":
		statement = `SELECT c.owner AS "schema_name",c.table_name AS "table_name",c.constraint_name AS "name",k.column_name AS "column_name",TO_CHAR(k.position,'FM9999999990') AS "position" FROM all_constraints c JOIN all_cons_columns k ON k.owner=c.owner AND k.constraint_name=c.constraint_name WHERE c.constraint_type='P' AND (? IS NULL OR c.owner=?) AND (? IS NULL OR c.table_name=?) ORDER BY c.owner,c.table_name,c.constraint_name,k.position`
		args = []any{schema, schema, name, name}
		numbers["position"] = true
	case "foreign_keys", "relationships":
		statement = `SELECT c.owner AS "schema_name",c.table_name AS "table_name",c.constraint_name AS "name",k.column_name AS "column_name",TO_CHAR(k.position,'FM9999999990') AS "position",u.owner AS "referenced_schema",u.table_name AS "referenced_table",u.column_name AS "referenced_column" FROM all_constraints c JOIN all_cons_columns k ON k.owner=c.owner AND k.constraint_name=c.constraint_name JOIN all_cons_columns u ON u.owner=c.r_owner AND u.constraint_name=c.r_constraint_name AND u.position=k.position WHERE c.constraint_type='R' AND (? IS NULL OR c.owner=?) AND (? IS NULL OR c.table_name=?) ORDER BY c.owner,c.table_name,c.constraint_name,k.position`
		args = []any{schema, schema, name, name}
		numbers["position"] = true
	case "indexes":
		statement = `SELECT i.table_owner AS "schema_name",i.table_name AS "table_name",i.index_name AS "name",k.column_name AS "column_name",TO_CHAR(k.column_position,'FM9999999990') AS "position",CASE i.uniqueness WHEN 'UNIQUE' THEN '0' ELSE '1' END AS "non_unique" FROM all_indexes i JOIN all_ind_columns k ON k.index_owner=i.owner AND k.index_name=i.index_name WHERE (? IS NULL OR i.table_owner=?) AND (? IS NULL OR i.table_name=?) ORDER BY i.table_owner,i.table_name,i.index_name,k.column_position`
		args = []any{schema, schema, name, name}
		numbers["position"], numbers["non_unique"] = true, true
	default:
		return "", nil, nil, adapter.ErrUnsupported
	}
	statement += " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
	args = append(args, offset, spec.Limit)
	var out strings.Builder
	index := 0
	for _, r := range statement {
		if r == '?' {
			index++
			out.WriteByte(':')
			out.WriteString(strconv.Itoa(index))
		} else {
			out.WriteRune(r)
		}
	}
	return out.String(), args, numbers, nil
}

type metadataSink struct {
	next            adapter.Sink
	numbers         map[string]bool
	schema          *arrow.Schema
	input           *arrow.Schema
	bytes, maxBytes int64
}

func (s *metadataSink) Schema(schema *arrow.Schema) error {
	if s.next == nil || schema == nil || s.schema != nil {
		return adapter.ErrInvalid
	}
	fields := schema.Fields()
	found := 0
	seen := make(map[string]bool, len(fields))
	for i, field := range fields {
		if seen[field.Name] {
			return adapter.ErrInvalid
		}
		seen[field.Name] = true
		if s.numbers[field.Name] {
			if field.Type.ID() != arrow.STRING {
				return adapter.ErrInvalid
			}
			fields[i].Type = arrow.PrimitiveTypes.Int64
			found++
		}
	}
	if found != len(s.numbers) {
		return adapter.ErrInvalid
	}
	meta := schema.Metadata()
	s.input = schema
	s.schema = arrow.NewSchema(fields, &meta)
	return s.next.Schema(s.schema)
}
func (s *metadataSink) Write(record arrow.RecordBatch) error {
	if s.schema == nil || record == nil || !record.Schema().Equal(s.input) {
		return adapter.ErrInvalid
	}
	values := make([]arrow.Array, record.NumCols())
	defer func() {
		for _, value := range values {
			if value != nil {
				value.Release()
			}
		}
	}()
	for i, field := range record.Schema().Fields() {
		if !s.numbers[field.Name] {
			values[i] = record.Column(i)
			values[i].Retain()
			continue
		}
		text, ok := record.Column(i).(*array.String)
		if !ok {
			return adapter.ErrInvalid
		}
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		for row := 0; row < text.Len(); row++ {
			if text.IsNull(row) {
				builder.AppendNull()
				continue
			}
			n, err := strconv.ParseInt(text.Value(row), 10, 64)
			if err != nil {
				builder.Release()
				return adapter.ErrInvalid
			}
			builder.Append(n)
		}
		values[i] = builder.NewArray()
		builder.Release()
	}
	converted := array.NewRecordBatch(s.schema, values, record.NumRows())
	defer converted.Release()
	bytes := arrowutil.TotalRecordSize(converted)
	if bytes > s.maxBytes-s.bytes {
		return adapter.ErrLimit
	}
	if err := s.next.Write(converted); err != nil {
		return err
	}
	s.bytes += bytes
	return nil
}
