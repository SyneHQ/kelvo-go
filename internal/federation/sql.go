// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

const countColumn = "__kelvo_count"

// ClickHouse quoted identifiers use backslash escapes. Operators select plain
// database/table identifiers; source-provided column names are quoted separately.
func quoteIdentifier(name string) (string, error) {
	if name == "" || len(name) > 1024 || !utf8.ValidString(name) {
		return "", query.NewError("UNSUPPORTED", "Federation column identifier is unsupported")
	}
	for _, c := range name {
		if c < 32 || c == 127 {
			return "", query.NewError("UNSUPPORTED", "Federation column identifier is unsupported")
		}
	}
	name = strings.ReplaceAll(name, `\`, `\\`)
	name = strings.ReplaceAll(name, "`", "\\`")
	return "`" + name + "`", nil
}

func (t *Table) compileScan(plan duckbridge.ScanPlan) (string, *arrow.Schema, error) {
	if len(plan.Columns) > 1024 || len(plan.Filters) > 256 {
		return "", nil, query.NewError("UNSUPPORTED", "Federation scan plan exceeds supported complexity")
	}
	columns := make([]string, len(plan.Columns))
	fields := make([]arrow.Field, len(plan.Columns))
	for i, name := range plan.Columns {
		field, found := t.columns[name]
		if !found {
			return "", nil, query.NewError("PERMISSION_DENIED", "Federation projection names an unavailable column")
		}
		quoted, err := quoteIdentifier(name)
		if err != nil {
			return "", nil, err
		}
		columns[i], fields[i] = quoted, field
	}
	if len(columns) == 0 {
		columns = []string{"toUInt8(1) AS `" + countColumn + "`"}
		fields = []arrow.Field{{Name: countColumn, Type: arrow.PrimitiveTypes.Uint8}}
	}
	compiler := predicateCompiler{columns: t.columns}
	filters := make([]string, len(plan.Filters))
	for i, filter := range plan.Filters {
		compiled, err := compiler.compile(filter, 0)
		if err != nil {
			return "", nil, err
		}
		filters[i] = compiled
	}
	sql := "SELECT " + strings.Join(columns, ", ") + " FROM " + t.remoteName
	if len(filters) != 0 {
		sql += " WHERE " + strings.Join(filters, " AND ")
	}
	metadata := t.schema.Metadata()
	return sql, arrow.NewSchema(fields, &metadata), nil
}

type predicateCompiler struct {
	columns map[string]arrow.Field
	nodes   int
}

func (p *predicateCompiler) compile(filter duckbridge.Filter, depth int) (string, error) {
	p.nodes++
	unsupported := query.NewError("UNSUPPORTED", "Required federation predicate is unsupported")
	if depth > 32 || p.nodes > 1024 {
		return "", unsupported
	}
	if filter.Kind == "and" || filter.Kind == "or" {
		if len(filter.Children) == 0 || len(filter.Children) > 256 || filter.Column != "" || filter.Op != "" || filter.Type != "" || filter.Value != "" {
			return "", unsupported
		}
		children := make([]string, len(filter.Children))
		for i, child := range filter.Children {
			compiled, err := p.compile(child, depth+1)
			if err != nil {
				return "", err
			}
			children[i] = compiled
		}
		return "(" + strings.Join(children, " "+strings.ToUpper(filter.Kind)+" ") + ")", nil
	}
	field, found := p.columns[filter.Column]
	if !found || len(filter.Children) != 0 {
		return "", unsupported
	}
	column, err := quoteIdentifier(filter.Column)
	if err != nil {
		return "", err
	}
	if filter.Kind == "is_null" || filter.Kind == "is_not_null" {
		if filter.Op != "" || filter.Type != "" || filter.Value != "" {
			return "", unsupported
		}
		if filter.Kind == "is_null" {
			return "(" + column + " IS NULL)", nil
		}
		return "(" + column + " IS NOT NULL)", nil
	}
	if filter.Kind != "comparison" {
		return "", unsupported
	}
	operator, found := map[string]string{"eq": "=", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}[filter.Op]
	if !found {
		return "", unsupported
	}
	constant, err := exactConstant(filter.Type, filter.Value, field.Type)
	if err != nil {
		return "", err
	}
	return "(" + column + " " + operator + " " + constant + ")", nil
}
func exactConstant(kind, value string, column arrow.DataType) (string, error) {
	unsupported := query.NewError("UNSUPPORTED", "Federation predicates require exact matching integer or boolean types")
	if kind == "bool" {
		if column.ID() != arrow.BOOL || (value != "true" && value != "false") {
			return "", unsupported
		}
		return value, nil
	}
	types := map[string]struct {
		id     arrow.Type
		bits   int
		signed bool
		sql    string
	}{
		"int8": {arrow.INT8, 8, true, "Int8"}, "int16": {arrow.INT16, 16, true, "Int16"}, "int32": {arrow.INT32, 32, true, "Int32"}, "int64": {arrow.INT64, 64, true, "Int64"},
		"uint8": {arrow.UINT8, 8, false, "UInt8"}, "uint16": {arrow.UINT16, 16, false, "UInt16"}, "uint32": {arrow.UINT32, 32, false, "UInt32"}, "uint64": {arrow.UINT64, 64, false, "UInt64"},
	}
	typ, found := types[kind]
	if !found || column.ID() != typ.id || len(value) > 21 {
		return "", unsupported
	}
	if typ.signed {
		parsed, err := strconv.ParseInt(value, 10, typ.bits)
		if err != nil || strconv.FormatInt(parsed, 10) != value {
			return "", unsupported
		}
	} else {
		parsed, err := strconv.ParseUint(value, 10, typ.bits)
		if err != nil || strconv.FormatUint(parsed, 10) != value {
			return "", unsupported
		}
	}
	// Validated decimal strings and typed CAST avoid any floating-literal path,
	// including UInt64 max and Int64 min.
	return "CAST('" + value + "' AS " + typ.sql + ")", nil
}
