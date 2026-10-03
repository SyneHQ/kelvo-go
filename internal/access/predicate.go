// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package access

import (
	"cmp"
	"strconv"
	"strings"
	"unicode/utf8"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type boundPredicate struct {
	kind, op string
	index    int
	signed   int64
	unsigned uint64
	text     string
	boolean  bool
	children []*boundPredicate
}

func fromPushed(f federationapi.Filter, depth int, nodes *int) (Predicate, error) {
	*nodes++
	if depth > 32 || *nodes > MaxPredicateNodes || len(f.Children) > 256 {
		return Predicate{}, unsupported()
	}
	p := Predicate{Kind: f.Kind, Column: f.Column, Op: f.Op, Type: f.Type, Value: f.Value}
	for _, child := range f.Children {
		converted, err := fromPushed(child, depth+1, nodes)
		if err != nil {
			return Predicate{}, err
		}
		p.Children = append(p.Children, converted)
	}
	return p, nil
}

func predicateColumns(p Predicate, add func(string)) {
	if p.Column != "" {
		add(p.Column)
	}
	for _, child := range p.Children {
		predicateColumns(child, add)
	}
}

func bindPredicate(p Predicate, schema *arrow.Schema) (*boundPredicate, error) {
	b := &boundPredicate{kind: p.Kind, op: p.Op, index: -1, text: p.Value, boolean: p.Value == "true"}
	if p.Kind == "and" || p.Kind == "or" {
		for _, child := range p.Children {
			item, err := bindPredicate(child, schema)
			if err != nil {
				return nil, err
			}
			b.children = append(b.children, item)
		}
		return b, nil
	}
	indices := schema.FieldIndices(p.Column)
	if len(indices) != 1 {
		return nil, denied()
	}
	b.index = indices[0]
	if p.Kind == "is_null" || p.Kind == "is_not_null" {
		return b, nil
	}
	field := schema.Field(b.index)
	if p.Type == "string" {
		if field.Type.ID() != arrow.STRING && field.Type.ID() != arrow.LARGE_STRING {
			return nil, unsupported()
		}
		return b, nil
	}
	if p.Type == "bool" {
		if field.Type.ID() != arrow.BOOL {
			return nil, unsupported()
		}
		return b, nil
	}
	types := map[string]arrow.Type{"int8": arrow.INT8, "int16": arrow.INT16, "int32": arrow.INT32, "int64": arrow.INT64, "uint8": arrow.UINT8, "uint16": arrow.UINT16, "uint32": arrow.UINT32, "uint64": arrow.UINT64}
	id, ok := types[p.Type]
	if !ok || field.Type.ID() != id {
		return nil, unsupported()
	}
	_, signed, _ := integerType(p.Type)
	if signed {
		b.signed, _ = strconv.ParseInt(p.Value, 10, 64)
	} else {
		b.unsigned, _ = strconv.ParseUint(p.Value, 10, 64)
	}
	return b, nil
}

func (p *boundPredicate) match(record arrow.RecordBatch, row int) (bool, error) {
	if p.kind == "and" || p.kind == "or" {
		for _, child := range p.children {
			matches, err := child.match(record, row)
			if err != nil {
				return false, err
			}
			if p.kind == "and" && !matches {
				return false, nil
			}
			if p.kind == "or" && matches {
				return true, nil
			}
		}
		return p.kind == "and", nil
	}
	column := record.Column(p.index)
	null := column.IsNull(row)
	if p.kind == "is_null" {
		return null, nil
	}
	if p.kind == "is_not_null" {
		return !null, nil
	}
	if null {
		return false, nil
	}
	var order int
	switch values := column.(type) {
	case *array.Int8:
		order = cmp.Compare(int64(values.Value(row)), p.signed)
	case *array.Int16:
		order = cmp.Compare(int64(values.Value(row)), p.signed)
	case *array.Int32:
		order = cmp.Compare(int64(values.Value(row)), p.signed)
	case *array.Int64:
		order = cmp.Compare(values.Value(row), p.signed)
	case *array.Uint8:
		order = cmp.Compare(uint64(values.Value(row)), p.unsigned)
	case *array.Uint16:
		order = cmp.Compare(uint64(values.Value(row)), p.unsigned)
	case *array.Uint32:
		order = cmp.Compare(uint64(values.Value(row)), p.unsigned)
	case *array.Uint64:
		order = cmp.Compare(values.Value(row), p.unsigned)
	case *array.Boolean:
		if values.Value(row) != p.boolean {
			if p.boolean {
				order = -1
			} else {
				order = 1
			}
		}
	case *array.String:
		value := values.Value(row)
		if !utf8.ValidString(value) {
			return false, unsupported()
		}
		order = strings.Compare(value, p.text)
	case *array.LargeString:
		value := values.Value(row)
		if !utf8.ValidString(value) {
			return false, unsupported()
		}
		order = strings.Compare(value, p.text)
	default:
		return false, unsupported()
	}
	switch p.op {
	case "eq":
		return order == 0, nil
	case "ne":
		return order != 0, nil
	case "lt":
		return order < 0, nil
	case "le":
		return order <= 0, nil
	case "gt":
		return order > 0, nil
	case "ge":
		return order >= 0, nil
	default:
		return false, unsupported()
	}
}

// Nested/dictionary/extension schemas can carry hidden children or metadata.
// Support them only after their policy and serialization boundary is defined.
func flatType(t arrow.DataType) bool {
	if t == nil {
		return false
	}
	switch t.ID() {
	case arrow.NULL, arrow.BOOL, arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64, arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY,
		arrow.DECIMAL128, arrow.DECIMAL256, arrow.DATE32, arrow.DATE64, arrow.TIME32, arrow.TIME64, arrow.TIMESTAMP, arrow.DURATION:
		return true
	default:
		return false
	}
}
