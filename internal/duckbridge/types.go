// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package duckbridge connects pinned DuckDB Arrow scan planning to Go readers.
package duckbridge

import (
	"context"
	"strconv"

	"github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// PredicateCapabilities explicitly allows the existing integer/Boolean filter
// vocabulary on named fields of the exposed schema. Empty keeps query predicates
// in DuckDB; it does not control independent authorization predicates.
type PredicateCapabilities struct{ Columns []string }

type predicateScalar struct {
	kind   string
	bits   int
	signed bool
}

func predicateScalarType(id arrow.Type) (predicateScalar, bool) {
	switch id {
	case arrow.BOOL:
		return predicateScalar{kind: "bool"}, true
	case arrow.INT8:
		return predicateScalar{"int8", 8, true}, true
	case arrow.INT16:
		return predicateScalar{"int16", 16, true}, true
	case arrow.INT32:
		return predicateScalar{"int32", 32, true}, true
	case arrow.INT64:
		return predicateScalar{"int64", 64, true}, true
	case arrow.UINT8:
		return predicateScalar{"uint8", 8, false}, true
	case arrow.UINT16:
		return predicateScalar{"uint16", 16, false}, true
	case arrow.UINT32:
		return predicateScalar{"uint32", 32, false}, true
	case arrow.UINT64:
		return predicateScalar{"uint64", 64, false}, true
	default:
		return predicateScalar{}, false
	}
}

func bindPredicateCapabilities(schema *arrow.Schema, capabilities PredicateCapabilities) (map[string]arrow.Type, []uint32, error) {
	invalid := query.NewError("INVALID_ARGUMENT", "Native bridge predicate columns are unavailable or unsupported")
	if schema == nil || schema.NumFields() == 0 || schema.NumFields() > 1024 || len(capabilities.Columns) > 1024 {
		return nil, nil, invalid
	}
	columns := append([]string(nil), capabilities.Columns...)
	eligible := make(map[string]arrow.Type, len(columns))
	ordinals := make([]uint32, 0, len(columns))
	for _, name := range columns {
		indexes := schema.FieldIndices(name)
		if _, duplicate := eligible[name]; duplicate || len(indexes) != 1 {
			return nil, nil, invalid
		}
		field := schema.Field(indexes[0])
		if field.Type == nil {
			return nil, nil, invalid
		}
		if _, supported := predicateScalarType(field.Type.ID()); !supported {
			return nil, nil, invalid
		}
		eligible[name] = field.Type.ID()
		ordinals = append(ordinals, uint32(indexes[0]))
	}
	return eligible, ordinals, nil
}

// Mandatory filters arriving from the native bridge cannot bypass the factory's
// registered eligibility. Shape and scalar checks match the existing vocabulary;
// unsupported filters are errors, never a reason to discard a predicate.
func validatePredicatePlan(plan ScanPlan, eligible map[string]arrow.Type) error {
	unsupported := query.NewError("UNSUPPORTED", "Required native bridge predicate is unavailable or invalid")
	if len(plan.Filters) > 256 {
		return unsupported
	}
	nodes := 0
	var validate func(Filter, int) error
	validate = func(filter Filter, depth int) error {
		nodes++
		if depth > 32 || nodes > 1024 {
			return unsupported
		}
		if filter.Kind == "and" || filter.Kind == "or" {
			if len(filter.Children) == 0 || len(filter.Children) > 256 || filter.Column != "" || filter.Op != "" || filter.Type != "" || filter.Value != "" {
				return unsupported
			}
			for _, child := range filter.Children {
				if err := validate(child, depth+1); err != nil {
					return err
				}
			}
			return nil
		}
		id, allowed := eligible[filter.Column]
		if !allowed || len(filter.Children) != 0 {
			return unsupported
		}
		typ, supported := predicateScalarType(id)
		if !supported {
			return unsupported
		}
		if filter.Kind == "is_null" || filter.Kind == "is_not_null" {
			if filter.Op != "" || filter.Type != "" || filter.Value != "" {
				return unsupported
			}
			return nil
		}
		if filter.Kind != "comparison" || filter.Type != typ.kind {
			return unsupported
		}
		switch filter.Op {
		case "eq", "ne", "lt", "le", "gt", "ge":
		default:
			return unsupported
		}
		if typ.kind == "bool" {
			if filter.Value != "true" && filter.Value != "false" {
				return unsupported
			}
			return nil
		}
		if len(filter.Value) > 21 {
			return unsupported
		}
		if typ.signed {
			value, err := strconv.ParseInt(filter.Value, 10, typ.bits)
			if err != nil || strconv.FormatInt(value, 10) != filter.Value {
				return unsupported
			}
		} else {
			value, err := strconv.ParseUint(filter.Value, 10, typ.bits)
			if err != nil || strconv.FormatUint(value, 10) != filter.Value {
				return unsupported
			}
		}
		return nil
	}
	for _, filter := range plan.Filters {
		if err := validate(filter, 0); err != nil {
			return err
		}
	}
	return nil
}

// ScanPlan contains the exact source column order required by DuckDB and every
// required pushed filter. The producer must apply every filter or return an
// error; DuckDB does not reapply pushed filters after consuming Arrow batches.
type ScanPlan = federation.ScanPlan

// Filter is a typed predicate, never source SQL. Kinds are comparison, is_null,
// is_not_null, and, or. Comparisons use eq/ne/lt/le/gt/ge and exact lexical values
// with type int8/int16/int32/int64/uint8/uint16/uint32/uint64/bool.
type Filter = federation.Filter

// Producer returns an owned reader whose schema and columns follow plan.Columns.
// For an empty projection it returns one private constant column per source row;
// DuckDB consumes only the row count. Calls may overlap for independent scans.
type Producer func(context.Context, ScanPlan) (array.RecordReader, error)
