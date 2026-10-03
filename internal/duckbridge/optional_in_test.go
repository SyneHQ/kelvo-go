//go:build duckbridge && duckdb_arrow && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckbridge

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func hasSparseEqualities(filters []Filter, column string) bool {
	for _, filter := range filters {
		if filter.Kind == "or" && len(filter.Children) >= 2 {
			exact := true
			for _, child := range filter.Children {
				exact = exact && child.Kind == "comparison" && child.Column == column && child.Op == "eq" && child.Type == "int64"
			}
			if exact {
				return true
			}
		}
		if hasSparseEqualities(filter.Children, column) {
			return true
		}
	}
	return false
}

func TestFactorySparseIntegerInPushesExactOptionalEqualities(t *testing.T) {
	var plans []ScanPlan
	conn, factory := registeredFactory(t, bridgeSchema(), func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		plans = append(plans, plan)
		return fixtureReader(t, bridgeSchema(), plan)
	})
	for _, tc := range []struct {
		sql    string
		want   int64
		pushed bool
	}{
		{"id IN (0,2,4)", 3, true},
		{"id IN (0,2,2,4)", 3, true},
		{"id >= 1 AND id IN (0,2,4)", 2, true},
		{"id IN (0,2,NULL)", 2, false},
		{"id NOT IN (0,2,4)", 2, false},
		{"id NOT IN (0,2,NULL)", 0, false},
		{"id IN (0,2,4) OR label='row_1'", 4, false},
		{"label IN ('row_0','row_2','row_4')", 3, false},
		{"enabled IN (true,false)", 5, false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			plans = nil
			var count int64
			err := conn.QueryRowContext(context.Background(), "SELECT count(*) FROM bridge.data WHERE "+tc.sql).Scan(&count)
			if err != nil || count != tc.want {
				t.Fatalf("IN semantics changed: count=%d want=%d err=%v bridge=%v", count, tc.want, err, factory.Err())
			}
			if tc.pushed {
				found := false
				for _, plan := range plans {
					found = found || hasSparseEqualities(plan.Filters, "id")
				}
				if !found {
					t.Fatalf("sparse IN did not reach producer: %+v", plans)
				}
			}
			for _, plan := range plans {
				var inspect func([]Filter)
				inspect = func(filters []Filter) {
					for _, filter := range filters {
						if filter.Column == "label" {
							t.Fatal("string predicate escaped the local engine")
						}
						inspect(filter.Children)
					}
				}
				inspect(plan.Filters)
			}
		})
	}
}

// A single integer column with NULL exercises SQL three-valued logic, and can
// use a long identifier to make a small IN list exceed the optional byte budget.
func optionalInReader(t *testing.T, schema *arrow.Schema, plan ScanPlan) (array.RecordReader, error) {
	t.Helper()
	var evaluate func(Filter, int64, bool) (bool, error)
	evaluate = func(filter Filter, value int64, null bool) (bool, error) {
		if filter.Kind == "and" || filter.Kind == "or" {
			result := filter.Kind == "and"
			for _, child := range filter.Children {
				match, err := evaluate(child, value, null)
				if err != nil {
					return false, err
				}
				if filter.Kind == "and" {
					result = result && match
				} else {
					result = result || match
				}
			}
			return result, nil
		}
		if filter.Kind == "is_null" {
			return null, nil
		}
		if filter.Kind == "is_not_null" {
			return !null, nil
		}
		if null {
			return false, nil
		}
		filter.Column = "id"
		return fixtureFilter(filter, value)
	}
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	for i := int64(0); i < 6; i++ {
		null, keep := i == 5, true
		for _, filter := range plan.Filters {
			match, err := evaluate(filter, i, null)
			if err != nil {
				return nil, err
			}
			keep = keep && match
		}
		if keep {
			if null {
				builder.AppendNull()
			} else {
				builder.Append(i)
			}
		}
	}
	column := builder.NewArray()
	defer column.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(column.Len()))
	defer record.Release()
	return array.NewRecordReader(schema, []arrow.RecordBatch{record})
}

func TestFactoryOptionalInBudgetsLeaveResidualPredicates(t *testing.T) {
	for _, tc := range []struct {
		name         string
		width, count int
	}{
		{"large list", 2, 257},
		{"large identifiers", 900, 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := strings.Repeat("i", tc.width)
			schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
			var plans []ScanPlan
			conn, factory := registeredFactory(t, schema, func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
				plans = append(plans, plan)
				return optionalInReader(t, schema, plan)
			})
			// Keep a large IN as a filter rather than rewriting it to a join;
			// the bridge must independently decline oversized optional hints.
			if _, err := conn.ExecContext(context.Background(), "SET disabled_optimizers='in_clause'"); err != nil {
				t.Fatal(err)
			}
			values := make([]string, tc.count)
			for i := range values {
				values[i] = strconv.Itoa(i * 2)
			}
			var count int64
			err := conn.QueryRowContext(context.Background(), `SELECT count(*) FROM bridge.data WHERE "`+name+`" IN (`+strings.Join(values, ",")+`)`).Scan(&count)
			if err != nil || count != 3 {
				t.Fatalf("optional budget changed residual results: %d %v %v", count, err, factory.Err())
			}
			if len(plans) == 0 {
				t.Fatal("source scan missing")
			}
			for _, plan := range plans {
				if len(plan.Filters) != 0 {
					t.Fatalf("oversized optional hint was pushed: %+v", plan.Filters)
				}
			}
		})
	}
}

func TestFactorySparseInRetainsSourceNullSemantics(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	conn, factory := registeredFactory(t, schema, func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		return optionalInReader(t, schema, plan)
	})
	for _, tc := range []struct {
		predicate string
		want      int64
	}{
		{"id IN (0,2,4)", 3}, {"id IN (0,2,NULL)", 2}, {"id NOT IN (0,2,4)", 2}, {"id IN (0,2,4) OR id IS NULL", 4},
	} {
		var count int64
		err := conn.QueryRowContext(context.Background(), "SELECT count(*) FROM bridge.data WHERE "+tc.predicate).Scan(&count)
		if err != nil || count != tc.want {
			t.Fatalf("NULL semantics changed for %s: %d %v %v", tc.predicate, count, err, factory.Err())
		}
	}
}
