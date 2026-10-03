//go:build duckbridge && duckdb_arrow && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckbridge

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func aggregateINNodeCount(filters []Filter) int {
	count := 0
	for _, filter := range filters {
		count += 1 + aggregateINNodeCount(filter.Children)
	}
	return count
}
func aggregateINMatches(filter Filter, row map[string]int64) (bool, error) {
	if filter.Kind == "and" || filter.Kind == "or" {
		result := filter.Kind == "and"
		for _, child := range filter.Children {
			matched, err := aggregateINMatches(child, row)
			if err != nil {
				return false, err
			}
			if filter.Kind == "and" {
				result = result && matched
			} else {
				result = result || matched
			}
		}
		return result, nil
	}
	value, ok := row[filter.Column]
	if !ok {
		return false, errors.New("unknown aggregate budget column")
	}
	filter.Column = "id"
	return fixtureFilter(filter, value)
}
func aggregateINReader(schema *arrow.Schema, plan ScanPlan, rows []map[string]int64) (array.RecordReader, error) {
	fields := make([]arrow.Field, len(plan.Columns))
	for i, name := range plan.Columns {
		indices := schema.FieldIndices(name)
		if len(indices) != 1 {
			return nil, errors.New("invalid budget projection")
		}
		fields[i] = schema.Field(indices[0])
	}
	if len(fields) == 0 {
		return nil, errors.New("budget query unexpectedly projected no columns")
	}
	projected := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, projected)
	defer builder.Release()
	for _, row := range rows {
		keep := true
		for _, filter := range plan.Filters {
			match, err := aggregateINMatches(filter, row)
			if err != nil {
				return nil, err
			}
			keep = keep && match
		}
		if !keep {
			continue
		}
		for i, field := range fields {
			builder.Field(i).(*array.Int64Builder).Append(row[field.Name])
		}
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	return array.NewRecordReader(projected, []arrow.RecordBatch{record})
}
func aggregateINHasMandatoryGate(filters []Filter) bool {
	for _, filter := range filters {
		if filter.Kind == "comparison" && filter.Column == "gate" && filter.Type == "int64" && filter.Op == "eq" && filter.Value == "7" {
			return true
		}
		// A mandatory equality may be nested under conjunction, never inferred
		// from an OR arm where it would not restrict the complete relation.
		if filter.Kind == "and" && aggregateINHasMandatoryGate(filter.Children) {
			return true
		}
	}
	return false
}

func TestFactoryOptionalInAggregateBudgetKeepsMandatoryAndResidual(t *testing.T) {
	const columns = 5
	const members = 256
	fields := []arrow.Field{{Name: "row_id", Type: arrow.PrimitiveTypes.Int64}, {Name: "gate", Type: arrow.PrimitiveTypes.Int64}}
	predicates := []string{"gate = 7"}
	values := make([]string, members)
	for i := range values {
		values[i] = strconv.Itoa(i * 2)
	}
	for i := 0; i < columns; i++ {
		name := "v" + strconv.Itoa(i)
		fields = append(fields, arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64})
		predicates = append(predicates, name+" IN ("+strings.Join(values, ",")+")")
	}
	schema := arrow.NewSchema(fields, nil)
	rows := make([]map[string]int64, columns+2)
	for i := range rows {
		row := map[string]int64{"row_id": int64(42 + i), "gate": 7}
		for c := 0; c < columns; c++ {
			row["v"+strconv.Itoa(c)] = 0
		}
		if i > 0 && i <= columns {
			row["v"+strconv.Itoa(i-1)] = 1
		}
		if i == columns+1 {
			row["gate"] = 0
		}
		rows[i] = row
	}
	// Every rejected row violates exactly one membership (or the mandatory
	// gate), so omitting a hint without retaining its residual leaks a real row.
	var plans []ScanPlan
	conn, factory := registeredFactory(t, schema, func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		plans = append(plans, plan)
		return aggregateINReader(schema, plan, rows)
	})
	if _, err := conn.ExecContext(context.Background(), "SET disabled_optimizers='in_clause'"); err != nil {
		t.Fatal(err)
	}
	result, err := conn.QueryContext(context.Background(), "SELECT row_id FROM bridge.data WHERE "+strings.Join(predicates, " AND ")+" ORDER BY row_id")
	if err != nil {
		t.Fatalf("aggregate optional budget failed: %v bridge=%v", err, factory.Err())
	}
	var ids []int64
	for result.Next() {
		var id int64
		if err = result.Scan(&id); err != nil {
			_ = result.Close()
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	err = result.Err()
	_ = result.Close()
	if err != nil || len(ids) != 1 || ids[0] != 42 {
		t.Fatalf("mandatory/residual predicate lost: ids=%v error=%v bridge=%v", ids, err, factory.Err())
	}
	if len(plans) == 0 {
		t.Fatal("no aggregate-budget scan observed")
	}
	for _, plan := range plans {
		if !aggregateINHasMandatoryGate(plan.Filters) {
			t.Fatal("mandatory equality disappeared when optional budget filled")
		}
		hinted := 0
		for i := 0; i < columns; i++ {
			if hasSparseEqualities(plan.Filters, "v"+strconv.Itoa(i)) {
				hinted++
			}
		}
		if hinted == 0 || hinted == columns {
			t.Fatalf("expected some accepted and some residual memberships, got %d", hinted)
		}
		if len(plan.Filters) > 256 || aggregateINNodeCount(plan.Filters) > 1024 {
			t.Fatal("aggregate predicate complexity exceeded budget")
		}
		wire, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		if len(wire) > 32<<10 {
			t.Fatal("aggregate optional expansion exceeded complete-plan byte budget")
		}
	}
	// Five individually legal lists contain1285 optional nodes; both the global
	// node ceiling and tighter32KiB serialization ceiling must hold. This test
	// does not claim which guard declines each optimization.
}

func TestFactoryOptionalInEscapedIdentifierBudgetKeepsResidual(t *testing.T) {
	name := strings.Repeat(`]"`, 450)
	schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	var plans []ScanPlan
	conn, factory := registeredFactory(t, schema, func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		plans = append(plans, plan)
		return optionalInReader(t, schema, plan)
	})
	if _, err := conn.ExecContext(context.Background(), "SET disabled_optimizers='in_clause'"); err != nil {
		t.Fatal(err)
	}
	values := make([]string, 64)
	for i := range values {
		values[i] = strconv.Itoa(i * 2)
	}
	quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	var count int64
	err := conn.QueryRowContext(context.Background(), "SELECT count(*) FROM bridge.data WHERE "+quoted+" IN ("+strings.Join(values, ",")+")").Scan(&count)
	if err != nil || count != 3 {
		t.Fatalf("escaped identifier budget changed residual: count=%d err=%v bridge=%v", count, err, factory.Err())
	}
	if len(plans) == 0 {
		t.Fatal("escaped identifier scan missing")
	}
	for _, plan := range plans {
		if len(plan.Filters) != 0 {
			t.Fatal("oversized escaped-identifier hint reached adapter")
		}
	}
}
