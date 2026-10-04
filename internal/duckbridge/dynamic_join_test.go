//go:build duckbridge && duckdb_arrow && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckbridge

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Dates select sparse join keys without putting an integer predicate in SQL.
// The last date is NULL, so local and source filtering must agree on its fate.
func dynamicJoinReader(schema *arrow.Schema, plan ScanPlan) (array.RecordReader, error) {
	fields := make([]arrow.Field, len(plan.Columns))
	for i, name := range plan.Columns {
		fields[i] = schema.Field(schema.FieldIndices(name)[0])
	}
	projected := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, projected)
	defer builder.Release()
	for id := int64(0); id < 6; id++ {
		day, null := int32(id%2-1), id == 5
		var match func(Filter) (bool, error)
		match = func(filter Filter) (bool, error) {
			if filter.Kind == "and" || filter.Kind == "or" {
				keep := filter.Kind == "and"
				for _, child := range filter.Children {
					value, err := match(child)
					if err != nil {
						return false, err
					}
					if filter.Kind == "and" {
						keep = keep && value
					} else {
						keep = keep || value
					}
				}
				return keep, nil
			}
			if filter.Column == "event_day" {
				return date32FixtureMatch(filter, day, null)
			}
			return fixtureFilter(filter, id)
		}
		keep := true
		for _, filter := range plan.Filters {
			value, err := match(filter)
			if err != nil {
				return nil, err
			}
			keep = keep && value
		}
		if !keep {
			continue
		}
		for i, field := range fields {
			switch field.Name {
			case "id":
				builder.Field(i).(*array.Int64Builder).Append(id)
			case "label":
				builder.Field(i).(*array.StringBuilder).Append(fmt.Sprintf("row_%d", id))
			case "event_day":
				if null {
					builder.Field(i).AppendNull()
				} else {
					builder.Field(i).(*array.Date32Builder).Append(arrow.Date32(day))
				}
			}
		}
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	return array.NewRecordReader(projected, []arrow.RecordBatch{record})
}

func checkDynamicJoinCapabilities(t *testing.T, singleton bool) {
	t.Helper()
	for _, reordered := range []bool{false, true} {
		for _, columns := range [][]string{nil, {"event_day"}, {"id"}, {"event_day", "id"}} {
			t.Run(fmt.Sprintf("reordered=%t/capabilities=%v", reordered, columns), func(t *testing.T) {
				fields := []arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
					{Name: "label", Type: arrow.BinaryTypes.String},
					{Name: "event_day", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
				}
				if reordered {
					fields = []arrow.Field{fields[1], fields[2], fields[0]}
				}
				schema := arrow.NewSchema(fields, nil)
				observed := &boundObservation{}
				conn, factories := boundConnection(t, boundTableFixture{"data", schema,
					observed.wrap(func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
						return dynamicJoinReader(schema, plan)
					}), PredicateCapabilities{Columns: columns}})
				defer func() {
					if !t.Failed() {
						return
					}
					for _, factory := range factories {
						var failure *query.Error
						if errors.As(factory.Err(), &failure) && failure.Code == "UNSUPPORTED" &&
							failure.Message == "Required native bridge predicate is unavailable or invalid" {
							t.Log("dynamic_join_ineligible_filter_rejected")
						}
					}
				}()
				// Change the build keys on the same factory: a prior hint must not leak.
				for repeat := 0; repeat < 2; repeat++ {
					predicate := "b.event_day<DATE '1970-01-01'"
					want := [][]any{{int64(0), int64(0)}, {int64(2), int64(2)}, {int64(4), int64(4)}}
					if singleton {
						id := int64(2 + repeat*2)
						predicate += fmt.Sprintf(" AND b.label='row_%d'", id)
						want = [][]any{{id, id}}
					} else if repeat == 1 {
						predicate = "b.event_day>=DATE '1970-01-01'"
						want = [][]any{{int64(1), int64(1)}, {int64(3), int64(3)}}
					}
					statement := "SELECT a.id,b.id FROM bridge.data a JOIN bridge.data b ON a.id=b.id WHERE " + predicate + " ORDER BY a.id,b.id"
					got := readDate32Rows(t, conn, statement)
					if !reflect.DeepEqual(got.types, []string{"BIGINT", "BIGINT"}) || !reflect.DeepEqual(got.values, want) {
						t.Fatalf("dynamic join changed typed rows: %#v", got)
					}
				}
				eligible, _, err := bindPredicateCapabilities(schema, PredicateCapabilities{Columns: columns})
				if err != nil {
					t.Fatal(err)
				}
				plans, _, _ := observed.snapshot()
				if len(plans) < 4 {
					t.Fatalf("join scans missing: %+v", plans)
				}
				pushedSparse := false
				for _, plan := range plans {
					if err := validatePredicatePlan(plan, eligible); err != nil {
						t.Fatalf("ineligible hint reached producer: %+v: %v", plan, err)
					}
					if len(columns) == 0 && len(plan.Filters) != 0 {
						t.Fatalf("disabled source received predicates: %+v", plan)
					}
					pushedSparse = pushedSparse || hasSparseEqualities(plan.Filters, "id")
				}
				if !singleton && eligible["id"] == arrow.INT64 && !pushedSparse {
					t.Fatalf("eligible sparse join hint stayed local: %+v", plans)
				}
				for _, factory := range factories {
					if err := factory.Err(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestBoundDynamicSparseJoinsRespectCapabilities(t *testing.T) {
	checkDynamicJoinCapabilities(t, false)
}

func TestBoundDynamicSingletonJoinsRespectCapabilities(t *testing.T) {
	checkDynamicJoinCapabilities(t, true)
}
