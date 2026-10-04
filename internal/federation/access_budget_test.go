// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestRawSourceBudgetPrecedesPolicyFiltering(t *testing.T) {
	for _, kind := range []string{"rows", "bytes"} {
		t.Run(kind, func(t *testing.T) {
			limits := query.DefaultLimits()
			values := []uint64{1, 2}
			if kind == "rows" {
				limits.MaxRows = 1
			} else {
				limits.MaxBytes = 1024
				values = make([]uint64, 300)
			}
			var closed atomic.Int32
			table := mockTable(t, limits, testSource(), func(ctx context.Context, _ query.Request, sink query.Sink) error {
				if err := sink.Schema(idSchema); err != nil {
					return err
				}
				record := uintRecord(memory.DefaultAllocator, values...)
				defer record.Release()
				return sink.Write(record)
			}, &closed)
			guard, err := access.NewRelation(table.Schema(), access.TablePolicy{Columns: []string{"id"}, Rows: &access.Predicate{Kind: "comparison", Column: "id", Type: "uint64", Op: "eq", Value: "999"}}, table.Scan)
			if err != nil {
				t.Fatal(err)
			}
			r, err := guard.Scan(context.Background(), duckbridge.ScanPlan{})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Release()
			if r.Next() {
				t.Fatal("over-budget raw batch reached policy output")
			}
			checkCode(t, r.Err(), "RESOURCE_EXHAUSTED")
			if table.Stats().Batches != 0 {
				t.Fatal("rejected raw batch was handed to guard")
			}
		})
	}
}
