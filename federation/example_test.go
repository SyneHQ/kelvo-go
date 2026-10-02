// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation_test

import (
	"context"
	"fmt"
	"time"

	"github.com/SYNEHQ/kelvo-go/federation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// This external-package example cannot import Kelvo's internal packages.
// A real adapter replaces the static relation with its own bounded source I/O.
type staticDriver struct{}

func (staticDriver) Validate(source federation.Source, table federation.Table) error {
	if source.Type != "example_static" || table.Table != "rows" || table.Database != "" || table.Schema != "" {
		return federation.ErrUnsupported
	}
	return nil
}
func (d staticDriver) Open(ctx context.Context, source federation.Source, table federation.Table, limits federation.Limits) (federation.Relation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.Validate(source, table); err != nil {
		return nil, err
	}
	return staticRelation{}, nil
}

type staticRelation struct{}

func (staticRelation) Schema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
}
func (r staticRelation) Scan(ctx context.Context, plan federation.ScanPlan, sink federation.Sink) (federation.ScanStats, error) {
	if err := ctx.Err(); err != nil {
		return federation.ScanStats{}, err
	}
	if len(plan.Filters) != 0 || len(plan.Columns) != 1 || plan.Columns[0] != "id" {
		return federation.ScanStats{}, federation.ErrUnsupported
	}
	schema := r.Schema()
	if err := sink.Schema(schema); err != nil {
		return federation.ScanStats{}, err
	}
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.Append(1)
	column := builder.NewArray()
	defer column.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 1)
	defer record.Release()
	return federation.ScanStats{}, sink.Write(record)
}
func (staticRelation) Close() error { return nil }

type countingSink struct{ rows int64 }

func (*countingSink) Schema(*arrow.Schema) error { return nil }
func (s *countingSink) Write(record arrow.RecordBatch) error {
	s.rows += record.NumRows()
	return nil
}

func init() {
	// Registration runs in both the gateway and each re-executed worker.
	federation.MustRegister("example_static", staticDriver{})
}

func ExampleRegister() {
	driver, _ := federation.Lookup("example_static")
	relation, err := driver.Open(context.Background(), federation.Source{ID: "demo", Type: "example_static"}, federation.Table{Name: "rows", Table: "rows"}, federation.Limits{MaxRows: 100, MaxBytes: 1024, Timeout: time.Second, MemoryMB: 16, Threads: 1})
	if err != nil {
		panic(err)
	}
	defer relation.Close()
	sink := &countingSink{}
	_, err = relation.Scan(context.Background(), federation.ScanPlan{Columns: []string{"id"}}, sink)
	if err != nil {
		panic(err)
	}
	fmt.Println(sink.rows)
	// Output: 1
}
