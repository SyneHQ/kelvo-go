// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type customTestDriver struct {
	opened, closed atomic.Int32
	schemaAt       func(int32) *arrow.Schema
	run            func(context.Context, federationapi.ScanPlan, federationapi.Sink) (federationapi.ScanStats, error)
	closeAt        func(int32) error
}

func (*customTestDriver) Validate(federationapi.Source, federationapi.Table) error { return nil }
func (d *customTestDriver) Open(ctx context.Context, source federationapi.Source, selected federationapi.Table, limits federationapi.Limits) (federationapi.Relation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source.ID != "custom" || selected.Table != "events" || limits.MaxRows < 1 || limits.MaxBytes < 1024 {
		return nil, errors.New("invalid public adapter input")
	}
	n := d.opened.Add(1)
	schema := idSchema
	if d.schemaAt != nil {
		schema = d.schemaAt(n)
	}
	return &customTestRelation{driver: d, schema: schema, ordinal: n}, nil
}

type customTestRelation struct {
	driver  *customTestDriver
	schema  *arrow.Schema
	ordinal int32
}

func (r *customTestRelation) Schema() *arrow.Schema { return r.schema }
func (r *customTestRelation) Scan(ctx context.Context, plan federationapi.ScanPlan, sink federationapi.Sink) (federationapi.ScanStats, error) {
	if r.driver.run != nil {
		return r.driver.run(ctx, plan, sink)
	}
	return federationapi.ScanStats{}, sink.Schema(r.schema)
}
func (r *customTestRelation) Close() error {
	r.driver.closed.Add(1)
	if r.driver.closeAt != nil {
		return r.driver.closeAt(r.ordinal)
	}
	return nil
}

func init() { federationapi.MustRegister("scan_fixture", &customTestDriver{}) }

func customFixtureSource() (catalog.Source, catalog.FederationTable) {
	table := catalog.FederationTable{Name: "orders", Table: "events"}
	return catalog.Source{ID: "custom", Type: "scan_fixture", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{table}}}, table
}
func customFixtureTable(t *testing.T, driver *customTestDriver, limits query.Limits) *Table {
	t.Helper()
	source, selected := customFixtureSource()
	table, err := newCustomTable(context.Background(), source, selected, limits, driver)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = table.Close() })
	return table
}

func TestCustomIndependentScansRetainOwnershipAndCountBytes(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	driver := &customTestDriver{run: func(ctx context.Context, plan federationapi.ScanPlan, sink federationapi.Sink) (federationapi.ScanStats, error) {
		stats := federationapi.ScanStats{SourceWireBytes: 123}
		if len(plan.Columns) != 1 || plan.Columns[0] != "id" {
			return stats, errors.New("unexpected projection")
		}
		if err := sink.Schema(idSchema); err != nil {
			return stats, err
		}
		record := uintRecord(allocator, 18446744073709551615)
		defer record.Release()
		return stats, sink.Write(record)
	}}
	table := customFixtureTable(t, driver, query.DefaultLimits())
	if driver.opened.Load() != 1 || driver.closed.Load() != 1 {
		t.Fatal("schema discovery did not close its independent relation")
	}
	first, err := table.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := table.Scan(context.Background(), federationapi.ScanPlan{})
	if err != nil {
		first.Release()
		t.Fatal(err)
	}
	if !first.Next() || !second.Next() {
		first.Release()
		second.Release()
		t.Fatal("independent scan did not produce a batch")
	}
	retained := first.RecordBatch()
	retained.Retain()
	if first.Next() || first.Err() != nil || second.Next() || second.Err() != nil {
		t.Fatal("scan did not finish cleanly")
	}
	first.Release()
	second.Release()
	_ = table.Close()
	if retained.Column(0).(*array.Uint64).Value(0) != 18446744073709551615 {
		t.Fatal("retained batch changed after relation closure")
	}
	retained.Release()
	allocator.AssertSize(t, 0)
	if driver.opened.Load() != 3 || driver.closed.Load() != 3 {
		t.Fatalf("relations were reused or leaked: %d opened, %d closed", driver.opened.Load(), driver.closed.Load())
	}
	if stats := table.Stats(); stats.Rows != 2 || stats.Scans != 2 || stats.SourceWireBytes != 246 {
		t.Fatalf("custom source statistics changed: %+v", stats)
	}
}

func TestCustomRejectsInvalidPlansBeforeOpeningScan(t *testing.T) {
	driver := &customTestDriver{}
	table := customFixtureTable(t, driver, query.DefaultLimits())
	if _, err := table.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"secret"}}); err == nil {
		t.Fatal("unknown projection accepted")
	}
	for _, filter := range []federationapi.Filter{
		{Kind: "comparison", Column: "id", Op: "eq", Type: "uint64", Value: "18446744073709551616"},
		{Kind: "comparison", Column: "id", Op: "eq", Type: "uint64", Value: "01"},
		{Kind: "comparison", Column: "id", Op: "eq", Type: "int64", Value: "1"},
		{Kind: "comparison", Column: "id", Op: "eq", Type: "string", Value: "secret"},
		{Kind: "is_null", Column: "id", Value: "ignored"},
		{Kind: "is_null", Column: "missing"},
		{Kind: "and"},
		{Kind: "arbitrary_sql", Value: "SELECT * FROM secret"},
	} {
		_, err := table.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"id"}, Filters: []federationapi.Filter{filter}})
		checkCode(t, err, "UNSUPPORTED")
	}
	if driver.opened.Load() != 1 {
		t.Fatal("invalid plan reached a source connection")
	}
	source, selected := customFixtureSource()
	selected.Table = "secret"
	if _, err := newCustomTable(context.Background(), source, selected, query.DefaultLimits(), driver); err == nil {
		t.Fatal("unregistered table reached the driver")
	}
}

func TestCustomSinkLimitsSurviveAnAdapterIgnoringErrors(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	driver := &customTestDriver{run: func(ctx context.Context, plan federationapi.ScanPlan, sink federationapi.Sink) (federationapi.ScanStats, error) {
		_ = sink.Schema(idSchema)
		record := uintRecord(allocator, 1, 2)
		defer record.Release()
		_ = sink.Write(record)
		return federationapi.ScanStats{SourceWireBytes: 99}, nil
	}}
	limits := query.DefaultLimits()
	limits.MaxRows = 1
	table := customFixtureTable(t, driver, limits)
	reader, err := table.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	if reader.Next() {
		t.Fatal("adapter silently exceeded the row budget")
	}
	checkCode(t, reader.Err(), "RESOURCE_EXHAUSTED")
	reader.Release()
	allocator.AssertSize(t, 0)
	if stats := table.Stats(); stats.Rows != 0 || stats.SourceWireBytes != 99 {
		t.Fatalf("rejected batch or wire accounting changed: %+v", stats)
	}
}

func TestCustomSchemaChangeAndMissingSchemaFail(t *testing.T) {
	for _, mode := range []string{"changed", "missing"} {
		t.Run(mode, func(t *testing.T) {
			driver := &customTestDriver{}
			if mode == "changed" {
				metadata := arrow.MetadataFrom(map[string]string{"version": "changed"})
				driver.schemaAt = func(n int32) *arrow.Schema {
					if n == 1 {
						return idSchema
					}
					return arrow.NewSchema(idSchema.Fields(), &metadata)
				}
			} else {
				driver.run = func(context.Context, federationapi.ScanPlan, federationapi.Sink) (federationapi.ScanStats, error) {
					return federationapi.ScanStats{}, nil
				}
			}
			table := customFixtureTable(t, driver, query.DefaultLimits())
			reader, err := table.Scan(context.Background(), federationapi.ScanPlan{})
			if err != nil {
				t.Fatal(err)
			}
			if reader.Next() {
				t.Fatal("invalid schema produced a batch")
			}
			checkCode(t, reader.Err(), "QUERY_FAILED")
			reader.Release()
			if driver.closed.Load() != 2 {
				t.Fatal("failed scan relation leaked")
			}
		})
	}
}

func TestCustomCancellationClosesSource(t *testing.T) {
	started := make(chan struct{})
	driver := &customTestDriver{run: func(ctx context.Context, plan federationapi.ScanPlan, sink federationapi.Sink) (federationapi.ScanStats, error) {
		close(started)
		<-ctx.Done()
		return federationapi.ScanStats{}, ctx.Err()
	}}
	table := customFixtureTable(t, driver, query.DefaultLimits())
	ctx, cancel := context.WithCancel(context.Background())
	reader, err := table.Scan(ctx, federationapi.ScanPlan{})
	if err != nil {
		t.Fatal(err)
	}
	await(t, started)
	cancel()
	if reader.Next() {
		t.Fatal("cancelled scan produced data")
	}
	checkCode(t, reader.Err(), "CANCELLED")
	reader.Release()
	if driver.closed.Load() != 2 {
		t.Fatal("cancellation leaked the source")
	}
}

func TestCustomErrorsAndPanicsAreSanitized(t *testing.T) {
	for _, mode := range []string{"unsupported", "error", "panic", "cleanup"} {
		t.Run(mode, func(t *testing.T) {
			driver := &customTestDriver{run: func(ctx context.Context, plan federationapi.ScanPlan, sink federationapi.Sink) (federationapi.ScanStats, error) {
				switch mode {
				case "unsupported":
					return federationapi.ScanStats{}, fmt.Errorf("private secret: %w", federationapi.ErrUnsupported)
				case "error":
					return federationapi.ScanStats{}, query.NewError("QUERY_FAILED", "private secret")
				case "panic":
					panic("private secret")
				default:
					return federationapi.ScanStats{}, sink.Schema(idSchema)
				}
			}}
			if mode == "cleanup" {
				driver.closeAt = func(n int32) error {
					if n > 1 {
						panic("private secret")
					}
					return nil
				}
			}
			table := customFixtureTable(t, driver, query.DefaultLimits())
			reader, err := table.Scan(context.Background(), federationapi.ScanPlan{})
			if err != nil {
				t.Fatal(err)
			}
			if reader.Next() {
				t.Fatal("failed adapter produced a batch")
			}
			code := "QUERY_FAILED"
			if mode == "unsupported" {
				code = "UNSUPPORTED"
			}
			checkCode(t, reader.Err(), code)
			if strings.Contains(reader.Err().Error(), "private") {
				t.Fatal("adapter diagnostic escaped")
			}
			reader.Release()
		})
	}
}

func TestCustomRegistryIsUsedWithoutNativeSQLFallback(t *testing.T) {
	source, selected := customFixtureSource()
	table, err := New(context.Background(), source, selected, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	_ = table.Close()
	source.Type = "unregistered_source"
	if _, err := New(context.Background(), source, selected, query.DefaultLimits()); err == nil {
		t.Fatal("unknown type acquired a fallback source")
	}
}
