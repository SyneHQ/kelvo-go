//go:build duckdb_arrow && duckbridge && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckbridge

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

type boundObservation struct {
	mu          sync.Mutex
	plans       []ScanPlan
	rows, bytes int64
}

func (o *boundObservation) wrap(producer Producer) Producer {
	return func(ctx context.Context, plan ScanPlan) (array.RecordReader, error) {
		o.mu.Lock()
		o.plans = append(o.plans, plan)
		o.mu.Unlock()
		reader, err := producer(ctx, plan)
		if err != nil {
			return reader, err
		}
		return &boundCountingReader{RecordReader: reader, observed: o}, nil
	}
}

type boundCountingReader struct {
	array.RecordReader
	observed *boundObservation
}

func (r *boundCountingReader) Next() bool {
	if !r.RecordReader.Next() {
		return false
	}
	record := r.RecordBatch()
	r.observed.mu.Lock()
	r.observed.rows += record.NumRows()
	r.observed.bytes += int64(arrowutil.TotalRecordSize(record))
	r.observed.mu.Unlock()
	return true
}

func (o *boundObservation) snapshot() ([]ScanPlan, int64, int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]ScanPlan(nil), o.plans...), o.rows, o.bytes
}

type boundTableFixture struct {
	name         string
	schema       *arrow.Schema
	producer     Producer
	capabilities PredicateCapabilities
}

func boundConnection(t *testing.T, tables ...boundTableFixture) (*sql.Conn, []*Factory) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		cancel()
		t.Fatal(err)
	}
	var factories []*Factory
	t.Cleanup(func() {
		conn.Close()
		db.Close()
		for _, factory := range factories {
			factory.Close()
		}
		cancel()
		runtime.GC()
		if activeStreams.Load() != 0 || activePins.Load() != 0 {
			t.Errorf("bound factory leaked streams=%d pins=%d", activeStreams.Load(), activePins.Load())
		}
	})
	if _, err = conn.ExecContext(ctx, "CREATE SCHEMA bridge"); err != nil {
		t.Fatal(err)
	}
	for _, table := range tables {
		factory, err := New(ctx, table.schema, table.producer, table.capabilities)
		if err != nil {
			t.Fatal(err)
		}
		factories = append(factories, factory)
		if err = conn.Raw(func(raw any) error { return factory.Register(raw.(driver.Conn), "bridge", table.name) }); err != nil {
			t.Fatal(err)
		}
	}
	return conn, factories
}

type boundRows struct {
	types  []string
	values [][]any
}

func readBoundRows(t *testing.T, conn *sql.Conn, statement string) boundRows {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), statement)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	result := boundRows{values: [][]any{}}
	for _, typ := range types {
		result.types = append(result.types, typ.DatabaseTypeName())
	}
	for rows.Next() {
		values, dest := make([]any, len(types)), make([]any, len(types))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		result.values = append(result.values, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestBoundPredicatesKeepPerRelationPlansAcrossJoins(t *testing.T) {
	schema := bridgeSchema()
	on, off := &boundObservation{}, &boundObservation{}
	reader := func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		return fixtureReader(t, schema, plan)
	}
	caps := PredicateCapabilities{Columns: []string{"id", "enabled"}}
	conn, _ := boundConnection(t,
		boundTableFixture{"selected", schema, on.wrap(reader), caps},
		boundTableFixture{"residual", schema, off.wrap(reader), PredicateCapabilities{}},
	)
	// Caller mutation must not change either the Go guard or native bound data.
	caps.Columns[0] = "label"
	got := readBoundRows(t, conn, "SELECT a.label,b.id FROM bridge.selected a JOIN bridge.residual b ON a.id=b.id WHERE a.id>=2 AND b.enabled=true ORDER BY b.id")
	if !reflect.DeepEqual(got.values, [][]any{{"row_2", int64(2)}, {"row_4", int64(4)}}) {
		t.Fatalf("join result: %#v", got)
	}
	selected, _, _ := on.snapshot()
	residual, _, _ := off.snapshot()
	if len(selected) == 0 || len(residual) == 0 {
		t.Fatal("join did not scan both relations")
	}
	pushed := false
	for _, plan := range selected {
		pushed = pushed || len(plan.Filters) != 0
	}
	if !pushed {
		t.Fatal("eligible bound relation received no predicate")
	}
	for _, plan := range residual {
		if len(plan.Filters) != 0 {
			t.Fatal("eligibility leaked between factories")
		}
	}
	self := readBoundRows(t, conn, "SELECT a.id FROM bridge.selected a JOIN bridge.selected b ON a.id=b.id WHERE a.id>=2 AND b.id<4 ORDER BY a.id")
	if !reflect.DeepEqual(self.values, [][]any{{int64(2)}, {int64(3)}}) {
		t.Fatalf("self join: %#v", self)
	}
}

func TestBoundPredicatesUseOriginalSchemaOrdinals(t *testing.T) {
	base := bridgeSchema()
	schema := arrow.NewSchema([]arrow.Field{base.Field(1), base.Field(3), base.Field(0), base.Field(2)}, nil)
	observed := &boundObservation{}
	conn, _ := boundConnection(t, boundTableFixture{"reordered", schema, observed.wrap(func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		return fixtureReader(t, schema, plan)
	}), PredicateCapabilities{Columns: []string{"id"}}})
	got := readBoundRows(t, conn, "SELECT label FROM bridge.reordered WHERE id>=2 AND label<>'row_3' ORDER BY label")
	if !reflect.DeepEqual(got.values, [][]any{{"row_2"}, {"row_4"}}) {
		t.Fatalf("reordered projection: %#v", got)
	}
	plans, rows, bytes := observed.snapshot()
	if len(plans) == 0 || rows != 3 || bytes <= 0 {
		t.Fatalf("scan evidence: plans=%d rows=%d bytes=%d", len(plans), rows, bytes)
	}
	for _, plan := range plans {
		if len(plan.Filters) == 0 {
			t.Fatal("original ordinal was lost")
		}
		if len(plan.Columns) >= 3 {
			t.Fatal("fixture did not separate original and projected ordinals")
		}
		if err := validatePredicatePlan(plan, map[string]arrow.Type{"id": arrow.INT64}); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("reordered source returned rows=%d Arrow bytes=%d", rows, bytes)
}

func TestBoundPredicatesMatchDisabledWithNullsAndResiduals(t *testing.T) {
	cases := []string{"id=2", "id<>2", "id<2", "id<=2", "id>2", "id>=2", "id IS NULL", "id IS NOT NULL", "id>=1 AND id<4", "id=0 OR id=4", "id IN (0,2,4)", "id IS NULL OR id=2"}
	for _, predicate := range cases {
		t.Run(predicate, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
			on, off := &boundObservation{}, &boundObservation{}
			reader := func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
				return optionalInReader(t, schema, plan)
			}
			conn, _ := boundConnection(t,
				boundTableFixture{"selected", schema, on.wrap(reader), PredicateCapabilities{Columns: []string{"id"}}},
				boundTableFixture{"residual", schema, off.wrap(reader), PredicateCapabilities{}},
			)
			statement := "SELECT id FROM bridge.%s WHERE " + predicate + " ORDER BY id NULLS LAST"
			a := readBoundRows(t, conn, strings.Replace(statement, "%s", "selected", 1))
			b := readBoundRows(t, conn, strings.Replace(statement, "%s", "residual", 1))
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("pushdown parity: %#v != %#v", a, b)
			}
			plans, fullRows, fullBytes := off.snapshot()
			for _, plan := range plans {
				if len(plan.Filters) != 0 {
					t.Fatal("disabled predicates reached producer")
				}
			}
			pushedPlans, reducedRows, reducedBytes := on.snapshot()
			if fullRows != 6 || reducedRows > fullRows || fullBytes <= 0 || reducedBytes < 0 {
				t.Fatal("invalid observed scan counters")
			}
			if predicate == "id IS NULL" && !reflect.DeepEqual(a.values, [][]any{{nil}}) {
				t.Fatalf("NULL predicate changed exact results: %#v", a)
			}
			// Eligibility permits pushdown; it does not force the optimizer to
			// choose it. v1.5.6 keeps IS NULL local in this query shape.
			if predicate == "id=2" || predicate == "id>=2" {
				pushed := false
				for _, plan := range pushedPlans {
					pushed = pushed || len(plan.Filters) != 0
				}
				if !pushed || reducedRows >= fullRows {
					t.Fatal("selective eligible predicate stayed local")
				}
			}
			t.Logf("on/off returned source rows=%d/%d Arrow bytes=%d/%d", reducedRows, fullRows, reducedBytes, fullBytes)
		})
	}
}

func TestBoundPredicatesRejectInvalidRegistrationAndPlans(t *testing.T) {
	schema := bridgeSchema()
	producerCalls := 0
	producer := func(context.Context, ScanPlan) (array.RecordReader, error) {
		producerCalls++
		return nil, errors.New("unexpected producer")
	}
	for _, columns := range [][]string{{"id", "id"}, {"missing"}, {"ID"}, {"label"}, make([]string, 1025)} {
		factory, err := New(context.Background(), schema, producer, PredicateCapabilities{Columns: columns})
		if factory != nil {
			factory.Close()
		}
		var typed *query.Error
		if !errors.As(err, &typed) || typed.Code != "INVALID_ARGUMENT" {
			t.Fatalf("invalid capability accepted: %v", err)
		}
	}
	factory, err := New(context.Background(), schema, producer, PredicateCapabilities{Columns: []string{"id", "enabled", "huge"}})
	if err != nil {
		t.Fatal(err)
	}
	defer factory.Close()
	valid := Filter{Kind: "comparison", Column: "id", Type: "int64", Op: "eq", Value: "2"}
	invalid := []Filter{
		{Kind: "comparison", Column: "label", Type: "string", Op: "eq", Value: "private-value"},
		{Kind: "comparison", Column: "id", Type: "int32", Op: "eq", Value: "2"},
		{Kind: "comparison", Column: "id", Type: "int64", Op: "eq", Value: "02"},
		{Kind: "comparison", Column: "id", Type: "int64", Op: "eq", Value: "9223372036854775808"},
		{Kind: "comparison", Column: "huge", Type: "uint64", Op: "eq", Value: "-1"},
		{Kind: "comparison", Column: "enabled", Type: "bool", Op: "eq", Value: "1"},
		{Kind: "is_null", Column: "id", Value: "ignored"},
		{Kind: "is_null", Column: "missing"}, {Kind: "and"},
		{Kind: "and", Column: "id", Children: []Filter{valid}},
		{Kind: "comparison", Column: "id", Type: "int64", Op: "eq", Value: "2", Children: []Filter{valid}},
		{Kind: "sql", Value: "private-value"},
	}
	deep := valid
	for range 34 {
		deep = Filter{Kind: "and", Children: []Filter{deep}}
	}
	invalid = append(invalid, deep)
	for _, filter := range invalid {
		reader, err := factory.produce(context.Background(), ScanPlan{Columns: []string{"id"}, Filters: []Filter{filter}})
		if reader != nil {
			reader.Release()
		}
		var typed *query.Error
		if !errors.As(err, &typed) || typed.Code != "UNSUPPORTED" || strings.Contains(err.Error(), "private-value") {
			t.Fatalf("invalid mandatory plan accepted or leaked: %v", err)
		}
	}
	if producerCalls != 0 {
		t.Fatal("invalid registration or plan reached producer")
	}
	disabled, err := New(context.Background(), schema, producer, PredicateCapabilities{})
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.Close()
	if _, err := disabled.produce(context.Background(), ScanPlan{Filters: []Filter{valid}}); err == nil || producerCalls != 0 {
		t.Fatal("valid but unregistered scalar predicate reached producer")
	}
	for _, count := range []int{257, 1024} {
		filters := make([]Filter, count)
		for i := range filters {
			filters[i] = valid
		}
		plans := []ScanPlan{{Filters: filters}, {Filters: []Filter{{Kind: "and", Children: filters}}}}
		if count == 1024 {
			groups := []Filter{}
			for i := 0; i < 4; i++ {
				groups = append(groups, Filter{Kind: "and", Children: filters[i*256 : (i+1)*256]})
			}
			plans = append(plans, ScanPlan{Filters: groups})
		}
		for _, plan := range plans {
			if _, err := factory.produce(context.Background(), plan); err == nil || producerCalls != 0 {
				t.Fatal("over-budget filter plan reached producer")
			}
		}
	}
}

func TestBoundPredicatesMatchDisabledAcrossNullableJoins(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	reader := func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		return optionalInReader(t, schema, plan)
	}
	off := &boundObservation{}
	conn, _ := boundConnection(t,
		boundTableFixture{"selected", schema, reader, PredicateCapabilities{Columns: []string{"id"}}},
		boundTableFixture{"residual", schema, off.wrap(reader), PredicateCapabilities{}},
	)
	for _, join := range []string{"=", "IS NOT DISTINCT FROM"} {
		statement := "SELECT a.id,b.id FROM bridge.%s a JOIN bridge.residual b ON a.id " + join + " b.id WHERE a.id>=2 OR a.id IS NULL ORDER BY a.id NULLS LAST"
		pushed := readBoundRows(t, conn, strings.Replace(statement, "%s", "selected", 1))
		local := readBoundRows(t, conn, strings.Replace(statement, "%s", "residual", 1))
		if !reflect.DeepEqual(pushed, local) {
			t.Fatalf("nullable join parity: %#v != %#v", pushed, local)
		}
		want := 3
		if join == "IS NOT DISTINCT FROM" {
			want = 4
		}
		if len(local.values) != want {
			t.Fatalf("NULL join semantics changed: %#v", local)
		}
		if want == 4 && !reflect.DeepEqual(local.values[3], []any{nil, nil}) {
			t.Fatal("NULL join row changed")
		}
	}
	plans, _, _ := off.snapshot()
	for _, plan := range plans {
		if len(plan.Filters) != 0 {
			t.Fatal("join pushed a disabled relation filter")
		}
	}
}

func TestBoundPredicatesRequireExactBoundLogicalType(t *testing.T) {
	// DuckDB v1.5.6 recognizes canonical arrow.bool8 metadata on Int8 storage.
	// The discovered storage type must not authorize a BOOLEAN bound predicate.
	metadata := arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "arrow.bool8"})
	schema := arrow.NewSchema([]arrow.Field{{Name: "flag", Type: arrow.PrimitiveTypes.Int8, Nullable: true, Metadata: metadata}}, nil)
	calls := 0
	conn, _ := boundConnection(t, boundTableFixture{"converted", schema, func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		calls++
		if len(plan.Filters) != 0 {
			return nil, errors.New("storage type authorized a different logical predicate")
		}
		builder := array.NewInt8Builder(memory.DefaultAllocator)
		defer builder.Release()
		builder.AppendValues([]int8{0, 1, 0}, []bool{true, true, false})
		column := builder.NewArray()
		defer column.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{column}, 3)
		defer record.Release()
		return array.NewRecordReader(schema, []arrow.RecordBatch{record})
	}, PredicateCapabilities{Columns: []string{"flag"}}})
	got := readBoundRows(t, conn, "SELECT flag FROM bridge.converted WHERE flag=true")
	if calls != 1 || !reflect.DeepEqual(got.types, []string{"BOOLEAN"}) || !reflect.DeepEqual(got.values, [][]any{{true}}) {
		t.Fatalf("bound logical conversion or residual changed: %#v calls=%d", got, calls)
	}
}

func TestBoundPredicatesKeepRequiredProducerFailures(t *testing.T) {
	wanted := query.NewError("UNSUPPORTED", "test producer refuses required predicate")
	calls := 0
	conn, factories := boundConnection(t, boundTableFixture{"required", bridgeSchema(), func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		calls++
		if len(plan.Filters) == 0 {
			t.Error("required filter was dropped")
		}
		return nil, wanted
	}, PredicateCapabilities{Columns: []string{"id"}}})
	rows, err := conn.QueryContext(context.Background(), "SELECT id FROM bridge.required WHERE id=2")
	if err == nil {
		for rows.Next() {
		}
		err = rows.Err()
		rows.Close()
	}
	if err == nil || !errors.Is(factories[0].Err(), wanted) || calls != 1 {
		t.Fatalf("required filter failure retried or hidden: %v calls=%d", err, calls)
	}
}
