//go:build duckdb_arrow && duckbridge && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

type predicateFixtureState struct {
	mu          sync.Mutex
	plans       []federationapi.ScanPlan
	rows, bytes int64
	active      int
}

func (s *predicateFixtureState) reset(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != 0 {
		t.Fatal("previous query leaked an adapter relation")
	}
	s.plans, s.rows, s.bytes = nil, 0, 0
}

type predicateFixtureDriver struct{ state *predicateFixtureState }

func (*predicateFixtureDriver) Validate(federationapi.Source, federationapi.Table) error { return nil }
func (d *predicateFixtureDriver) Open(context.Context, federationapi.Source, federationapi.Table, federationapi.Limits) (federationapi.Relation, error) {
	d.state.mu.Lock()
	d.state.active++
	d.state.mu.Unlock()
	return &predicateFixtureRelation{state: d.state}, nil
}

type predicateDeclaredFixture struct {
	*predicateFixtureDriver
	mode string
}

func (d *predicateDeclaredFixture) FederationCapabilities() federationapi.Capabilities {
	declaration := federationapi.Capabilities{Version: federationapi.CapabilityVersion,
		Projection: true, NullPredicates: true, Conjunction: true, Disjunction: true,
		Comparisons: []federationapi.ComparisonCapability{{Type: "int64", Operators: []string{"eq", "ne", "lt", "le", "gt", "ge"}}}}
	switch d.mode {
	case "new":
		declaration.Version++
	case "invalid":
		declaration.Comparisons[0].Operators = []string{"unsupported"}
	case "panic":
		panic("broken advisory capability method")
	case "partial":
		declaration.Comparisons[0].Operators = []string{"eq"}
	}
	return declaration
}

var predicateFixtures = map[string]*predicateFixtureState{}

func init() {
	for _, mode := range []string{"absent", "new", "invalid", "panic", "partial", "full"} {
		state := &predicateFixtureState{}
		predicateFixtures[mode] = state
		base := &predicateFixtureDriver{state: state}
		var driver federationapi.Driver = base
		if mode != "absent" {
			driver = &predicateDeclaredFixture{predicateFixtureDriver: base, mode: mode}
		}
		federationapi.MustRegister("predicate_fixture_"+mode, driver)
	}
}

type predicateFixtureRelation struct{ state *predicateFixtureState }

func (*predicateFixtureRelation) Schema() *arrow.Schema {
	metadata := arrow.MetadataFrom(map[string]string{"fixture-private": "policy metadata"})
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata},
		{Name: "tenant_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	}, &metadata)
}

func (r *predicateFixtureRelation) Close() error {
	r.state.mu.Lock()
	r.state.active--
	r.state.mu.Unlock()
	return nil
}

func (r *predicateFixtureRelation) Scan(ctx context.Context, plan federationapi.ScanPlan, sink federationapi.Sink) (federationapi.ScanStats, error) {
	r.state.mu.Lock()
	r.state.plans = append(r.state.plans, federationapi.ScanPlan{Columns: slices.Clone(plan.Columns), Filters: slices.Clone(plan.Filters)})
	r.state.mu.Unlock()
	if len(plan.Filters) != 0 {
		return federationapi.ScanStats{}, federationapi.ErrUnsupported
	}
	if err := ctx.Err(); err != nil {
		return federationapi.ScanStats{}, err
	}
	full := r.Schema()
	fields := make([]arrow.Field, len(plan.Columns))
	for i, name := range plan.Columns {
		indices := full.FieldIndices(name)
		if len(indices) != 1 {
			return federationapi.ScanStats{}, federationapi.ErrUnsupported
		}
		fields[i] = full.Field(indices[0])
	}
	metadata := full.Metadata()
	schema := arrow.NewSchema(fields, &metadata)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for i, name := range plan.Columns {
		switch name {
		case "id":
			builder.Field(i).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
		case "tenant_id":
			builder.Field(i).(*array.Int64Builder).AppendValues([]int64{7, 7, 8, 7, 8}, nil)
		case "amount":
			builder.Field(i).(*array.Int64Builder).AppendValues([]int64{10, 0, 9000, 20, -1}, []bool{true, false, true, true, true})
		case "label":
			builder.Field(i).(*array.StringBuilder).AppendValues([]string{"a", "", "x", "z", "b"}, []bool{true, false, true, true, true})
		}
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	r.state.mu.Lock()
	r.state.rows += record.NumRows()
	r.state.bytes += arrowutil.TotalRecordSize(record)
	r.state.mu.Unlock()
	if err := sink.Schema(schema); err != nil {
		return federationapi.ScanStats{}, err
	}
	return federationapi.ScanStats{}, sink.Write(record)
}

type predicateResultSink struct {
	schema *arrow.Schema
	values [][]any
}

func (s *predicateResultSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *predicateResultSink) Write(record arrow.RecordBatch) error {
	for row := 0; row < int(record.NumRows()); row++ {
		values := make([]any, record.NumCols())
		for i, column := range record.Columns() {
			if column.IsNull(row) {
				continue
			}
			switch column := column.(type) {
			case *array.Int64:
				values[i] = column.Value(row)
			case *array.String:
				values[i] = strings.Clone(column.Value(row))
			default:
				return fmt.Errorf("unexpected result type %s", column.DataType())
			}
		}
		s.values = append(s.values, values)
	}
	return nil
}

func predicateEngine(t *testing.T, mode string, maxRows int64) *Engine {
	t.Helper()
	limits := query.DefaultLimits()
	limits.Threads, limits.Timeout = 1, 10*time.Second
	source := catalog.Source{ID: "warehouse", Type: "predicate_fixture_" + mode,
		Federation: &catalog.FederationConfig{MaxScanRows: maxRows,
			Tables: []catalog.FederationTable{{Name: "events", Table: "events"}}}}
	engine, err := New(catalog.Config{Sources: []catalog.Source{source}}, limits)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

const predicateQuery = "SELECT id,amount,label FROM warehouse.events WHERE (amount>=10 OR amount IS NULL) AND id<5 AND (label<>'excluded' OR label IS NULL) ORDER BY id"

func assertPredicateResult(t *testing.T, sink *predicateResultSink, want [][]any) {
	t.Helper()
	if !reflect.DeepEqual(sink.values, want) {
		t.Fatalf("rows = %v, want %v", sink.values, want)
	}
	if sink.schema == nil || sink.schema.NumFields() != 3 {
		t.Fatal("result schema missing")
	}
	for i, expected := range []arrow.Type{arrow.INT64, arrow.INT64, arrow.STRING} {
		if sink.schema.Field(i).Type.ID() != expected {
			t.Fatalf("result type %d = %s", i, sink.schema.Field(i).Type)
		}
	}
}

func TestProjectionOnlyCustomAdaptersKeepQueryPredicatesLocal(t *testing.T) {
	for _, mode := range []string{"absent", "new", "invalid", "panic", "partial", "full"} {
		t.Run(mode, func(t *testing.T) {
			state := predicateFixtures[mode]
			state.reset(t)
			engine := predicateEngine(t, mode, 0)
			sink := &predicateResultSink{}
			stats, err := engine.Execute(context.Background(), query.Request{Mode: "federated",
				Sources: []string{"warehouse"}, SQL: predicateQuery, ScanDiagnostics: true}, sink)
			if err != nil {
				t.Fatal(err)
			}
			assertPredicateResult(t, sink, [][]any{{int64(1), int64(10), "a"}, {int64(2), nil, nil}, {int64(3), int64(9000), "x"}, {int64(4), int64(20), "z"}})
			state.mu.Lock()
			defer state.mu.Unlock()
			if state.active != 0 || len(state.plans) != 1 || len(state.plans[0].Filters) != 0 {
				t.Fatalf("adapter ownership or predicate contract changed: active=%d plans=%+v", state.active, state.plans)
			}
			if state.rows != 5 || state.bytes <= 0 || len(stats.Federation) != 1 || stats.Federation[0].Rows != state.rows || stats.Federation[0].Bytes != state.bytes {
				t.Fatalf("source rows/Arrow bytes were not measured: state=%d/%d stats=%+v", state.rows, state.bytes, stats.Federation)
			}
			if stats.ScanDiagnostics == nil || len(stats.ScanDiagnostics.Scans) != 1 {
				t.Fatal("executed source scan diagnostics missing")
			}
			diagnostic := stats.ScanDiagnostics.Scans[0]
			if len(diagnostic.Predicates) != 0 || diagnostic.ResidualVisibility != "not_observed" || diagnostic.Rows != state.rows || diagnostic.ArrowBytes != state.bytes || diagnostic.Outcome != "success" {
				t.Fatalf("source diagnostics misreported local filters: %+v", diagnostic)
			}
			if diagnostic.CapabilitiesKnown != (mode == "partial" || mode == "full") {
				t.Fatal("advisory declaration visibility changed")
			}
		})
	}
}

func TestProjectionOnlyAdapterRejectsAnExplicitRequiredFilter(t *testing.T) {
	state := &predicateFixtureState{}
	relation := &predicateFixtureRelation{state: state}
	sink := &predicateResultSink{}
	_, err := relation.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"id"},
		Filters: []federationapi.Filter{{Kind: "comparison", Column: "id", Type: "int64", Op: "eq", Value: "1"}}}, sink)
	if !errors.Is(err, federationapi.ErrUnsupported) || sink.schema != nil || len(sink.values) != 0 || state.rows != 0 {
		t.Fatal("required adapter predicate was silently dropped", err)
	}
}

func TestGuardedCustomEligibilityPreservesPolicyAndRawScanBudget(t *testing.T) {
	policy := func(tenant string) context.Context {
		ctx, err := access.WithPolicy(context.Background(), access.Policy{Sources: map[string]access.SourcePolicy{
			"warehouse": {Tables: map[string]access.TablePolicy{"events": {
				Columns: []string{"id", "amount", "label"},
				Rows:    &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: tenant},
			}}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		return ctx
	}
	state := predicateFixtures["full"]
	state.reset(t)
	engine := predicateEngine(t, "full", 0)
	sink := &predicateResultSink{}
	stats, err := engine.Execute(policy("7"), query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: predicateQuery}, sink)
	if err != nil {
		t.Fatal(err)
	}
	assertPredicateResult(t, sink, [][]any{{int64(1), int64(10), "a"}, {int64(2), nil, nil}, {int64(4), int64(20), "z"}})
	if sink.schema.Metadata().Len() != 0 || stats.Federation != nil || stats.ScanDiagnostics != nil || stats.SourceWireBytes != 0 {
		t.Fatal("pre-policy metadata or counters escaped")
	}
	for _, field := range sink.schema.Fields() {
		if field.Name == "tenant_id" || field.Metadata.Len() != 0 {
			t.Fatal("hidden policy schema escaped")
		}
	}
	state.mu.Lock()
	clean := state.active == 0 && len(state.plans) == 1 && len(state.plans[0].Filters) == 0 && slices.Contains(state.plans[0].Columns, "tenant_id")
	state.mu.Unlock()
	if !clean {
		t.Fatal("guard did not own policy/query filtering and source cleanup")
	}
	for _, sql := range []string{"SELECT tenant_id FROM warehouse.events", "SELECT id FROM warehouse.events WHERE tenant_id=8"} {
		output := &predicateResultSink{}
		if _, err := engine.Execute(policy("7"), query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: sql}, output); err == nil || len(output.values) != 0 {
			t.Fatal("hidden policy column was queryable")
		}
	}
	state.reset(t)
	limited := predicateEngine(t, "full", 4)
	output := &predicateResultSink{}
	_, err = limited.Execute(policy("999"), query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: predicateQuery}, output)
	var public *query.Error
	if !errors.As(err, &public) || public.Code != "RESOURCE_EXHAUSTED" || len(output.values) != 0 {
		t.Fatal("policy-discarded source rows escaped the raw scan budget", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.rows != 5 || state.active != 0 || len(state.plans) != 1 || len(state.plans[0].Filters) != 0 {
		t.Fatal("raw budget refusal did not preserve source accounting and cleanup")
	}
}
