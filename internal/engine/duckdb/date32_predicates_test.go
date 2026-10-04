//go:build linux && duckdb_arrow && duckbridge && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

var date32FixtureState = &predicateFixtureState{}

type date32FixtureDriver struct{}

func (date32FixtureDriver) Validate(federationapi.Source, federationapi.Table) error { return nil }
func (date32FixtureDriver) Open(context.Context, federationapi.Source, federationapi.Table, federationapi.Limits) (federationapi.Relation, error) {
	date32FixtureState.mu.Lock()
	date32FixtureState.active++
	date32FixtureState.mu.Unlock()
	return &date32FixtureRelation{}, nil
}
func (date32FixtureDriver) FederationCapabilities() federationapi.Capabilities {
	// An advisory custom declaration cannot enable mandatory Date32 filters.
	return federationapi.Capabilities{Version: federationapi.CapabilityVersion,
		Projection: true, NullPredicates: true, Conjunction: true, Disjunction: true,
		Comparisons: []federationapi.ComparisonCapability{{Type: "date32", Operators: []string{"eq", "ne", "lt", "le", "gt", "ge"}}}}
}

func init() { federationapi.MustRegister("date32_fixture", date32FixtureDriver{}) }

type date32FixtureRelation struct{}

func date32FixtureSchema() *arrow.Schema {
	metadata := arrow.MetadataFrom(map[string]string{"private-fixture": "date-policy-metadata"})
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata},
		{Name: "tenant_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "event_day", Type: arrow.FixedWidthTypes.Date32, Nullable: true, Metadata: metadata},
		{Name: "date64", Type: arrow.FixedWidthTypes.Date64, Nullable: true},
	}, &metadata)
}
func (*date32FixtureRelation) Schema() *arrow.Schema { return date32FixtureSchema() }
func (*date32FixtureRelation) Close() error {
	date32FixtureState.mu.Lock()
	date32FixtureState.active--
	date32FixtureState.mu.Unlock()
	return nil
}
func date32FixtureRecord(schema *arrow.Schema) arrow.RecordBatch {
	return date32FixtureRecordWithDays(schema, []arrow.Date32{-1, 0, 11016, 19782, 0, -2147483647, 2147483647})
}
func date32FixtureRecordWithDays(schema *arrow.Schema, days []arrow.Date32) arrow.RecordBatch {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for i, field := range schema.Fields() {
		switch field.Name {
		case "id":
			builder.Field(i).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5, 6, 7}, nil)
		case "tenant_id":
			builder.Field(i).(*array.Int64Builder).AppendValues([]int64{7, 7, 8, 7, 7, 8, 7}, nil)
		case "event_day":
			builder.Field(i).(*array.Date32Builder).AppendValues(days, []bool{true, true, true, true, false, true, true})
		case "date64":
			builder.Field(i).(*array.Date64Builder).AppendValues([]arrow.Date64{-86400000, 0, 11016 * 86400000, 19782 * 86400000, 0, -100000 * 86400000, 100000 * 86400000}, []bool{true, true, true, true, false, true, true})
		}
	}
	return builder.NewRecordBatch()
}
func (*date32FixtureRelation) Scan(ctx context.Context, plan federationapi.ScanPlan, sink federationapi.Sink) (federationapi.ScanStats, error) {
	date32FixtureState.mu.Lock()
	date32FixtureState.plans = append(date32FixtureState.plans, federationapi.ScanPlan{Columns: slices.Clone(plan.Columns), Filters: slices.Clone(plan.Filters)})
	date32FixtureState.mu.Unlock()
	if len(plan.Filters) != 0 {
		return federationapi.ScanStats{}, federationapi.ErrUnsupported
	}
	if err := ctx.Err(); err != nil {
		return federationapi.ScanStats{}, err
	}
	full := date32FixtureSchema()
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
	record := date32FixtureRecord(schema)
	defer record.Release()
	date32FixtureState.mu.Lock()
	date32FixtureState.rows += record.NumRows()
	date32FixtureState.bytes += arrowutil.TotalRecordSize(record)
	date32FixtureState.mu.Unlock()
	if err := sink.Schema(schema); err != nil {
		return federationapi.ScanStats{}, err
	}
	return federationapi.ScanStats{}, sink.Write(record)
}

func date32FixtureEngine(t *testing.T, maxRows int64) *Engine {
	t.Helper()
	limits := query.DefaultLimits()
	limits.Threads, limits.Timeout = 1, 10*time.Second
	source := catalog.Source{ID: "warehouse", Type: "date32_fixture", Federation: &catalog.FederationConfig{
		MaxScanRows: maxRows, Tables: []catalog.FederationTable{{Name: "events", Table: "events"}},
	}}
	engine, err := New(catalog.Config{Sources: []catalog.Source{source}}, limits)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}
func date32Policy(t *testing.T, source, table, tenant string) context.Context {
	t.Helper()
	ctx, err := access.WithPolicy(context.Background(), access.Policy{Sources: map[string]access.SourcePolicy{
		source: {Tables: map[string]access.TablePolicy{table: {
			Columns: []string{"id", "event_day"},
			Rows:    &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: tenant},
		}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}
func assertDate32IDs(t *testing.T, sink *predicateResultSink, ids ...int64) {
	t.Helper()
	want := make([][]any, len(ids))
	for i, id := range ids {
		want[i] = []any{id}
	}
	rowsEqual := len(sink.values) == len(want) && (len(want) == 0 || reflect.DeepEqual(sink.values, want))
	if !rowsEqual || sink.schema == nil || sink.schema.NumFields() != 1 || sink.schema.Field(0).Type.ID() != arrow.INT64 {
		t.Fatalf("Date32 query rows = %v, want %v", sink.values, want)
	}
}
func assertDate32CustomScansClosed(t *testing.T, policyColumn bool) {
	t.Helper()
	date32FixtureState.mu.Lock()
	defer date32FixtureState.mu.Unlock()
	if date32FixtureState.active != 0 || len(date32FixtureState.plans) != 1 || date32FixtureState.rows != 7 || date32FixtureState.bytes == 0 {
		t.Fatalf("Date32 source scan accounting/ownership changed: active=%d rows=%d plans=%v", date32FixtureState.active, date32FixtureState.rows, date32FixtureState.plans)
	}
	plan := date32FixtureState.plans[0]
	if len(plan.Filters) != 0 || (policyColumn && !slices.Contains(plan.Columns, "tenant_id")) {
		t.Fatalf("Date32 predicate escaped local evaluation or policy input was lost: %+v", plan)
	}
}

func TestDate32CustomPredicatesRemainLocalInActualEngine(t *testing.T) {
	engine := date32FixtureEngine(t, 0)
	cases := []struct {
		where string
		ids   []int64
	}{
		{"event_day >= DATE '1970-01-01' OR event_day IS NULL", []int64{2, 3, 4, 5, 7}},
		{"event_day > DATE '-infinity' AND event_day < DATE 'infinity'", []int64{1, 2, 3, 4}},
		{"event_day = DATE 'infinity'", []int64{7}},
		{"event_day = DATE '-infinity'", []int64{6}},
		{"date64 >= DATE '1970-01-01'", []int64{2, 3, 4, 7}},
	}
	for _, tc := range cases {
		t.Run(tc.where, func(t *testing.T) {
			date32FixtureState.reset(t)
			sink := &predicateResultSink{}
			stats, err := engine.Execute(context.Background(), query.Request{Mode: "federated", Sources: []string{"warehouse"},
				SQL: "SELECT id FROM warehouse.events WHERE " + tc.where + " ORDER BY id", ScanDiagnostics: true}, sink)
			if err != nil {
				t.Fatal(err)
			}
			assertDate32IDs(t, sink, tc.ids...)
			assertDate32CustomScansClosed(t, false)
			if len(stats.Federation) != 1 || stats.Federation[0].Rows != 7 || stats.ScanDiagnostics == nil || len(stats.ScanDiagnostics.Scans) != 1 || len(stats.ScanDiagnostics.Scans[0].Predicates) != 0 {
				t.Fatal("custom Date32 query misreported local predicates or raw rows")
			}
		})
	}
}

func TestDate32GuardedCustomPreservesPolicyAndRawBudget(t *testing.T) {
	date32FixtureState.reset(t)
	engine := date32FixtureEngine(t, 0)
	ctx := date32Policy(t, "warehouse", "events", "7")
	sink := &predicateResultSink{}
	request := query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: "SELECT id FROM warehouse.events WHERE event_day >= DATE '1970-01-01' OR event_day IS NULL ORDER BY id"}
	stats, err := engine.Execute(ctx, request, sink)
	if err != nil {
		t.Fatal(err)
	}
	assertDate32IDs(t, sink, 2, 4, 5, 7)
	assertDate32CustomScansClosed(t, true)
	if sink.schema.Metadata().Len() != 0 || sink.schema.Field(0).Metadata.Len() != 0 || stats.Federation != nil || stats.ScanDiagnostics != nil || stats.SourceWireBytes != 0 {
		t.Fatal("guarded Date32 query exposed private schema or raw statistics")
	}
	for _, sql := range []string{"SELECT tenant_id FROM warehouse.events WHERE event_day=DATE '1970-01-01'", "SELECT id FROM warehouse.events WHERE tenant_id=8 AND event_day=DATE '2000-02-29'"} {
		output := &predicateResultSink{}
		if _, err := engine.Execute(ctx, query.Request{Sources: request.Sources, SQL: sql}, output); err == nil || len(output.values) != 0 {
			t.Fatal("Date32 residual exposed a hidden policy column")
		}
	}
	date32FixtureState.reset(t)
	limited := date32FixtureEngine(t, 6)
	output := &predicateResultSink{}
	_, err = limited.Execute(date32Policy(t, "warehouse", "events", "999"), request, output)
	var public *query.Error
	if !errors.As(err, &public) || public.Code != "RESOURCE_EXHAUSTED" || len(output.values) != 0 {
		t.Fatal("Date32 local/policy filters hid source rows from the raw budget", err)
	}
	assertDate32CustomScansClosed(t, true)
}

type date32SnapshotRows struct{}

func (date32SnapshotRows) Execute(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	full := date32FixtureSchema()
	metadata := full.Metadata()
	// Persisted snapshots require finite dates. Custom/native fixtures retain
	// infinity coverage; this fixture still compares finite data with infinity.
	schema := arrow.NewSchema(full.Fields()[:3], &metadata)
	record := date32FixtureRecordWithDays(schema, []arrow.Date32{-1, 0, 11016, 19782, 0, -100000, 100000})
	defer record.Release()
	if err := ctx.Err(); err != nil {
		return query.Stats{}, err
	}
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	return query.Stats{Rows: record.NumRows()}, sink.Write(record)
}

func TestDate32GuardedSnapshotPredicatesStayLocalInActualEngine(t *testing.T) {
	config := catalog.Config{
		Sources: []catalog.Source{{ID: "origin", Type: "clickhouse", URLEnv: "KELVO_TEST_UNUSED_DATE32_URL"}},
		Acceleration: &catalog.AccelerationConfig{Directory: filepath.Join(t.TempDir(), "snapshots"), TenantID: "tenant-a",
			Datasets: []catalog.Dataset{{ID: "dates_fast", Query: query.Request{Mode: "native", ConnectionID: "origin", SQL: "SELECT fixture"},
				RefreshInterval: time.Minute, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits()}},
		},
	}
	manager, err := acceleration.NewManager(config, func(catalog.Config, query.Limits) (query.Executor, error) { return date32SnapshotRows{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if _, err := manager.Refresh(context.Background(), "dates_fast", false); err != nil {
		t.Fatal(err)
	}
	sources, _, release, err := acceleration.Resolve(context.Background(), config, query.Request{Sources: []string{"dates_fast"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	limits := query.DefaultLimits()
	limits.Threads, limits.Timeout = 1, 10*time.Second
	engine, err := New(catalog.Config{Sources: sources}, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx := date32Policy(t, "dates_fast", "dates_fast", "7")
	cases := []struct {
		sql string
		ids []int64
	}{
		{"SELECT id FROM dates_fast WHERE event_day >= DATE '1970-01-01' OR event_day IS NULL ORDER BY id", []int64{2, 4, 5, 7}},
		{"WITH finite AS (SELECT id FROM dates_fast WHERE event_day > DATE '-infinity' AND event_day < DATE 'infinity') SELECT id FROM finite ORDER BY id", []int64{1, 2, 4, 7}},
		{"SELECT id FROM dates_fast WHERE event_day = DATE 'infinity'", nil},
	}
	for _, tc := range cases {
		sink := &predicateResultSink{}
		stats, err := engine.Execute(ctx, query.Request{Sources: []string{"dates_fast"}, SQL: tc.sql}, sink)
		if err != nil {
			t.Fatal(err)
		}
		assertDate32IDs(t, sink, tc.ids...)
		if stats.Federation != nil || stats.SourceWireBytes != 0 || stats.ScanDiagnostics != nil || sink.schema.Metadata().Len() != 0 || sink.schema.Field(0).Metadata.Len() != 0 {
			t.Fatal("guarded Date32 snapshot exposed private schema or raw statistics")
		}
	}
	output := &predicateResultSink{}
	if _, err := engine.Execute(ctx, query.Request{Sources: []string{"dates_fast"}, SQL: "SELECT tenant_id FROM dates_fast WHERE event_day=DATE '1970-01-01'"}, output); err == nil || len(output.values) != 0 {
		t.Fatal("snapshot Date32 residual exposed a hidden policy column")
	}
	// The source contains seven rows even when the policy rejects all of them.
	engine.config.Sources[0].LocalSnapshot.Scan.MaxRows = 6
	output = &predicateResultSink{}
	_, err = engine.Execute(date32Policy(t, "dates_fast", "dates_fast", "999"), query.Request{Sources: []string{"dates_fast"}, SQL: cases[0].sql}, output)
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" || len(output.values) != 0 {
		t.Fatal("snapshot Date32 predicates bypassed raw scan budget", err)
	}
	if _, err := os.Stat(sources[0].Path); err != nil {
		t.Fatal("Date32 query/error removed its pinned snapshot generation", err)
	}
}
