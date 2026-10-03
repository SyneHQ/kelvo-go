//go:build linux && duckdb_arrow && duckbridge && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type snapshotAccessRows struct{}

func (snapshotAccessRows) Execute(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	meta := arrow.MetadataFrom(map[string]string{"private-schema": "snapshot-private-metadata"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: meta},
		{Name: "tenant_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "secret", Type: arrow.BinaryTypes.String},
	}, &meta)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{7, 7, 8, 7, 8}, nil)
	b.Field(2).(*array.Int64Builder).AppendValues([]int64{10, 20, 9000, -1, 8000}, []bool{true, false, true, true, true})
	b.Field(3).(*array.StringBuilder).AppendValues([]string{"private", "private", "private", "private", "private"}, nil)
	record := b.NewRecordBatch()
	defer record.Release()
	if err := ctx.Err(); err != nil {
		return query.Stats{}, err
	}
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	return query.Stats{Rows: 5}, sink.Write(record)
}

func snapshotAccessFixture(t *testing.T) (*Engine, context.Context, catalog.Config, catalog.Source, *acceleration.Manager, func()) {
	t.Helper()
	config := catalog.Config{
		Sources: []catalog.Source{{ID: "origin", Type: "clickhouse", URLEnv: "KELVO_TEST_UNUSED_URL"}},
		Acceleration: &catalog.AccelerationConfig{
			Directory: filepath.Join(t.TempDir(), "snapshots"), TenantID: "tenant-a",
			Datasets: []catalog.Dataset{{ID: "orders_fast", Query: query.Request{Mode: "native", ConnectionID: "origin", SQL: "SELECT fixture"},
				RefreshInterval: time.Minute, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits()}},
		},
	}
	manager, err := acceleration.NewManager(config, func(catalog.Config, query.Limits) (query.Executor, error) { return snapshotAccessRows{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if _, err = manager.Refresh(context.Background(), "orders_fast", false); err != nil {
		t.Fatal(err)
	}
	sources, _, release, err := acceleration.Resolve(context.Background(), config, query.Request{Mode: "federated", Sources: []string{"orders_fast"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	policy := access.Policy{Sources: map[string]access.SourcePolicy{"orders_fast": {Tables: map[string]access.TablePolicy{
		"orders_fast": {Columns: []string{"id", "amount"}, Rows: &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"}},
	}}}}
	ctx, err := access.WithPolicy(context.Background(), policy)
	if err != nil {
		t.Fatal(err)
	}
	limit := query.DefaultLimits()
	limit.Timeout = 10 * time.Second
	limit.Threads = 2
	engine, err := New(catalog.Config{Sources: sources}, limit)
	if err != nil {
		t.Fatal(err)
	}
	return engine, ctx, config, sources[0], manager, release
}

func TestSnapshotPolicyPrecedesCTEsJoinsWindowsAndExpressions(t *testing.T) {
	engine, ctx, _, source, _, _ := snapshotAccessFixture(t)
	cases := []struct {
		sql  string
		want [][]int64
	}{
		{"SELECT count(*)::BIGINT,sum(amount)::BIGINT FROM orders_fast", [][]int64{{3, 9}}},
		{"WITH positive AS (SELECT * FROM orders_fast WHERE amount>0) SELECT count(*)::BIGINT,sum(amount)::BIGINT FROM positive", [][]int64{{1, 10}}},
		{"SELECT count(*)::BIGINT FROM main.orders_fast a JOIN main.orders_fast b USING(id)", [][]int64{{3}}},
		{"SELECT sum(CASE WHEN amount=9000 THEN error('hidden row reached SQL') ELSE amount END)::BIGINT FROM orders_fast", [][]int64{{9}}},
		{"SELECT id::BIGINT,row_number() OVER(ORDER BY id)::BIGINT FROM orders_fast ORDER BY id", [][]int64{{1, 1}, {2, 2}, {4, 3}}},
		{"SELECT count(*)::BIGINT FROM orders_fast WHERE amount IS NULL", [][]int64{{1}}},
		{"SELECT count(*)::BIGINT FROM information_schema.columns WHERE column_name IN ('tenant_id','secret')", [][]int64{{0}}},
		{"SELECT count(*)::BIGINT FROM duckdb_views() WHERE sql LIKE '%" + quoteLiteral(source.Path) + "%'", [][]int64{{0}}},
	}
	for _, tc := range cases {
		sink := &accessValueSink{}
		stats, err := engine.Execute(ctx, query.Request{Sources: []string{"orders_fast"}, SQL: tc.sql}, sink)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if !reflect.DeepEqual(sink.values, tc.want) {
			t.Fatalf("%s: got%v want%v", tc.sql, sink.values, tc.want)
		}
		if stats.Federation != nil || stats.SourceWireBytes != 0 || stats.ScanDiagnostics != nil {
			t.Fatal("raw policy statistics escaped")
		}
	}
}

func TestSnapshotPolicyJoinsGuardedRemoteRelation(t *testing.T) {
	engine, ctx, _, _, _, _ := snapshotAccessFixture(t)
	policy, _ := access.PolicyFromContext(ctx)
	policy.Sources["warehouse"] = access.SourcePolicy{Tables: map[string]access.TablePolicy{
		"customers": {Columns: []string{"id"}, Rows: &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"}},
	}}
	ctx, err := access.WithPolicy(context.Background(), policy)
	if err != nil {
		t.Fatal(err)
	}
	engine.config.Sources = append(engine.config.Sources, catalog.Source{ID: "warehouse", Type: "access_fixture", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "customers", Table: "customers"}}}})
	sink := &accessValueSink{}
	_, err = engine.Execute(ctx, query.Request{Sources: []string{"orders_fast", "warehouse"}, SQL: "SELECT count(*)::BIGINT,sum(o.amount)::BIGINT FROM orders_fast o JOIN warehouse.customers c USING(id)"}, sink)
	if err != nil || !reflect.DeepEqual(sink.values, [][]int64{{2, 9}}) {
		t.Fatalf("mixed guarded join: %v %v", sink.values, err)
	}
}

func TestSnapshotPolicyRejectsDirectReadersAndHiddenColumns(t *testing.T) {
	engine, ctx, _, source, _, _ := snapshotAccessFixture(t)
	for _, sql := range []string{
		"SELECT tenant_id FROM orders_fast", "SELECT secret FROM main.orders_fast", "SELECT * FROM orders_fast WHERE tenant_id=8",
		"SELECT * FROM read_parquet('" + quoteLiteral(source.Path) + "')", "SELECT * FROM parquet_scan('" + quoteLiteral(source.Path) + "')",
		"SELECT * FROM read_blob('" + quoteLiteral(source.Path) + "')",
		"SELECT * FROM parquet_metadata('" + quoteLiteral(source.Path) + "')",
		"SELECT * FROM parquet_schema('" + quoteLiteral(source.Path) + "')", "SELECT * FROM kelvo_arrow_scan_1(NULL,NULL,NULL)",
	} {
		sink := &accessValueSink{}
		_, err := engine.Execute(ctx, query.Request{Sources: []string{"orders_fast"}, SQL: sql}, sink)
		if err == nil || sink.rows != 0 {
			t.Fatalf("raw access succeeded: %s", sql)
		}
		if strings.Contains(err.Error(), source.Path) || strings.Contains(err.Error(), "snapshot-private-metadata") {
			t.Fatal("private error detail escaped")
		}
	}
	sink := &accessValueSink{}
	if _, err := engine.Execute(ctx, query.Request{Sources: []string{"orders_fast"}, SQL: "SELECT * FROM orders_fast"}, sink); err != nil {
		t.Fatal(err)
	}
	if sink.schema.NumFields() != 2 || sink.schema.Metadata().Len() != 0 {
		t.Fatal("hidden schema exposed")
	}
	for _, field := range sink.schema.Fields() {
		if field.Metadata.Len() != 0 {
			t.Fatal("private field metadata exposed")
		}
	}
	source.LocalSnapshot = nil
	engine.config.Sources = []catalog.Source{source}
	if _, err := engine.Execute(ctx, query.Request{Sources: []string{"orders_fast"}, SQL: "SELECT 1"}, &accessValueSink{}); err == nil {
		t.Fatal("descriptor-free policy source accepted")
	}
}

func TestSnapshotPolicyRawBudgetAndGenerationRevocation(t *testing.T) {
	engine, ctx, config, source, manager, release := snapshotAccessFixture(t)
	descriptor := *source.LocalSnapshot
	descriptor.Scan.MaxRows = 4
	source.LocalSnapshot = &descriptor
	engine.config.Sources = []catalog.Source{source}
	_, err := engine.Execute(ctx, query.Request{Sources: []string{"orders_fast"}, SQL: "SELECT count(*) FROM orders_fast"}, &accessValueSink{})
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("hidden rows bypassed raw limit: %v", err)
	}
	config.Acceleration.Datasets[0].AuthorizationVersion = "revoked-v2"
	if _, _, done, err := acceleration.Resolve(ctx, config, query.Request{Sources: []string{"orders_fast"}}); err == nil {
		done()
		t.Fatal("revoked generation admitted")
	}
	// A pinned generation remains present across publication/pruning, then becomes
	// reclaimable only after the query owner releases it.
	for range 3 {
		if _, err := manager.Refresh(context.Background(), "orders_fast", false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(source.Path); err != nil {
		t.Fatal("pinned generation removed", err)
	}
	release()
	backend, err := acceleration.OpenBackend(*config.Acceleration)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err := backend.Prune(context.Background(), "orders_fast", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(source.Path); !os.IsNotExist(err) {
		t.Fatalf("released generation not pruned: %v", err)
	}
}

func TestSnapshotPoliciesCountTowardCombinedTableLimit(t *testing.T) {
	engine, ctx, _, source, _, _ := snapshotAccessFixture(t)
	policy := access.Policy{Sources: map[string]access.SourcePolicy{}}
	sources := []catalog.Source{}
	for i := range 32 {
		copied := source
		copied.ID = fmt.Sprintf("snapshot_%d", i)
		descriptor := *source.LocalSnapshot
		descriptor.Dataset = copied.ID
		copied.LocalSnapshot = &descriptor
		sources = append(sources, copied)
		policy.Sources[copied.ID] = access.SourcePolicy{Tables: map[string]access.TablePolicy{copied.ID: {Columns: []string{"id"}, AllRows: true}}}
	}
	var err error
	ctx, err = access.WithPolicy(context.Background(), policy)
	if err != nil {
		t.Fatal(err)
	}
	engine.config.Sources = append(sources, catalog.Source{ID: "warehouse", Type: "access_fixture", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "customers", Table: "customers"}}}})
	selected := make([]string, len(engine.config.Sources))
	for i, source := range engine.config.Sources {
		selected[i] = source.ID
	}
	_, err = engine.Execute(ctx, query.Request{Sources: selected, SQL: "SELECT 1"}, &accessValueSink{})
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("combined snapshot table count not bounded: %v", err)
	}
}
