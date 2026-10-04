//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
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

type snapshotWorkerRows struct{}

func (snapshotWorkerRows) Execute(_ context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "tenant_id", Type: arrow.PrimitiveTypes.Int64}, {Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{7, 7, 8, 7}, nil)
	b.Field(2).(*array.Int64Builder).AppendValues([]int64{10, 20, 9000, -1}, []bool{true, false, true, true})
	record := b.NewRecordBatch()
	defer record.Release()
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	return query.Stats{Rows: 4}, sink.Write(record)
}

func snapshotWorkerPolicy(t *testing.T) context.Context {
	t.Helper()
	ctx, err := access.WithPolicy(context.Background(), access.Policy{Sources: map[string]access.SourcePolicy{
		"orders_fast": {Tables: map[string]access.TablePolicy{"orders_fast": {Columns: []string{"id", "amount"}, Rows: &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestSnapshotChildRequiresResolvedDescriptor(t *testing.T) {
	ctx := snapshotWorkerPolicy(t)
	policy, _ := access.PolicyFromContext(ctx)
	input := Input{Access: &policy, Limits: query.DefaultLimits(), Request: query.Request{Mode: "federated", Sources: []string{"orders_fast"}, SQL: "SELECT 1"}}
	for _, source := range []catalog.Source{
		{ID: "orders_fast", Type: "accelerated"},
		{ID: "orders_fast", Type: "parquet", Path: "/must-not-open"},
		{ID: "orders_fast", Type: "parquet", Path: "/must-not-open", LocalSnapshot: &catalog.LocalSnapshotRead{Dataset: "orders_fast"}},
	} {
		input.Config = catalog.Config{Sources: []catalog.Source{source}}
		if _, err := input.ExecutionContext(context.Background()); err == nil {
			t.Fatal("child accepted unresolved or malformed snapshot")
		}
	}
}

type snapshotWorkerSink struct {
	schema  *arrow.Schema
	values  [][]int64
	entered chan struct{}
	ctx     context.Context
}

func (s *snapshotWorkerSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *snapshotWorkerSink) Write(record arrow.RecordBatch) error {
	if s.entered != nil {
		close(s.entered)
		<-s.ctx.Done()
		return s.ctx.Err()
	}
	for row := 0; row < int(record.NumRows()); row++ {
		values := []int64{}
		for _, column := range record.Columns() {
			if col, ok := column.(*array.Int64); ok {
				values = append(values, col.Value(row))
			}
		}
		s.values = append(s.values, values)
	}
	return nil
}

// This gate executes the built child through the real Landlock launcher. Ordinary
// package tests keep its external binary/kernel prerequisite explicit.
func TestSnapshotWorkerSandboxPoliciesCancellationAndPinnedGeneration(t *testing.T) {
	binary, launcher := os.Getenv("KELVO_TEST_SNAPSHOT_BINARY"), os.Getenv("KELVO_TEST_SNAPSHOT_SANDBOX")
	if binary == "" || launcher == "" {
		t.Skip("set KELVO_TEST_SNAPSHOT_BINARY and KELVO_TEST_SNAPSHOT_SANDBOX for the real Linux worker gate")
	}
	config := catalog.Config{Sources: []catalog.Source{{ID: "origin", Type: "clickhouse", URLEnv: "KELVO_TEST_UNUSED_URL"}}, Acceleration: &catalog.AccelerationConfig{
		Directory: filepath.Join(t.TempDir(), "snapshots"), TenantID: "tenant-a", Datasets: []catalog.Dataset{{ID: "orders_fast", Query: query.Request{Mode: "native", ConnectionID: "origin", SQL: "SELECT fixture"}, RefreshInterval: time.Minute, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits()}},
	}}
	manager, err := acceleration.NewManager(config, func(catalog.Config, query.Limits) (query.Executor, error) { return snapshotWorkerRows{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	published, err := manager.Refresh(context.Background(), "orders_fast", false)
	if err != nil {
		t.Fatal(err)
	}
	limit := query.DefaultLimits()
	limit.Timeout = 20 * time.Second
	limit.Threads = 1
	engine, err := New(config, limit)
	if err != nil {
		t.Fatal(err)
	}
	engine.Binary, engine.SandboxPath = binary, launcher
	ctx := snapshotWorkerPolicy(t)
	request := query.Request{Sources: []string{"orders_fast"}, SQL: "SELECT count(*)::BIGINT,sum(amount)::BIGINT,count(*) FILTER(WHERE amount IS NULL)::BIGINT FROM orders_fast"}
	sink := &snapshotWorkerSink{}
	stats, err := engine.Execute(ctx, request, sink)
	if err != nil || !reflect.DeepEqual(sink.values, [][]int64{{3, 9, 1}}) {
		t.Fatalf("real policy result: %v %v", sink.values, err)
	}
	if len(stats.Accelerations) != 1 || stats.Accelerations[0].Generation != published.Generation || len(stats.Federation) != 0 {
		t.Fatal("generation binding or restricted metadata lost")
	}
	for _, sql := range []string{"SELECT tenant_id FROM orders_fast", "SELECT * FROM read_parquet('" + strings.ReplaceAll(published.Path, "'", "''") + "')"} {
		if _, err := engine.Execute(ctx, query.Request{Sources: request.Sources, SQL: sql}, &snapshotWorkerSink{}); err == nil {
			t.Fatal("sandbox policy bypass succeeded")
		}
	}
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	blocked := &snapshotWorkerSink{entered: make(chan struct{}), ctx: childCtx}
	done := make(chan error, 1)
	go func() { _, err := engine.Execute(childCtx, request, blocked); done <- err }()
	select {
	case <-blocked.entered:
	case err := <-done:
		t.Fatalf("child failed before delivery: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("child did not deliver")
	}
	for range 3 {
		if _, err := manager.Refresh(context.Background(), "orders_fast", false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(published.Path); err != nil {
		t.Fatal("in-flight generation was reclaimed", err)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || (!errors.Is(err, context.Canceled) && query.PublicError(err).Code != "CANCELLED") {
			t.Fatalf("cancellation did not fail cleanly: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled child did not exit")
	}
	backend, err := acceleration.OpenBackend(*config.Acceleration)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err := backend.Prune(context.Background(), "orders_fast", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(published.Path); !os.IsNotExist(err) {
		t.Fatal("completed child retained generation", err)
	}
	config.Acceleration.Datasets[0].AuthorizationVersion = "v2"
	engine.Config = config
	if _, err := engine.Execute(ctx, request, &snapshotWorkerSink{}); err == nil || query.PublicError(err).Code != "DATASET_UNAVAILABLE" {
		t.Fatalf("revoked generation reached child: %v", err)
	}
}
