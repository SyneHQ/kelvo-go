// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type refreshFixture struct {
	calls int
	fail  bool
	value int64
}

func (f *refreshFixture) Execute(ctx context.Context, r query.Request, sink query.Sink) (query.Stats, error) {
	f.calls++
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(f.value)
	b.AppendNull()
	a := b.NewArray()
	defer a.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{a}, 2)
	defer record.Release()
	if err := sink.Write(record); err != nil {
		return query.Stats{}, err
	}
	if f.fail {
		return query.Stats{}, errors.New("simulated partial source failure")
	}
	return query.Stats{Rows: 2}, nil
}

func managerFixture(t *testing.T) (catalog.Config, *Manager, *refreshFixture) {
	t.Helper()
	c := catalog.Config{Sources: []catalog.Source{{ID: "source", Type: "clickhouse", URLEnv: "SOURCE_PRIVATE_URL"}}, Acceleration: &catalog.AccelerationConfig{Directory: filepath.Join(t.TempDir(), "snapshots"), TenantID: "tenant-a", Datasets: []catalog.Dataset{{ID: "orders_fast", Query: query.Request{Mode: "native", ConnectionID: "source", SQL: "SELECT id FROM orders"}, RefreshInterval: time.Minute, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits()}}}}
	f := &refreshFixture{value: 7}
	m, err := NewManager(c, func(config catalog.Config, _ query.Limits) (query.Executor, error) {
		if config.Acceleration != nil {
			t.Fatal("recursive acceleration in refresh")
		}
		return f, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, m, f
}

func TestManagerPublishesCompleteSnapshotsAndReusesScheduledRefresh(t *testing.T) {
	c, m, f := managerFixture(t)
	s, err := m.Refresh(context.Background(), "orders_fast", false)
	if err != nil || s.Rows != 2 || s.Bytes < 8 {
		t.Fatalf("refresh: %+v %v", s, err)
	}
	if _, err = m.Refresh(context.Background(), "orders_fast", true); err != nil || f.calls != 1 {
		t.Fatal("scheduled redelivery repeats ingestion", err, f.calls)
	}
	sources, versions, release, err := Resolve(context.Background(), c, query.Request{Mode: "federated", Sources: []string{"orders_fast"}})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(versions) != 1 || versions[0].Generation != s.Generation || sources[0].Type != "parquet" || sources[0].URLEnv != "" {
		t.Fatal("snapshot resolution leaks origin or loses version")
	}
	f.fail = true
	if _, err = m.Refresh(context.Background(), "orders_fast", false); err == nil {
		t.Fatal("partial ingestion published")
	}
	current, err := m.Status("orders_fast")
	if err != nil || current.Generation != s.Generation {
		t.Fatal("failed refresh replaced good data", err)
	}
	f.fail = false
	for range 3 {
		if _, err = m.Refresh(context.Background(), "orders_fast", false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = os.Stat(s.Path); err != nil {
		t.Fatal("refresh pruned a query's pinned generation", err)
	}
	release()
	if err = m.store.Prune(context.Background(), "orders_fast", 2); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(s.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("retired unleased generation not reclaimed")
	}
}

func TestResolveFailsClosedForUnavailableOrChangedDatasets(t *testing.T) {
	c, m, _ := managerFixture(t)
	r := query.Request{Mode: "federated", Sources: []string{"orders_fast"}}
	if _, _, _, err := Resolve(context.Background(), c, r); err == nil {
		t.Fatal("uninitialized dataset accepted")
	}
	if _, err := m.Refresh(context.Background(), "orders_fast", false); err != nil {
		t.Fatal(err)
	}
	c.Acceleration.Datasets[0].AuthorizationVersion = "revoked-v2"
	if _, _, _, err := Resolve(context.Background(), c, r); err == nil {
		t.Fatal("revoked snapshot accepted")
	}
	c.Acceleration.Datasets[0].AuthorizationVersion = "v1"
	c.Acceleration.Datasets[0].MaxAge = time.Nanosecond
	if _, _, _, err := Resolve(context.Background(), c, r); err == nil {
		t.Fatal("stale snapshot accepted")
	}
	r = query.Request{Mode: "native", ConnectionID: "orders_fast"}
	if _, _, _, err := Resolve(context.Background(), c, r); err == nil {
		t.Fatal("native mode accepts materialized alias")
	}
}
