//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// deltaSource serves ids, honouring the wrapped `"id" >= N` predicate the way a
// real source would, and records every SQL it received.
type deltaSource struct {
	ids      []int64
	extra    bool
	requests []string
}

func (f *deltaSource) Execute(_ context.Context, r query.Request, sink query.Sink) (query.Stats, error) {
	f.requests = append(f.requests, r.SQL)
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}
	if f.extra {
		fields = append(fields, arrow.Field{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true})
	}
	schema := arrow.NewSchema(fields, nil)
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	floor := int64(-1 << 62)
	if i := strings.LastIndex(r.SQL, `"id" >= `); i >= 0 {
		floor, _ = strconv.ParseInt(strings.TrimSpace(r.SQL[i+len(`"id" >= `):]), 10, 64)
		floor--
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	var count int64
	for _, id := range f.ids {
		if id > floor {
			builder.Field(0).(*array.Int64Builder).Append(id)
			if f.extra {
				builder.Field(1).(*array.StringBuilder).AppendNull()
			}
			count++
		}
	}
	if count == 0 {
		return query.Stats{}, nil
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	return query.Stats{Rows: count}, sink.Write(record)
}

type idCollector struct{ ids []int64 }

func (c *idCollector) Schema(*arrow.Schema) error { return nil }
func (c *idCollector) Write(r arrow.RecordBatch) error {
	column := r.Column(0).(*array.Int64)
	for i := 0; i < column.Len(); i++ {
		c.ids = append(c.ids, column.Value(i))
	}
	return nil
}

func storedIDs(t *testing.T, m *Manager) []int64 {
	t.Helper()
	prev, err := m.store.(PreviousReader).OpenPrevious(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer prev.Release()
	var sink idCollector
	if err := copyPreviousRows(context.Background(), prev, firstSchema(t, prev), &sink); err != nil {
		t.Fatal(err)
	}
	slices.Sort(sink.ids)
	return sink.ids
}

func firstSchema(t *testing.T, prev PreviousGeneration) *arrow.Schema {
	t.Helper()
	f, closeFile, err := prev.Open(0)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFile()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	schema, err := ReadParquetSchema(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func incrementalDataset(multipart bool, request query.Request) catalog.Dataset {
	d := catalog.Dataset{ID: "events", Query: request, RefreshInterval: time.Minute, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits(),
		SchemaEvolution: &catalog.SchemaEvolution{AddNullableColumns: true},
		Incremental:     &catalog.Incremental{Column: "id", Key: []string{"id"}, Deletes: "soft", FullEvery: time.Hour}}
	if multipart {
		d.Multipart = &catalog.MultipartConfig{MaxParts: 2, MaxPartBytes: 1 << 20}
	}
	return d
}

func runIncrementalScenario(t *testing.T, m *Manager, source *deltaSource) {
	t.Helper()
	ctx := context.Background()
	clock := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return clock }
	var logs []string
	m.logf = func(format string, args ...any) { logs = append(logs, format) }

	source.ids = []int64{1, 2, 3}
	first, err := m.Refresh(ctx, "events", false)
	if err != nil || first.Rows != 3 || first.CoveredUntil != "3" || first.LastFullAt != clock.Format(time.RFC3339Nano) {
		t.Fatalf("first refresh %+v %v", first, err)
	}
	if info, _ := m.LastRefresh("events"); info.Incremental {
		t.Fatalf("first refresh cannot be incremental: %+v", info)
	}

	clock = clock.Add(time.Minute)
	source.ids = []int64{1, 2, 3, 4, 5}
	second, err := m.Refresh(ctx, "events", false)
	if err != nil || second.Rows != 5 || second.CoveredUntil != "5" || second.LastFullAt != first.LastFullAt || second.Generation == first.Generation {
		t.Fatalf("incremental refresh %+v %v", second, err)
	}
	if last := source.requests[len(source.requests)-1]; !strings.Contains(last, `"id" >= 3`) {
		t.Fatalf("delta predicate missing: %q", last)
	}
	if info, ok := m.LastRefresh("events"); !ok || !info.Incremental || info.AppendedRows != 3 {
		t.Fatalf("status info %+v", info)
	}
	if got := storedIDs(t, m); !slices.Equal(got, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("old rows lost or new rows missing: %v", got)
	}

	// Nothing new: still incremental, watermark unchanged, the boundary row re-read.
	third, err := m.Refresh(ctx, "events", false)
	if err != nil || third.Rows != 5 || third.CoveredUntil != "5" {
		t.Fatalf("empty delta %+v %v", third, err)
	}
	if info, _ := m.LastRefresh("events"); !info.Incremental || info.AppendedRows != 1 {
		t.Fatalf("empty delta info %+v", info)
	}

	// A row changed behind the watermark stays unseen until the full refresh.
	source.ids = []int64{1, 2, 3, 4, 5, 9}
	if _, err := m.Refresh(ctx, "events", false); err != nil {
		t.Fatal(err)
	}

	// full_every elapsed: whole source re-read and last_full_at advances.
	clock = clock.Add(2 * time.Hour)
	before := len(source.requests)
	full, err := m.Refresh(ctx, "events", false)
	if err != nil || len(source.requests) != before+1 || strings.Contains(source.requests[before], "kelvo_incr") || full.LastFullAt != clock.Format(time.RFC3339Nano) {
		t.Fatalf("full_every did not force a full refresh %+v %v", full, err)
	}
	if info, _ := m.LastRefresh("events"); info.Incremental {
		t.Fatalf("expected full: %+v", info)
	}
	if len(logs) == 0 {
		t.Fatal("fallback was not logged")
	}

	// A delta whose schema differs falls back to a full refresh that adopts it.
	clock = clock.Add(time.Minute)
	source.extra = true
	source.ids = append(source.ids, 10)
	changed, err := m.Refresh(ctx, "events", false)
	if err != nil || changed.Rows != 7 || changed.LastFullAt != clock.Format(time.RFC3339Nano) {
		t.Fatalf("schema change %+v %v", changed, err)
	}
	if info, _ := m.LastRefresh("events"); info.Incremental || info.Fallback == "" {
		t.Fatalf("schema change must be a logged full refresh: %+v", info)
	}
}

func TestIncrementalRefreshLocalBackends(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		_, m, _ := managerFixture(t)
		source := &deltaSource{}
		m.factory = func(catalog.Config, query.Limits) (query.Executor, error) { return source, nil }
		d := incrementalDataset(multipart, query.Request{Mode: "federated", Sources: []string{"source"}, SQL: "SELECT id FROM events"})
		m.config.Acceleration.Datasets[0] = d
		runIncrementalScenario(t, m, source)
	}
}

func TestIncrementalRefreshObjectBackends(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		var backend *objectBackend
		if multipart {
			backend, _ = remoteMultipartFixture(t)
		} else {
			backend = testObjectBackend(t, newFakeSnapshotObjects())
		}
		source := &deltaSource{}
		d := incrementalDataset(multipart, query.Request{Mode: "federated", Sources: []string{"mongo"}, SQL: "SELECT id FROM events"})
		config := catalog.Config{Sources: []catalog.Source{{ID: "mongo", Type: "mongodb", DSNEnv: "KELVO_SOURCE_FIXTURE_MONGO"}}, Acceleration: &backend.config}
		config.Acceleration.Datasets = []catalog.Dataset{d}
		m := &Manager{config: config, store: backend, factory: func(catalog.Config, query.Limits) (query.Executor, error) { return source, nil }}
		runIncrementalScenario(t, m, source)
	}
}
