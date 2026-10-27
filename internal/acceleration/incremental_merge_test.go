//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type mergeRow struct {
	id   int64
	ts   time.Time
	note string
}

// tsSource is a keyed table with a timestamp watermark that honours the
// wrapped `"ts" >= TIMESTAMP '...'` predicate. failAfterWrite simulates a crash
// after part of the delta reached the sink.
type tsSource struct {
	rows           []mergeRow
	failAfterWrite bool
	requests       []string
}

var tsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

func (f *tsSource) Execute(_ context.Context, r query.Request, sink query.Sink) (query.Stats, error) {
	f.requests = append(f.requests, r.SQL)
	if err := sink.Schema(tsSchema); err != nil {
		return query.Stats{}, err
	}
	var floor time.Time
	// The ts column is zoned, so the bound must carry an explicit UTC offset.
	if i := strings.LastIndex(r.SQL, `"ts" >= TIMESTAMPTZ '`); i >= 0 {
		v := strings.TrimSuffix(r.SQL[i+len(`"ts" >= TIMESTAMPTZ '`):], "'")
		floor, _ = time.Parse("2006-01-02 15:04:05.000000-07:00", v)
	} else if strings.Contains(r.SQL, "kelvo_incr") {
		return query.Stats{}, errors.New("zoned watermark bounded without an offset")
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, tsSchema)
	defer b.Release()
	var n int64
	for _, row := range f.rows {
		if row.ts.Before(floor) {
			continue
		}
		b.Field(0).(*array.Int64Builder).Append(row.id)
		b.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(row.ts.UnixMicro()))
		b.Field(2).(*array.StringBuilder).Append(row.note)
		n++
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	if err := sink.Write(rec); err != nil {
		return query.Stats{}, err
	}
	if f.failAfterWrite {
		return query.Stats{}, errors.New("source connection lost")
	}
	return query.Stats{Rows: n}, nil
}

type noteCollector struct{ rows []string }

func (c *noteCollector) Schema(*arrow.Schema) error { return nil }
func (c *noteCollector) Write(r arrow.RecordBatch) error {
	ids, notes := r.Column(0).(*array.Int64), r.Column(2).(*array.String)
	for i := 0; i < ids.Len(); i++ {
		c.rows = append(c.rows, fmt.Sprintf("%d=%s", ids.Value(i), notes.Value(i)))
	}
	return nil
}

func storedNotes(t *testing.T, m *Manager) []string {
	t.Helper()
	prev, err := m.store.(PreviousReader).OpenPrevious(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer prev.Release()
	var sink noteCollector
	if err := copyPreviousRows(context.Background(), prev, firstSchema(t, prev), &sink); err != nil {
		t.Fatal(err)
	}
	slices.Sort(sink.rows)
	return sink.rows
}

func TestIncrementalMergeSemantics(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		t.Run(fmt.Sprintf("multipart=%v", multipart), func(t *testing.T) { runMergeScenario(t, multipart) })
	}
}

func runMergeScenario(t *testing.T, multipart bool) {
	ctx := context.Background()
	_, m, _ := managerFixture(t)
	source := &tsSource{}
	m.factory = func(catalog.Config, query.Limits) (query.Executor, error) { return source, nil }
	d := incrementalDataset(multipart, query.Request{Mode: "federated", Sources: []string{"source"}, SQL: "SELECT id, ts, note FROM events"})
	d.SchemaEvolution = nil
	d.Incremental = &catalog.Incremental{Column: "ts", Key: []string{"id"}, Deletes: "soft", LateWindow: 10 * time.Minute, FullEvery: time.Hour}
	m.config.Acceleration.Datasets[0] = d
	clock := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return clock }
	m.logf = func(string, ...any) {}
	t0 := time.Date(2026, 3, 1, 11, 0, 0, 0, time.UTC)
	refresh := func(want ...string) Snapshot {
		t.Helper()
		clock = clock.Add(time.Second)
		s, err := m.Refresh(ctx, "events", false)
		if err != nil {
			t.Fatal(err)
		}
		if got := storedNotes(t, m); !slices.Equal(got, want) {
			t.Fatalf("stored %v, want %v", got, want)
		}
		return s
	}
	incremental := func(want bool) RefreshInfo {
		t.Helper()
		info, _ := m.LastRefresh("events")
		if info.Incremental != want {
			t.Fatalf("incremental=%v, want %v (%+v)", info.Incremental, want, info)
		}
		return info
	}

	source.rows = []mergeRow{{1, t0, "a"}}
	first := refresh("1=a")
	incremental(false)

	// A row committed later with the same watermark value is not missed, and the
	// re-read boundary row is not duplicated. The generation changes.
	source.rows = append(source.rows, mergeRow{2, t0, "b"})
	second := refresh("1=a", "2=b")
	incremental(true)
	if second.Generation == first.Generation {
		t.Fatal("incremental publication kept the previous generation identity")
	}

	// An update in place and a soft delete replace the previous row by key.
	t1 := t0.Add(time.Minute)
	source.rows = []mergeRow{{1, t1, "a2"}, {2, t1, "deleted"}}
	refresh("1=a2", "2=deleted")
	incremental(true)

	// Late rows: inside late_window they arrive; outside they wait for full_every.
	source.rows = append(source.rows, mergeRow{3, t1.Add(-5 * time.Minute), "late-in"}, mergeRow{4, t1.Add(-30 * time.Minute), "late-out"})
	refresh("1=a2", "2=deleted", "3=late-in")
	incremental(true)

	// A key whose watermark went backwards cannot be merged: full refresh.
	source.rows[0] = mergeRow{1, t1.Add(-time.Minute), "regressed"}
	refresh("1=regressed", "2=deleted", "3=late-in", "4=late-out")
	if info := incremental(false); !strings.Contains(info.Fallback, "backwards") {
		t.Fatalf("fallback reason %q", info.Fallback)
	}

	// A repeated key in the delta is not unique: full refresh keeps both rows.
	source.rows = append(source.rows, mergeRow{3, t1, "dup"})
	refresh("1=regressed", "2=deleted", "3=dup", "3=late-in", "4=late-out")
	if info := incremental(false); !strings.Contains(info.Fallback, "repeats a key") {
		t.Fatalf("fallback reason %q", info.Fallback)
	}
	source.rows = source.rows[:len(source.rows)-1]
	refresh("1=regressed", "2=deleted", "3=late-in", "4=late-out")

	// A crash after part of the delta was written publishes nothing; the
	// previous generation stays current and the next refresh succeeds.
	before, err := m.Status("events")
	if err != nil {
		t.Fatal(err)
	}
	source.rows = append(source.rows, mergeRow{5, t1.Add(time.Hour), "new"})
	source.failAfterWrite = true
	if _, err := m.Refresh(ctx, "events", false); err == nil {
		t.Fatal("failed delta published")
	}
	after, err := m.Status("events")
	if err != nil || after.Generation != before.Generation {
		t.Fatalf("previous generation not preserved: %+v %v", after, err)
	}
	if got := storedNotes(t, m); len(got) != 4 {
		t.Fatalf("previous rows changed: %v", got)
	}
	source.failAfterWrite = false
	refresh("1=regressed", "2=deleted", "3=late-in", "4=late-out", "5=new")
	incremental(true)
}

// A delta above the merge cap (half of memory_mb here) falls back to a full
// refresh that succeeds, instead of failing the refresh.
func TestIncrementalDeltaOverCapFallsBackToFull(t *testing.T) {
	_, m, _ := managerFixture(t)
	source := &tsSource{}
	m.factory = func(catalog.Config, query.Limits) (query.Executor, error) { return source, nil }
	d := incrementalDataset(false, query.Request{Mode: "federated", Sources: []string{"source"}, SQL: "SELECT id, ts, note FROM events"})
	d.SchemaEvolution = nil
	d.Limits.MemoryMB = 1
	d.Incremental = &catalog.Incremental{Column: "ts", Key: []string{"id"}, Deletes: "soft", FullEvery: time.Hour}
	m.config.Acceleration.Datasets[0] = d
	m.logf = func(string, ...any) {}
	t0 := time.Date(2026, 3, 1, 11, 0, 0, 0, time.UTC)
	for i := int64(0); i < 40000; i++ {
		source.rows = append(source.rows, mergeRow{i, t0, "row-payload-0123456789"})
	}
	if _, err := m.Refresh(context.Background(), "events", false); err != nil {
		t.Fatal(err)
	}
	s, err := m.Refresh(context.Background(), "events", false)
	if err != nil || s.Rows != 40000 {
		t.Fatalf("full fallback %+v %v", s, err)
	}
	if info, _ := m.LastRefresh("events"); info.Incremental || !strings.Contains(info.Fallback, "cap") {
		t.Fatalf("expected cap fallback: %+v", info)
	}
	if info, _ := m.LastRefresh("events"); info.Fallbacks != 2 || info.EverIncremental {
		t.Fatalf("fallback streak %+v", info)
	}
}

func TestMergeCapAndFieldLookup(t *testing.T) {
	if got := mergeCap(query.Limits{MaxBytes: 1 << 30, MemoryMB: 64}); got != 32<<20 {
		t.Fatalf("memory-bound cap %d", got)
	}
	if got := mergeCap(query.Limits{MaxBytes: 1 << 10, MemoryMB: 64}); got != 1<<10 {
		t.Fatalf("max_bytes cap %d", got)
	}
	if fieldIndex(tsSchema, "TS") != 1 || fieldIndex(tsSchema, "missing") != -1 {
		t.Fatal("key and watermark columns must match case-insensitively, like coverage")
	}
	for _, typ := range []arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Uint32, arrow.FixedWidthTypes.Date64} {
		b := array.NewBuilder(memory.DefaultAllocator, typ)
		switch bb := b.(type) {
		case *array.Int8Builder:
			bb.Append(1)
		case *array.Uint32Builder:
			bb.Append(1)
		case *array.Date64Builder:
			bb.Append(86400000)
		}
		a := b.NewArray()
		if _, ok := watermarkOf(a, 0); !ok {
			t.Errorf("%s watermark rejected though coverage accepts it", typ)
		}
		a.Release()
		b.Release()
	}
}

// Panics outside Parquet decoding are not corruption.
func TestEachPartBatchDoesNotMaskSinkPanics(t *testing.T) {
	_, m, _ := managerFixture(t)
	source := &tsSource{rows: []mergeRow{{1, time.Now().UTC().Truncate(time.Microsecond), "a"}}}
	m.factory = func(catalog.Config, query.Limits) (query.Executor, error) { return source, nil }
	d := incrementalDataset(false, query.Request{Mode: "federated", Sources: []string{"source"}, SQL: "SELECT id, ts, note FROM events"})
	d.SchemaEvolution = nil
	d.Incremental = &catalog.Incremental{Column: "ts", Key: []string{"id"}, Deletes: "soft", FullEvery: time.Hour}
	m.config.Acceleration.Datasets[0] = d
	if _, err := m.Refresh(context.Background(), "events", false); err != nil {
		t.Fatal(err)
	}
	prev, err := m.store.(PreviousReader).OpenPrevious(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer prev.Release()
	schema := firstSchema(t, prev)
	f, closeFile, err := prev.Open(0)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFile()
	defer func() {
		if recover() == nil {
			t.Fatal("sink panic was swallowed")
		}
	}()
	_, _ = eachPartBatch(context.Background(), f, schema, func(arrow.RecordBatch) error { panic("sink bug") })
}

// The delta spill lives in the dataset's transaction directory: removed on
// abort, and a crash leftover is removed by the existing stage cleanup.
func TestIncrementalSpillStaysInTransactionDirectory(t *testing.T) {
	_, m, _ := managerFixture(t)
	source := &tsSource{rows: []mergeRow{{1, time.Date(2026, 3, 1, 11, 0, 0, 0, time.UTC), "a"}}}
	m.factory = func(catalog.Config, query.Limits) (query.Executor, error) { return source, nil }
	d := incrementalDataset(false, query.Request{Mode: "federated", Sources: []string{"source"}, SQL: "SELECT id, ts, note FROM events"})
	d.SchemaEvolution = nil
	d.Incremental = &catalog.Incremental{Column: "ts", Key: []string{"id"}, Deletes: "soft", FullEvery: time.Hour}
	m.config.Acceleration.Datasets[0] = d
	ctx := context.Background()
	for range 2 {
		if _, err := m.Refresh(ctx, "events", false); err != nil {
			t.Fatal(err)
		}
	}
	if info, _ := m.LastRefresh("events"); !info.Incremental {
		t.Fatalf("expected incremental %+v", info)
	}
	snapshot, err := m.Status("events")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(snapshot.Path)
	leftovers := func() []string {
		matches, _ := filepath.Glob(filepath.Join(dir, "*delta.arrow"))
		return matches
	}
	if l := leftovers(); len(l) != 0 {
		t.Fatalf("spill left after commit: %v", l)
	}
	tx, err := m.store.Begin(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	spill, err := tx.(spillWriter).NewSpill()
	if err != nil {
		t.Fatal(err)
	}
	if l := leftovers(); len(l) != 1 {
		t.Fatalf("spill not in the dataset directory: %v", l)
	}
	_ = spill.Close()
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
	if l := leftovers(); len(l) != 0 {
		t.Fatalf("spill left after abort: %v", l)
	}
	stray := filepath.Join(dir, ".stage-0123456789abcdef0123456789abcdef-delta.arrow")
	if err := os.WriteFile(stray, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := storeCleanStages(ctx, f); err != nil {
		t.Fatal(err)
	}
	if l := leftovers(); len(l) != 0 {
		t.Fatalf("crash leftover not cleaned: %v", l)
	}
}

func TestMergeSinkRowCapSpillFailureAndSentinelWatermarks(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int8}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := range 11 {
		b.Field(0).(*array.Int8Builder).Append(int8(i))
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	// 11 rows over a 640-byte cap's 10-row limit, though the bytes fit.
	capped := &mergeSink{loaded: true, maxBytes: 640}
	if err := capped.Write(rec); !errors.Is(err, errIncrementalFallback) || !strings.Contains(capped.fallback, "row cap") {
		t.Fatalf("row cap: %v %q", err, capped.fallback)
	}
	// A spill that cannot be written falls back instead of failing the refresh.
	dir := t.TempDir()
	broken := &mergeSink{schema: schema, watermark: "id", key: []string{"id"}, inner: &noteCollector{}, newSpill: func() (*os.File, error) {
		return os.Open(dir) // a directory: every write fails
	}}
	err := broken.Schema(schema)
	if err == nil {
		err = broken.Write(rec)
	}
	if err == nil {
		err = broken.finish()
	}
	if !errors.Is(err, errIncrementalFallback) || !strings.Contains(broken.fallback, "spill") {
		t.Fatalf("spill failure: %v %q", err, broken.fallback)
	}
	broken.release()
	// 9999-12-31 in nanoseconds overflows UnixNano; raw values still order.
	ts := &arrow.TimestampType{Unit: arrow.Microsecond}
	tb := array.NewTimestampBuilder(memory.DefaultAllocator, ts)
	defer tb.Release()
	tb.Append(arrow.Timestamp(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()))
	tb.Append(arrow.Timestamp(time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC).UnixMicro()))
	a := tb.NewArray()
	defer a.Release()
	early, _ := watermarkOf(a, 0)
	late, _ := watermarkOf(a, 1)
	if !early.less(late) || late.less(early) {
		t.Fatal("sentinel watermark ordered incorrectly")
	}
}
