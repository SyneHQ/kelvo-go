//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"fmt"
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
	if i := strings.LastIndex(r.SQL, `"ts" >= TIMESTAMP '`); i >= 0 {
		v := strings.TrimSuffix(r.SQL[i+len(`"ts" >= TIMESTAMP '`):], "'")
		floor, _ = time.Parse("2006-01-02 15:04:05.000000", v)
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
