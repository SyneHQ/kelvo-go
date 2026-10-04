//go:build duckbridge && duckdb_arrow && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckbridge

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func date32Schema() *arrow.Schema {
	// The Date32 ordinal differs from its index in a selective projection.
	return arrow.NewSchema([]arrow.Field{
		{Name: "row_id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "label", Type: arrow.BinaryTypes.String},
		{Name: "event_day", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
	}, nil)
}

var date32FixtureDays = []int32{
	-2147483647, -2147483646, -800000, -25568, -1, 0, 1,
	11016, 120529, 120530, 2932896, 2932897, 2147483646, 2147483647,
}

// This fixture evaluates membership in the TRUE set independently of DuckDB.
// With positive AND/OR trees, a NULL comparison cannot make a row qualify;
// IS NULL/IS NOT NULL supply their own ordinary Boolean result.
func date32FixtureMatch(filter Filter, value int32, null bool) (bool, error) {
	if filter.Kind == "and" || filter.Kind == "or" {
		result := filter.Kind == "and"
		for _, child := range filter.Children {
			match, err := date32FixtureMatch(child, value, null)
			if err != nil {
				return false, err
			}
			if filter.Kind == "and" {
				result = result && match
			} else {
				result = result || match
			}
		}
		return result, nil
	}
	if filter.Column != "event_day" {
		return false, errors.New("non-date predicate escaped local evaluation")
	}
	if filter.Kind == "is_null" {
		return null, nil
	}
	if filter.Kind == "is_not_null" {
		return !null, nil
	}
	if filter.Kind != "comparison" || filter.Type != "date32" {
		return false, errors.New("source did not receive a typed Date32 comparison")
	}
	constant, err := strconv.ParseInt(filter.Value, 10, 32)
	if err != nil || strconv.FormatInt(constant, 10) != filter.Value {
		return false, errors.New("source did not receive exact signed days")
	}
	if null {
		return false, nil
	}
	day := int64(value)
	switch filter.Op {
	case "eq":
		return day == constant, nil
	case "ne":
		return day != constant, nil
	case "lt":
		return day < constant, nil
	case "le":
		return day <= constant, nil
	case "gt":
		return day > constant, nil
	case "ge":
		return day >= constant, nil
	default:
		return false, errors.New("source received unsupported date operator")
	}
}

func date32FixtureReader(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
	schema := date32Schema()
	fields := make([]arrow.Field, 0, len(plan.Columns))
	for _, name := range plan.Columns {
		indices := schema.FieldIndices(name)
		if len(indices) != 1 {
			return nil, errors.New("unknown fixture projection")
		}
		fields = append(fields, schema.Field(indices[0]))
	}
	if len(fields) == 0 {
		fields = []arrow.Field{{Name: "__kelvo_count", Type: arrow.PrimitiveTypes.Uint8}}
	}
	projected := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, projected)
	defer builder.Release()
	for i := 0; i <= len(date32FixtureDays); i++ {
		null, day := i == len(date32FixtureDays), int32(0)
		if !null {
			day = date32FixtureDays[i]
		}
		keep := true
		for _, filter := range plan.Filters {
			match, err := date32FixtureMatch(filter, day, null)
			if err != nil {
				return nil, err
			}
			keep = keep && match
		}
		if !keep {
			continue
		}
		for j, field := range fields {
			switch field.Name {
			case "row_id":
				builder.Field(j).(*array.Int32Builder).Append(int32(i + 1))
			case "label":
				builder.Field(j).(*array.StringBuilder).Append(fmt.Sprintf("row_%02d", i+1))
			case "event_day":
				if null {
					builder.Field(j).AppendNull()
				} else {
					builder.Field(j).(*array.Date32Builder).Append(arrow.Date32(day))
				}
			case "__kelvo_count":
				builder.Field(j).(*array.Uint8Builder).Append(1)
			}
		}
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	reader, err := array.NewRecordReader(projected, []arrow.RecordBatch{record})
	if err != nil {
		return nil, err
	}
	// Force GC between Arrow batches while native code retains the buffers.
	return gcReader{reader}, nil
}

func readDate32Rows(t *testing.T, conn *sql.Conn, statement string) boundRows {
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
		result.types = append(result.types, strings.Clone(typ.DatabaseTypeName()))
	}
	for rows.Next() {
		values, dest := make([]any, len(types)), make([]any, len(types))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		// Driver strings/buffers can borrow the current chunk. Detach before
		// Next/Close or another query, including the disabled parity scan.
		for i, value := range values {
			switch v := value.(type) {
			case string:
				values[i] = strings.Clone(v)
			case []byte:
				values[i] = bytes.Clone(v)
			}
		}
		result.values = append(result.values, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func date32FilterPresent(filters []Filter, op, value string) bool {
	for _, filter := range filters {
		if filter.Kind == "comparison" && filter.Column == "event_day" && filter.Type == "date32" && filter.Op == op && filter.Value == value {
			return true
		}
		if date32FilterPresent(filter.Children, op, value) {
			return true
		}
	}
	return false
}

func TestDate32BoundPredicatesSerializeExactDays(t *testing.T) {
	for _, tc := range []struct {
		name, constant, days string
		id                   int32
	}{
		{"negative infinity", "DATE '-infinity'", "-2147483647", 1},
		{"minimum finite", "(DATE '1970-01-01' - 2147483646)", "-2147483646", 2},
		{"pre epoch", "DATE '1969-12-31'", "-1", 5},
		{"epoch", "DATE '1970-01-01'", "0", 6},
		{"leap day", "DATE '2000-02-29'", "11016", 8},
		{"extended calendar", "DATE '10000-01-01'", "2932897", 12},
		{"maximum finite", "(DATE '1970-01-01' + 2147483646)", "2147483646", 13},
		{"positive infinity", "DATE 'infinity'", "2147483647", 14},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := &boundObservation{}
			conn, _ := boundConnection(t, boundTableFixture{"selected", date32Schema(), observed.wrap(date32FixtureReader), PredicateCapabilities{Columns: []string{"event_day"}}})
			got := readDate32Rows(t, conn, "SELECT row_id FROM bridge.selected WHERE event_day="+tc.constant)
			if !reflect.DeepEqual(got.types, []string{"INTEGER"}) || !reflect.DeepEqual(got.values, [][]any{{tc.id}}) {
				t.Fatalf("DATE value changed: %#v", got)
			}
			plans, rows, _ := observed.snapshot()
			found := false
			for _, plan := range plans {
				found = found || date32FilterPresent(plan.Filters, "eq", tc.days)
			}
			if !found || rows != 1 {
				t.Fatalf("raw DATE constant did not reach the producer: %+v rows=%d", plans, rows)
			}
		})
	}
}

func TestDate32BoundPredicatesMatchDisabled(t *testing.T) {
	for _, predicate := range []string{
		"event_day=DATE '1970-01-01'", "event_day<>DATE '1970-01-01'", "event_day<DATE '1970-01-01'",
		"event_day<=DATE '1970-01-01'", "event_day>DATE '1970-01-01'", "event_day>=DATE '1970-01-01'",
		"event_day IS NULL", "event_day IS NOT NULL", "event_day=CAST(NULL AS DATE)",
		"event_day>=DATE '1969-12-31' AND event_day<=DATE '1970-01-02'",
		"event_day=DATE '1969-12-31' OR event_day=DATE '1970-01-02'",
		"event_day>DATE '10000-01-01' OR event_day IS NULL",
		"(event_day<DATE '1900-01-01' OR event_day IS NULL) AND label<>'row_01'",
		"event_day>DATE '-infinity' AND event_day<DATE 'infinity'",
		"event_day=DATE 'infinity' OR event_day=DATE '-infinity'",
		"event_day<DATE '-infinity'", "event_day>DATE 'infinity'",
	} {
		t.Run(predicate, func(t *testing.T) {
			on, off := &boundObservation{}, &boundObservation{}
			conn, _ := boundConnection(t,
				boundTableFixture{"selected", date32Schema(), on.wrap(date32FixtureReader), PredicateCapabilities{Columns: []string{"event_day"}}},
				boundTableFixture{"residual", date32Schema(), off.wrap(date32FixtureReader), PredicateCapabilities{}},
			)
			statement := "SELECT row_id FROM bridge.%s WHERE " + predicate + " ORDER BY row_id"
			pushed := readDate32Rows(t, conn, fmt.Sprintf(statement, "selected"))
			local := readDate32Rows(t, conn, fmt.Sprintf(statement, "residual"))
			if !reflect.DeepEqual(pushed, local) {
				t.Fatalf("Date32 pushdown changed results: %#v != %#v", pushed, local)
			}
			plans, full, _ := off.snapshot()
			for _, plan := range plans {
				if len(plan.Filters) != 0 {
					t.Fatal("disabled Date32 predicates reached the producer")
				}
			}
			_, filtered, _ := on.snapshot()
			if filtered > full {
				t.Fatal("pushdown unexpectedly fetched more rows")
			}
			if predicate == "event_day=DATE '1970-01-01'" && (filtered != 1 || full != 15) {
				t.Fatal("selective Date32 equality did not reduce actual source rows")
			}
		})
	}
}

func TestDate32BoundPredicatesPreserveRelationsProjectionAndRescans(t *testing.T) {
	on, off := &boundObservation{}, &boundObservation{}
	conn, _ := boundConnection(t,
		boundTableFixture{"selected", date32Schema(), on.wrap(date32FixtureReader), PredicateCapabilities{Columns: []string{"event_day"}}},
		boundTableFixture{"residual", date32Schema(), off.wrap(date32FixtureReader), PredicateCapabilities{}},
	)
	for _, statement := range []string{
		"SELECT label FROM bridge.%s WHERE event_day>=DATE '1969-12-31' AND event_day<=DATE '1970-01-02' AND label<>'row_06' ORDER BY label",
		"WITH recent AS (SELECT row_id,event_day FROM bridge.%s WHERE event_day>=DATE '1969-12-31') SELECT row_id FROM recent WHERE event_day<=DATE '1970-01-02' ORDER BY row_id",
		"SELECT a.row_id,b.row_id FROM bridge.%s a JOIN bridge.residual b ON a.event_day=b.event_day WHERE a.event_day>=DATE '1969-12-31' AND a.event_day<=DATE '1970-01-02' ORDER BY a.row_id,b.row_id",
		"SELECT a.row_id,b.row_id FROM bridge.%s a LEFT JOIN bridge.residual b ON a.event_day=b.event_day WHERE a.event_day<=DATE '1970-01-01' OR a.event_day IS NULL ORDER BY a.row_id,b.row_id",
		"SELECT a.row_id,b.row_id FROM bridge.%s a JOIN bridge.residual b ON a.event_day IS NOT DISTINCT FROM b.event_day WHERE a.event_day=DATE '1970-01-01' OR a.event_day IS NULL ORDER BY a.row_id,b.row_id",
		"SELECT count(*) FROM bridge.%s WHERE event_day<DATE '1970-01-01'",
	} {
		pushed := readDate32Rows(t, conn, fmt.Sprintf(statement, "selected"))
		local := readDate32Rows(t, conn, fmt.Sprintf(statement, "residual"))
		if !reflect.DeepEqual(pushed, local) {
			t.Fatalf("Date32 relation/projection parity failed: %#v != %#v", pushed, local)
		}
	}
	self := readDate32Rows(t, conn, "SELECT a.row_id FROM bridge.selected a JOIN bridge.selected b ON a.event_day=b.event_day WHERE a.event_day>=DATE '1969-12-31' AND b.event_day<=DATE '1970-01-02' ORDER BY a.row_id")
	if !reflect.DeepEqual(self.values, [][]any{{int32(5)}, {int32(6)}, {int32(7)}}) {
		t.Fatalf("independent Date32 self scans changed: %#v", self)
	}
	plans, _, _ := off.snapshot()
	for _, plan := range plans {
		if len(plan.Filters) != 0 {
			t.Fatal("Date32 eligibility leaked between relations")
		}
	}
	pushed, _, _ := on.snapshot()
	if len(pushed) < 7 || len(pushed[0].Columns) >= date32Schema().NumFields() || !date32FilterPresent(pushed[0].Filters, "ge", "-1") {
		t.Fatal("Date32 original ordinal, projection or independent rescan was lost")
	}
}

func TestDate32BoundPredicatesKeepSparseINLocal(t *testing.T) {
	observed := &boundObservation{}
	conn, _ := boundConnection(t, boundTableFixture{"selected", date32Schema(), observed.wrap(date32FixtureReader), PredicateCapabilities{Columns: []string{"event_day"}}})
	if _, err := conn.ExecContext(context.Background(), "SET disabled_optimizers='in_clause'"); err != nil {
		t.Fatal(err)
	}
	got := readDate32Rows(t, conn, "SELECT row_id FROM bridge.selected WHERE event_day IN (DATE '1969-12-31',DATE '1970-01-02',DATE '2000-02-29') ORDER BY row_id")
	if !reflect.DeepEqual(got.values, [][]any{{int32(5)}, {int32(7)}, {int32(8)}}) {
		t.Fatalf("local Date32 IN changed: %#v", got)
	}
	plans, _, _ := observed.snapshot()
	if len(plans) == 0 {
		t.Fatal("Date32 IN source scan missing")
	}
	for _, plan := range plans {
		// The pinned plan wraps this IN as optional. It must not be expanded
		// into mandatory equality filters by the existing integer hint path.
		if len(plan.Filters) != 0 {
			t.Fatalf("Date32 optional IN vocabulary unexpectedly widened: %+v", plan)
		}
	}
}

type date32ReleaseReader struct {
	array.RecordReader
	releases *atomic.Int64
}

func (r date32ReleaseReader) Release() {
	r.RecordReader.Release()
	r.releases.Add(1)
}

func TestDate32BoundPredicateFailureAndCancellationReleaseOwnership(t *testing.T) {
	t.Run("producer error owns returned reader", func(t *testing.T) {
		wanted := query.NewError("UNSUPPORTED", "fixture refuses a required Date32 predicate")
		var calls, releases atomic.Int64
		conn, factories := boundConnection(t, boundTableFixture{"selected", date32Schema(), func(ctx context.Context, plan ScanPlan) (array.RecordReader, error) {
			calls.Add(1)
			if !date32FilterPresent(plan.Filters, "eq", "0") {
				return nil, errors.New("required Date32 filter disappeared")
			}
			reader, err := date32FixtureReader(ctx, plan)
			if err != nil {
				return nil, err
			}
			return date32ReleaseReader{reader, &releases}, wanted
		}, PredicateCapabilities{Columns: []string{"event_day"}}})
		_, err := conn.ExecContext(context.Background(), "SELECT row_id FROM bridge.selected WHERE event_day=DATE '1970-01-01'")
		if err == nil || !errors.Is(factories[0].Err(), wanted) || calls.Load() != 1 || releases.Load() != 1 {
			t.Fatalf("required Date32 failure retried, lost or leaked: %v calls=%d releases=%d", err, calls.Load(), releases.Load())
		}
	})
	t.Run("cancel blocked date scan", func(t *testing.T) {
		started := make(chan struct{})
		conn, factories := boundConnection(t, boundTableFixture{"selected", date32Schema(), func(ctx context.Context, plan ScanPlan) (array.RecordReader, error) {
			if !date32FilterPresent(plan.Filters, "eq", "0") {
				return nil, errors.New("required Date32 filter disappeared")
			}
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}, PredicateCapabilities{Columns: []string{"event_day"}}})
		done := make(chan error, 1)
		go func() {
			_, err := conn.ExecContext(context.Background(), "SELECT row_id FROM bridge.selected WHERE event_day=DATE '1970-01-01'")
			done <- err
		}()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			factories[0].cancel()
			t.Fatal("date source did not start")
		}
		factories[0].cancel()
		select {
		case err := <-done:
			if err == nil || !errors.Is(factories[0].Err(), context.Canceled) {
				t.Fatal("date source cancellation was lost", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("date source cancellation did not join")
		}
	})
}
