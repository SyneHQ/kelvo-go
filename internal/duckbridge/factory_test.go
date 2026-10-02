//go:build duckbridge && duckdb_arrow && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckbridge

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func bridgeSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "label", Type: arrow.BinaryTypes.String}, {Name: "huge", Type: arrow.PrimitiveTypes.Uint64}, {Name: "enabled", Type: arrow.FixedWidthTypes.Boolean}}, nil)
}

type gcReader struct{ array.RecordReader }

func (r gcReader) Next() bool { runtime.GC(); return r.RecordReader.Next() }

func fixtureReader(t *testing.T, schema *arrow.Schema, plan ScanPlan) (array.RecordReader, error) {
	t.Helper()
	var fields []arrow.Field
	for _, name := range plan.Columns {
		fields = append(fields, schema.Field(schema.FieldIndices(name)[0]))
	}
	if len(fields) == 0 {
		fields = []arrow.Field{{Name: "__kelvo_count", Type: arrow.PrimitiveTypes.Uint8}}
	}
	projected := arrow.NewSchema(fields, nil)
	var records []arrow.RecordBatch
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()
	for id := int64(0); id < 5; id++ {
		keep := true
		for _, filter := range plan.Filters {
			match, err := fixtureFilter(filter, id)
			if err != nil {
				return nil, err
			}
			keep = keep && match
		}
		if !keep {
			continue
		}
		builder := array.NewRecordBuilder(memory.NewGoAllocator(), projected)
		for i, field := range fields {
			switch field.Name {
			case "id":
				builder.Field(i).(*array.Int64Builder).Append(id)
			case "label":
				builder.Field(i).(*array.StringBuilder).Append("row_" + strconv.FormatInt(id, 10))
			case "huge":
				builder.Field(i).(*array.Uint64Builder).Append(math.MaxUint64 - uint64(id))
			case "enabled":
				builder.Field(i).(*array.BooleanBuilder).Append(id%2 == 0)
			case "__kelvo_count":
				builder.Field(i).(*array.Uint8Builder).Append(1)
			default:
				builder.Release()
				return nil, errors.New("unknown fixture column")
			}
		}
		records = append(records, builder.NewRecordBatch())
		builder.Release()
	}
	reader, err := array.NewRecordReader(projected, records)
	if err != nil {
		return nil, err
	}
	return gcReader{reader}, nil
}

func fixtureFilter(filter Filter, id int64) (bool, error) {
	switch filter.Kind {
	case "and", "or":
		value := filter.Kind == "and"
		for _, child := range filter.Children {
			next, err := fixtureFilter(child, id)
			if err != nil {
				return false, err
			}
			if filter.Kind == "and" {
				value = value && next
			} else {
				value = value || next
			}
		}
		return value, nil
	case "is_null":
		return false, nil
	case "is_not_null":
		return true, nil
	case "comparison":
		if filter.Column == "enabled" {
			expected, err := strconv.ParseBool(filter.Value)
			return (id%2 == 0) == expected, err
		}
		if filter.Column != "id" {
			return false, errors.New("unsupported fixture column")
		}
		v, err := strconv.ParseInt(filter.Value, 10, 64)
		if err != nil {
			return false, err
		}
		switch filter.Op {
		case "eq":
			return id == v, nil
		case "ne":
			return id != v, nil
		case "lt":
			return id < v, nil
		case "le":
			return id <= v, nil
		case "gt":
			return id > v, nil
		case "ge":
			return id >= v, nil
		}
	}
	return false, errors.New("unsupported fixture filter")
}

func registeredFactory(t *testing.T, schema *arrow.Schema, producer Producer) (*sql.Conn, *Factory) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	if _, err := conn.ExecContext(ctx, "CREATE SCHEMA bridge"); err != nil {
		conn.Close()
		db.Close()
		cancel()
		t.Fatal(err)
	}
	factory, err := New(ctx, schema, producer)
	if err != nil {
		conn.Close()
		db.Close()
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		db.Close()
		factory.Close()
		cancel()
		runtime.GC()
		if activeStreams.Load() != 0 || activePins.Load() != 0 {
			t.Errorf("bridge ownership leaked: streams=%d pins=%d", activeStreams.Load(), activePins.Load())
		}
	})
	if err := conn.Raw(func(raw any) error { return factory.Register(raw.(driver.Conn), "bridge", "data") }); err != nil {
		t.Fatal(err)
	}
	return conn, factory
}

func TestFactoryProjectionFiltersCountAndIndependentRescans(t *testing.T) {
	schema := bridgeSchema()
	var mu sync.Mutex
	var plans []ScanPlan
	conn, factory := registeredFactory(t, schema, func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		mu.Lock()
		plans = append(plans, plan)
		mu.Unlock()
		return fixtureReader(t, schema, plan)
	})
	rows, err := conn.QueryContext(context.Background(), "SELECT label,id FROM bridge.data WHERE id>=2 AND id<4 ORDER BY id")
	if err != nil {
		t.Fatal(err, factory.Err())
	}
	var got []int64
	for rows.Next() {
		var label string
		var id int64
		if err := rows.Scan(&label, &id); err != nil {
			t.Fatal(err)
		}
		if label != "row_"+strconv.FormatInt(id, 10) {
			t.Fatal("projection order corrupted")
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if !reflect.DeepEqual(got, []int64{2, 3}) {
		t.Fatalf("required filter not applied: %v", got)
	}
	var count int64
	if err := conn.QueryRowContext(context.Background(), "SELECT count(*) FROM bridge.data").Scan(&count); err != nil || count != 5 {
		t.Fatalf("empty projection failed: %d %v %v", count, err, factory.Err())
	}
	if err := conn.QueryRowContext(context.Background(), "SELECT count(*) FROM bridge.data a JOIN bridge.data b ON a.id=b.id WHERE a.id=2").Scan(&count); err != nil || count != 1 {
		t.Fatalf("independent self-join failed: %d %v %v", count, err, factory.Err())
	}
	var maximum uint64
	if err := conn.QueryRowContext(context.Background(), "SELECT max(huge) FROM bridge.data").Scan(&maximum); err != nil || maximum != math.MaxUint64 {
		t.Fatalf("uint64 changed: %d %v", maximum, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(plans) < 5 {
		t.Fatalf("expected independent reader per scan, got %d", len(plans))
	}
	if len(plans[0].Columns) != 2 || len(plans[0].Filters) == 0 {
		t.Fatalf("projection/filter missing: %+v", plans[0])
	}
	// This pinned optimizer selects the first physical column for COUNT(*).
	// Preserve that requested projection instead of inventing a dummy column.
	if len(plans[1].Columns) > 1 {
		t.Fatalf("count unnecessarily projected the full source: %+v", plans[1])
	}
}

func TestFactoryResidualStringPredicatesKeepCorrectResults(t *testing.T) {
	var plans []ScanPlan
	conn, factory := registeredFactory(t, bridgeSchema(), func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		plans = append(plans, plan)
		for _, filter := range plan.Filters {
			if filter.Column == "label" {
				return nil, errors.New("string filter must remain local")
			}
		}
		return fixtureReader(t, bridgeSchema(), plan)
	})
	var label string
	if err := conn.QueryRowContext(context.Background(), "SELECT label FROM bridge.data WHERE label='row_2'").Scan(&label); err != nil || label != "row_2" {
		t.Fatalf("local string predicate failed: %q %v %v", label, err, factory.Err())
	}
	if len(plans) != 1 || len(plans[0].Filters) != 0 {
		t.Fatalf("unsupported predicate was pushed: %+v", plans)
	}
	var count int64
	if err := conn.QueryRowContext(context.Background(), "SELECT count(*) FROM bridge.data WHERE id >= 2 AND label='row_2'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("mixed source/local predicates changed rows: %d %v %v", count, err, factory.Err())
	}
	if len(plans) != 2 || len(plans[1].Filters) == 0 {
		t.Fatal("integer pushdown was unnecessarily disabled")
	}
	if err := conn.QueryRowContext(context.Background(), "SELECT count(*) FROM bridge.data WHERE label='row_2' OR id=4").Scan(&count); err != nil || count != 2 {
		t.Fatalf("OR across local and source columns changed rows: %d %v %v", count, err, factory.Err())
	}
}

func TestFactoryPropagatesTypedProducerFailure(t *testing.T) {
	wanted := query.NewError("RESOURCE_EXHAUSTED", "Source scan exceeds its configured limit")
	conn, factory := registeredFactory(t, bridgeSchema(), func(context.Context, ScanPlan) (array.RecordReader, error) { return nil, wanted })
	_, err := conn.ExecContext(context.Background(), "SELECT id FROM bridge.data")
	if err == nil || factory.Err() != wanted {
		t.Fatalf("source error was lost: %v %v", err, factory.Err())
	}
}

func TestFactoryDictionaryBuffersSurviveGCAndRelease(t *testing.T) {
	dictionaryType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: dictionaryType}}, nil)
	conn, factory := registeredFactory(t, schema, func(context.Context, ScanPlan) (array.RecordReader, error) {
		indicesBuilder := array.NewInt8Builder(memory.NewGoAllocator())
		indicesBuilder.AppendValues([]int8{1, 0, 1}, nil)
		indices := indicesBuilder.NewArray()
		indicesBuilder.Release()
		defer indices.Release()
		valuesBuilder := array.NewStringBuilder(memory.NewGoAllocator())
		valuesBuilder.AppendValues([]string{"a", "b"}, nil)
		values := valuesBuilder.NewArray()
		valuesBuilder.Release()
		defer values.Release()
		column := array.NewDictionaryArray(dictionaryType, indices, values)
		defer column.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{column}, 3)
		defer record.Release()
		reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
		if err != nil {
			return nil, err
		}
		return gcReader{reader}, nil
	})
	var result string
	if err := conn.QueryRowContext(context.Background(), "SELECT string_agg(value, '' ORDER BY value) FROM bridge.data").Scan(&result); err != nil || result != "abb" {
		t.Fatalf("dictionary ownership changed: %q %v %v", result, err, factory.Err())
	}
}

type panicReader struct{ array.RecordReader }

func (panicReader) Next() bool { panic("fixture-private-callback-detail") }

func TestFactoryCallbackPanicsBecomeErrorsAndReleaseOwnership(t *testing.T) {
	for _, phase := range []string{"producer", "next"} {
		t.Run(phase, func(t *testing.T) {
			conn, factory := registeredFactory(t, bridgeSchema(), func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
				if phase == "producer" {
					panic("fixture-private-callback-detail")
				}
				reader, err := fixtureReader(t, bridgeSchema(), plan)
				if err != nil {
					return nil, err
				}
				return panicReader{reader}, nil
			})
			_, err := conn.ExecContext(context.Background(), "SELECT id FROM bridge.data")
			if err == nil || factory.Err() == nil || strings.Contains(factory.Err().Error(), "fixture-private") {
				t.Fatalf("callback panic escaped or disclosed details: %v %v", err, factory.Err())
			}
		})
	}
}

func TestFactoryCancellationReachesBlockedProducer(t *testing.T) {
	started := make(chan struct{})
	conn, factory := registeredFactory(t, bridgeSchema(), func(ctx context.Context, _ ScanPlan) (array.RecordReader, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	done := make(chan error, 1)
	go func() { _, err := conn.ExecContext(context.Background(), "SELECT id FROM bridge.data"); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("source producer did not start")
	}
	factory.cancel()
	select {
	case err := <-done:
		if err == nil || !errors.Is(factory.Err(), context.Canceled) {
			t.Fatalf("cancellation was not preserved: %v %v", err, factory.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("source producer ignored cancellation")
	}
}

func TestFactoryPlanEscapesColumnNames(t *testing.T) {
	name := "quoted\"\\column\n"
	schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int64}}, nil)
	conn, factory := registeredFactory(t, schema, func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
		if !reflect.DeepEqual(plan.Columns, []string{name}) || len(plan.Filters) != 1 || plan.Filters[0].Column != name || plan.Filters[0].Value != "1" {
			return nil, errors.New("escaped plan name was changed")
		}
		builder := array.NewInt64Builder(memory.NewGoAllocator())
		builder.AppendValues([]int64{2, 3}, nil)
		column := builder.NewArray()
		builder.Release()
		defer column.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{column}, 2)
		defer record.Release()
		return array.NewRecordReader(schema, []arrow.RecordBatch{record})
	})
	quoted := "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
	var sum int64
	if err := conn.QueryRowContext(context.Background(), "SELECT CAST(sum("+quoted+") AS BIGINT) FROM bridge.data WHERE "+quoted+">1").Scan(&sum); err != nil || sum != 5 {
		t.Fatalf("escaped plan failed: %d %v %v", sum, err, factory.Err())
	}
}

type panicReleaseReader struct {
	array.RecordReader
	releases *atomic.Int64
}

func (r panicReleaseReader) Release() {
	r.RecordReader.Release()
	r.releases.Add(1)
	panic("fixture-private-release-detail")
}

func TestFactoryReleasePanicsRemainInsideCallbacks(t *testing.T) {
	for _, phase := range []string{"producer_error", "stream_release"} {
		t.Run(phase, func(t *testing.T) {
			var releases atomic.Int64
			wanted := query.NewError("RESOURCE_EXHAUSTED", "Source scan exceeds its configured limit")
			conn, factory := registeredFactory(t, bridgeSchema(), func(_ context.Context, plan ScanPlan) (array.RecordReader, error) {
				reader, err := fixtureReader(t, bridgeSchema(), plan)
				if err != nil {
					return nil, err
				}
				wrapped := panicReleaseReader{RecordReader: reader, releases: &releases}
				if phase == "producer_error" {
					return wrapped, wanted
				}
				return wrapped, nil
			})
			_, err := conn.ExecContext(context.Background(), "SELECT id FROM bridge.data")
			if phase == "producer_error" && (err == nil || factory.Err() != wanted) {
				t.Fatalf("cleanup panic replaced the source error: %v %v", err, factory.Err())
			}
			if releases.Load() != 1 || factory.Err() == nil || strings.Contains(factory.Err().Error(), "fixture-private") {
				t.Fatalf("release panic escaped, leaked, or disclosed details: releases=%d error=%v callback=%v", releases.Load(), err, factory.Err())
			}
		})
	}
}
