// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package access

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func policyRecord(t *testing.T) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	meta := arrow.MetadataFrom(map[string]string{"private": "must-not-reach-query"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64, Metadata: meta},
		{Name: "tenant_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 38, Scale: 2}, Nullable: true},
		{Name: "secret", Type: arrow.BinaryTypes.String},
	}, &meta)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Uint64Builder).AppendValues([]uint64{1, 9007199254740993, 18446744073709551615, 100}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{7, 7, 8, 0}, []bool{true, true, true, false})
	b.Field(2).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(123), decimal128.FromI64(456), decimal128.FromI64(999), decimal128.FromI64(0)}, []bool{true, true, true, false})
	b.Field(3).(*array.StringBuilder).AppendValues([]string{"alpha", "Beta", "beta", "é"}, nil)
	record := b.NewRecordBatch()
	t.Cleanup(record.Release)
	return record
}
func recordProducer(record arrow.RecordBatch, calls *atomic.Int32) Producer {
	return func(ctx context.Context, plan federationapi.ScanPlan) (array.RecordReader, error) {
		if calls != nil {
			calls.Add(1)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(plan.Filters) != 0 {
			return nil, errors.New("policy delegated to adapter")
		}
		fields := make([]arrow.Field, len(plan.Columns))
		cols := make([]arrow.Array, len(plan.Columns))
		for i, name := range plan.Columns {
			indices := record.Schema().FieldIndices(name)
			if len(indices) != 1 {
				return nil, errors.New("unknown projection")
			}
			fields[i] = record.Schema().Field(indices[0])
			cols[i] = record.Column(indices[0])
		}
		meta := record.Schema().Metadata()
		schema := arrow.NewSchema(fields, &meta)
		batch := array.NewRecordBatch(schema, cols, record.NumRows())
		defer batch.Release()
		return array.NewRecordReader(schema, []arrow.RecordBatch{batch})
	}
}
func rowPolicy() TablePolicy {
	return TablePolicy{Columns: []string{"id", "amount"}, Rows: &Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"}}
}
func readIDs(t *testing.T, r array.RecordReader) []uint64 {
	t.Helper()
	defer r.Release()
	var ids []uint64
	for r.Next() {
		values := r.RecordBatch().Column(0).(*array.Uint64)
		for i := 0; i < values.Len(); i++ {
			ids = append(ids, values.Value(i))
		}
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestGuardAppliesPolicyAndPushedFiltersBeforeProjection(t *testing.T) {
	record := policyRecord(t)
	var calls atomic.Int32
	relation, err := NewRelation(record.Schema(), rowPolicy(), recordProducer(record, &calls))
	if err != nil {
		t.Fatal(err)
	}
	if relation.Schema().NumFields() != 2 || relation.Schema().Metadata().Len() != 0 || relation.Schema().Field(0).Metadata.Len() != 0 {
		t.Fatal("private schema exposed")
	}
	reader, err := relation.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"id", "amount"}, Filters: []federationapi.Filter{{Kind: "comparison", Column: "id", Type: "uint64", Op: "ge", Value: "9007199254740993"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !reader.Next() {
		t.Fatal(reader.Err())
	}
	batch := reader.RecordBatch()
	if batch.NumRows() != 1 || batch.NumCols() != 2 || batch.Column(0).(*array.Uint64).Value(0) != 9007199254740993 || batch.Column(1).(*array.Decimal128).Value(0) != decimal128.FromI64(456) {
		t.Fatal("policy/filter changed exact values")
	}
	batch.Retain()
	reader.Release()
	if batch.Column(0).(*array.Uint64).Value(0) != 9007199254740993 {
		t.Fatal("retained batch lifetime changed")
	}
	batch.Release()
	reader, err = relation.Scan(context.Background(), federationapi.ScanPlan{})
	if err != nil {
		t.Fatal(err)
	}
	if got := readIDs(t, reader); !reflect.DeepEqual(got, []uint64{1, 9007199254740993}) {
		t.Fatal("count-only scan bypassed rows", got)
	}
	if calls.Load() != 2 {
		t.Fatal("unexpected source scans")
	}
}

func TestGuardRejectsHiddenColumnsAndUnsupportedFiltersBeforeScan(t *testing.T) {
	record := policyRecord(t)
	var calls atomic.Int32
	relation, err := NewRelation(record.Schema(), rowPolicy(), recordProducer(record, &calls))
	if err != nil {
		t.Fatal(err)
	}
	plans := []federationapi.ScanPlan{
		{Columns: []string{"secret"}},
		{Columns: []string{"id"}, Filters: []federationapi.Filter{{Kind: "is_null", Column: "tenant_id"}}},
		{Columns: []string{"id"}, Filters: []federationapi.Filter{{Kind: "comparison", Column: "id", Type: "int64", Op: "eq", Value: "1"}}},
		{Columns: []string{"id"}, Filters: []federationapi.Filter{{Kind: "comparison", Column: "id", Type: "uint64", Op: "eq", Value: "01"}}},
		{Columns: []string{"amount"}, Filters: []federationapi.Filter{{Kind: "comparison", Column: "amount", Type: "decimal", Op: "eq", Value: "1.23"}}},
	}
	deep := federationapi.Filter{Kind: "is_null", Column: "id"}
	for i := 0; i < 40; i++ {
		deep = federationapi.Filter{Kind: "and", Children: []federationapi.Filter{deep}}
	}
	plans = append(plans, federationapi.ScanPlan{Filters: []federationapi.Filter{deep}})
	for i, plan := range plans {
		if reader, err := relation.Scan(context.Background(), plan); err == nil {
			reader.Release()
			t.Fatalf("bad scan %d accepted", i)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("denied scan reached source")
	}
}

func TestGuardNullStringAndLogicalPolicySemantics(t *testing.T) {
	record := policyRecord(t)
	cases := []struct {
		predicate Predicate
		want      []uint64
	}{
		{Predicate{Kind: "is_null", Column: "tenant_id"}, []uint64{100}},
		{Predicate{Kind: "comparison", Column: "secret", Type: "string", Op: "eq", Value: "Beta"}, []uint64{9007199254740993}},
		{Predicate{Kind: "comparison", Column: "secret", Type: "string", Op: "eq", Value: "beta"}, []uint64{18446744073709551615}},
		{Predicate{Kind: "or", Children: []Predicate{{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "8"}, {Kind: "is_null", Column: "tenant_id"}}}, []uint64{18446744073709551615, 100}},
		{Predicate{Kind: "and", Children: []Predicate{{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"}, {Kind: "comparison", Column: "secret", Type: "string", Op: "ne", Value: "Beta"}}}, []uint64{1}},
	}
	for _, tc := range cases {
		relation, err := NewRelation(record.Schema(), TablePolicy{Columns: []string{"id"}, Rows: &tc.predicate}, recordProducer(record, nil))
		if err != nil {
			t.Fatal(err)
		}
		reader, err := relation.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		if got := readIDs(t, reader); !reflect.DeepEqual(got, tc.want) {
			t.Fatal("predicate mismatch", got, tc.want)
		}
	}
}

func TestGuardSchemaDriftAndComplexColumnsFailClosed(t *testing.T) {
	record := policyRecord(t)
	for _, kind := range []arrow.DataType{arrow.StructOf(arrow.Field{Name: "private", Type: arrow.PrimitiveTypes.Int64}), &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}} {
		schema := arrow.NewSchema([]arrow.Field{{Name: "nested", Type: kind}}, nil)
		if _, err := NewRelation(schema, TablePolicy{Columns: []string{"nested"}, AllRows: true}, recordProducer(record, nil)); err == nil {
			t.Fatal("complex projection accepted")
		}
	}
	bad := rowPolicy()
	bad.Rows.Type = "uint64"
	if _, err := NewRelation(record.Schema(), bad, recordProducer(record, nil)); err == nil {
		t.Fatal("row type coercion accepted")
	}
	relation, err := NewRelation(record.Schema(), rowPolicy(), func(context.Context, federationapi.ScanPlan) (array.RecordReader, error) {
		return array.NewRecordReader(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil), nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	if reader, err := relation.Scan(context.Background(), federationapi.ScanPlan{}); err == nil {
		reader.Release()
		t.Fatal("changed source schema accepted")
	}
}

func TestGuardConcurrentRescansAndCancellation(t *testing.T) {
	record := policyRecord(t)
	var calls atomic.Int32
	relation, err := NewRelation(record.Schema(), rowPolicy(), recordProducer(record, &calls))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := relation.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"id"}})
			if err != nil {
				failures <- err
				return
			}
			defer r.Release()
			var rows int64
			for r.Next() {
				rows += r.RecordBatch().NumRows()
			}
			if r.Err() != nil {
				failures <- r.Err()
			} else if rows != 2 {
				failures <- errors.New("rescan policy bypass")
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if reader, err := relation.Scan(ctx, federationapi.ScanPlan{}); err == nil {
		reader.Release()
		t.Fatal("canceled scan started")
	}
	if calls.Load() != 16 {
		t.Fatal("unexpected scan count")
	}
}

func TestGuardExactPrimitivePolicyComparisons(t *testing.T) {
	types := []struct {
		name  string
		dtype arrow.DataType
		value string
		want  []uint64
	}{
		{"int8", arrow.PrimitiveTypes.Int8, "127", []uint64{2}},
		{"int64", arrow.PrimitiveTypes.Int64, "9223372036854775807", []uint64{2}},
		{"uint8", arrow.PrimitiveTypes.Uint8, "255", []uint64{2}},
		{"uint64", arrow.PrimitiveTypes.Uint64, "18446744073709551615", []uint64{2}},
		{"bool", arrow.FixedWidthTypes.Boolean, "true", []uint64{1, 2}},
	}
	for _, tc := range types {
		t.Run(tc.name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "row", Type: arrow.PrimitiveTypes.Uint64}, {Name: "value", Type: tc.dtype, Nullable: true}}, nil)
			b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			defer b.Release()
			b.Field(0).(*array.Uint64Builder).AppendValues([]uint64{0, 1, 2, 3}, nil)
			valid := []bool{true, true, true, false}
			switch col := b.Field(1).(type) {
			case *array.Int8Builder:
				col.AppendValues([]int8{-128, 0, 127, 127}, valid)
			case *array.Int64Builder:
				col.AppendValues([]int64{-9223372036854775808, 9007199254740993, 9223372036854775807, 9223372036854775807}, valid)
			case *array.Uint8Builder:
				col.AppendValues([]uint8{0, 1, 255, 255}, valid)
			case *array.Uint64Builder:
				col.AppendValues([]uint64{0, 9007199254740993, 18446744073709551615, 18446744073709551615}, valid)
			case *array.BooleanBuilder:
				col.AppendValues([]bool{false, true, true, true}, valid)
			}
			record := b.NewRecordBatch()
			defer record.Release()
			policy := TablePolicy{Columns: []string{"row"}, Rows: &Predicate{Kind: "comparison", Column: "value", Type: tc.name, Op: "ge", Value: tc.value}}
			relation, err := NewRelation(schema, policy, recordProducer(record, nil))
			if err != nil {
				t.Fatal(err)
			}
			r, err := relation.Scan(context.Background(), federationapi.ScanPlan{})
			if err != nil {
				t.Fatal(err)
			}
			if got := readIDs(t, r); !reflect.DeepEqual(got, tc.want) {
				t.Fatal("primitive comparison lost width, sign, or NULL", got, tc.want)
			}
		})
	}
}

func TestGuardIPCContainsNoExcludedRowsColumnsOrMetadata(t *testing.T) {
	const excludedRow = "EXCLUDED_ROW_4e9a9f7c6b844bef9b2596d0278c69a9"
	const hiddenColumn = "HIDDEN_COLUMN_f1b7dd631df5487da6c0461bd6a9ca89"
	const hiddenMetadata = "HIDDEN_METADATA_8ed911aac3994637acb7025467fb518e"
	meta := arrow.MetadataFrom(map[string]string{"private": hiddenMetadata})
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String, Metadata: meta}, {Name: "tenant", Type: arrow.PrimitiveTypes.Int64}, {Name: "private", Type: arrow.BinaryTypes.String}}, &meta)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues([]string{excludedRow, "authorized", excludedRow}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{8, 7, 8}, nil)
	b.Field(2).(*array.StringBuilder).AppendValues([]string{hiddenColumn, hiddenColumn, hiddenColumn}, nil)
	record := b.NewRecordBatch()
	defer record.Release()
	rule := TablePolicy{Columns: []string{"value"}, Rows: &Predicate{Kind: "comparison", Column: "tenant", Type: "int64", Op: "eq", Value: "7"}}
	relation, err := NewRelation(schema, rule, recordProducer(record, nil))
	if err != nil {
		t.Fatal(err)
	}
	r, err := relation.Scan(context.Background(), federationapi.ScanPlan{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	var encoded bytes.Buffer
	w := ipc.NewWriter(&encoded, ipc.WithSchema(r.Schema()))
	rows := int64(0)
	for r.Next() {
		rows += r.RecordBatch().NumRows()
		if err = w.Write(r.RecordBatch()); err != nil {
			t.Fatal(err)
		}
	}
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || !bytes.Contains(encoded.Bytes(), []byte("authorized")) {
		t.Fatal("authorized row missing")
	}
	for _, sentinel := range []string{excludedRow, hiddenColumn, hiddenMetadata} {
		if bytes.Contains(encoded.Bytes(), []byte(sentinel)) {
			t.Fatal("private backing buffers or metadata serialized")
		}
	}
}

type blockedAccessReader struct {
	ctx               context.Context
	schema            *arrow.Schema
	started, released chan struct{}
	once              sync.Once
}

func (r *blockedAccessReader) Retain()                        {}
func (r *blockedAccessReader) Release()                       { close(r.released) }
func (r *blockedAccessReader) Schema() *arrow.Schema          { return r.schema }
func (r *blockedAccessReader) RecordBatch() arrow.RecordBatch { return nil }
func (r *blockedAccessReader) Record() arrow.RecordBatch      { return nil }
func (r *blockedAccessReader) Err() error                     { return r.ctx.Err() }
func (r *blockedAccessReader) Next() bool {
	r.once.Do(func() { close(r.started) })
	<-r.ctx.Done()
	return false
}

func TestGuardReleaseCancelsBlockedProducer(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	started, released := make(chan struct{}), make(chan struct{})
	relation, err := NewRelation(schema, TablePolicy{Columns: []string{"id"}, AllRows: true}, func(ctx context.Context, _ federationapi.ScanPlan) (array.RecordReader, error) {
		return &blockedAccessReader{ctx: ctx, schema: schema, started: started, released: released}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := relation.Scan(context.Background(), federationapi.ScanPlan{})
	if err != nil {
		t.Fatal(err)
	}
	nextDone := make(chan struct{})
	go func() { r.Next(); close(nextDone) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("producer did not start")
	}
	releaseDone := make(chan struct{})
	go func() { r.Release(); close(releaseDone) }()
	for _, done := range []<-chan struct{}{nextDone, releaseDone, released} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("release did not cancel blocked producer")
		}
	}
}

type filterCancellationContext struct {
	context.Context
	cancel context.CancelFunc
	polls  atomic.Int32
}

func (c *filterCancellationContext) Err() error {
	if c.polls.Add(1) == 2 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestGuardObservesCancellationWhileFiltering(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	b := array.NewInt64Builder(mem)
	b.AppendValues(make([]int64, 4096), nil)
	column := b.NewArray()
	b.Release()
	defer column.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 4096)
	defer record.Release()
	predicate, err := bindPredicate(Predicate{Kind: "comparison", Column: "id", Type: "int64", Op: "ge", Value: "0"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	controlled := &filterCancellationContext{Context: ctx, cancel: cancel}
	reader := &guardedReader{ctx: controlled, schema: schema, indices: []int{0}, predicates: []*boundPredicate{predicate}}
	filtered, err := reader.filter(record)
	if filtered != nil {
		filtered.Release()
		t.Fatal("canceled filtering produced output")
	}
	if !errors.Is(err, context.Canceled) || controlled.polls.Load() != 2 {
		t.Fatal("filter did not observe mid-batch cancellation", err)
	}
}
