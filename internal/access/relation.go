// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package access

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Producer func(context.Context, federationapi.ScanPlan) (array.RecordReader, error)

// Relation exposes only its authorized schema. Raw data and policy-only columns
// never enter DuckDB; every independent scan applies the guard locally.
type Relation struct {
	raw, exposed *arrow.Schema
	policy       TablePolicy
	producer     Producer
	columns      map[string]arrow.Field
	allowed      map[string]bool
}

func NewRelation(schema *arrow.Schema, policy TablePolicy, producer Producer) (*Relation, error) {
	if schema == nil || schema.NumFields() == 0 || schema.NumFields() > MaxColumns || producer == nil {
		return nil, unsupported()
	}
	if err := (&validation{}).table(policy); err != nil {
		return nil, err
	}
	r := &Relation{raw: schema, policy: cloneTable(policy), producer: producer, columns: make(map[string]arrow.Field), allowed: make(map[string]bool)}
	seen := map[string]bool{}
	for _, field := range schema.Fields() {
		folded := strings.ToLower(field.Name)
		if !validColumn(field.Name) || seen[folded] || field.Type == nil {
			return nil, unsupported()
		}
		seen[folded] = true
		r.columns[field.Name] = field
	}
	fields := make([]arrow.Field, len(policy.Columns))
	for i, name := range policy.Columns {
		field, ok := r.columns[name]
		if !ok {
			return nil, denied()
		}
		if !flatType(field.Type) {
			return nil, unsupported()
		}
		field.Metadata = arrow.Metadata{}
		fields[i] = field
		r.allowed[name] = true
	}
	r.exposed = arrow.NewSchema(fields, nil)
	if policy.Rows != nil {
		if _, err := bindPredicate(*policy.Rows, schema); err != nil {
			return nil, err
		}
		valid := true
		predicateColumns(*policy.Rows, func(name string) { field, ok := r.columns[name]; valid = valid && ok && flatType(field.Type) })
		if !valid {
			return nil, unsupported()
		}
	}
	return r, nil
}
func (r *Relation) Schema() *arrow.Schema { return r.exposed }

func (r *Relation) Scan(parent context.Context, plan federationapi.ScanPlan) (array.RecordReader, error) {
	if err := parent.Err(); err != nil {
		return nil, query.PublicError(err)
	}
	if len(plan.Columns) > MaxColumns || len(plan.Filters) > 256 {
		return nil, unsupported()
	}
	projection := append([]string(nil), plan.Columns...)
	if len(projection) == 0 {
		projection = []string{r.policy.Columns[0]}
	}
	fields := make([]arrow.Field, len(projection))
	rawColumns := []string{}
	seen := map[string]bool{}
	add := func(name string) {
		if !seen[name] {
			seen[name] = true
			rawColumns = append(rawColumns, name)
		}
	}
	for i, name := range projection {
		if !r.allowed[name] {
			return nil, denied()
		}
		field := r.columns[name]
		field.Metadata = arrow.Metadata{}
		fields[i] = field
		add(name)
	}
	var predicates []Predicate
	if r.policy.Rows != nil {
		predicates = append(predicates, *r.policy.Rows)
		predicateColumns(*r.policy.Rows, add)
	}
	validator := &validation{}
	nodes := 0
	for _, filter := range plan.Filters {
		p, err := fromPushed(filter, 0, &nodes)
		if err != nil {
			return nil, err
		}
		if err = validator.predicate(p, 0); err != nil {
			return nil, unsupported()
		}
		visible := true
		predicateColumns(p, func(name string) { visible = visible && r.allowed[name]; add(name) })
		if !visible {
			return nil, denied()
		}
		predicates = append(predicates, p)
	}
	if len(rawColumns) > MaxColumns {
		return nil, unsupported()
	}
	rawFields := make([]arrow.Field, len(rawColumns))
	positions := make(map[string]int, len(rawColumns))
	for i, name := range rawColumns {
		rawFields[i] = r.columns[name]
		positions[name] = i
	}
	metadata := r.raw.Metadata()
	expected := arrow.NewSchema(rawFields, &metadata)
	var bound []*boundPredicate
	for _, predicate := range predicates {
		p, err := bindPredicate(predicate, expected)
		if err != nil {
			return nil, err
		}
		bound = append(bound, p)
	}
	indices := make([]int, len(projection))
	for i, name := range projection {
		indices[i] = positions[name]
	}
	ctx, cancel := context.WithCancel(parent)
	// The adapter receives no policy or optimizer predicates. The local guard
	// independently enforces all of them, including filters DuckDB removed.
	input, err := r.producer(ctx, federationapi.ScanPlan{Columns: rawColumns})
	if err != nil {
		cancel()
		return nil, err
	}
	if input == nil {
		cancel()
		return nil, unsupported()
	}
	if !sameSchema(expected, input.Schema()) {
		cancel()
		input.Release()
		return nil, unsupported()
	}
	reader := &guardedReader{ctx: ctx, cancel: cancel, input: input, expected: expected, schema: arrow.NewSchema(fields, nil), predicates: bound, indices: indices}
	reader.refs.Store(1)
	return reader, nil
}

func sameSchema(expected, actual *arrow.Schema) bool {
	return expected != nil && actual != nil && expected.Equal(actual) && expected.Metadata().Equal(actual.Metadata())
}

type guardedReader struct {
	refs             atomic.Int64
	mu               sync.Mutex
	ctx              context.Context
	cancel           context.CancelFunc
	input            array.RecordReader
	expected, schema *arrow.Schema
	predicates       []*boundPredicate
	indices          []int
	current          arrow.RecordBatch
	err              error
	finished         bool
}

func (r *guardedReader) Retain() { r.refs.Add(1) }
func (r *guardedReader) Release() {
	n := r.refs.Add(-1)
	if n < 0 {
		panic("access reader released too many times")
	}
	if n != 0 {
		return
	}
	r.cancel() // Wake a Next blocked on the source before acquiring the mutex.
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	r.finished = true
	r.input.Release()
}
func (r *guardedReader) Schema() *arrow.Schema { return r.schema }
func (r *guardedReader) RecordBatch() arrow.RecordBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current
}
func (r *guardedReader) Record() arrow.RecordBatch { return r.RecordBatch() }
func (r *guardedReader) Err() error                { r.mu.Lock(); defer r.mu.Unlock(); return r.err }
func (r *guardedReader) fail(err error) bool {
	r.finished = true
	r.err = query.PublicError(err)
	r.cancel()
	return false
}
func (r *guardedReader) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.finished {
		return false
	}
	for {
		if err := r.ctx.Err(); err != nil {
			return r.fail(err)
		}
		if !r.input.Next() {
			r.finished = true
			r.err = r.input.Err()
			if err := r.ctx.Err(); err != nil {
				r.err = query.PublicError(err)
			}
			return false
		}
		record := r.input.RecordBatch()
		if record == nil || record.NumRows() < 0 || record.NumCols() != int64(r.expected.NumFields()) || !sameSchema(r.expected, record.Schema()) {
			return r.fail(unsupported())
		}
		for i, field := range r.expected.Fields() {
			column := record.Column(i)
			if column == nil || int64(column.Len()) != record.NumRows() || !arrow.TypeEqual(field.Type, column.DataType()) {
				return r.fail(unsupported())
			}
		}
		filtered, err := r.filter(record)
		if err != nil {
			return r.fail(err)
		}
		if err = r.ctx.Err(); err != nil {
			filtered.Release()
			return r.fail(err)
		}
		if filtered.NumRows() == 0 {
			filtered.Release()
			continue
		}
		r.current = filtered
		return true
	}
}

func (r *guardedReader) filter(record arrow.RecordBatch) (arrow.RecordBatch, error) {
	columns := make([]arrow.Array, len(r.indices))
	for i, index := range r.indices {
		columns[i] = record.Column(index)
	}
	projected := array.NewRecordBatch(r.schema, columns, record.NumRows())
	if len(r.predicates) == 0 {
		return projected, nil
	}
	defer projected.Release()
	mask := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer mask.Release()
	mask.Reserve(int(record.NumRows()))
	for row := 0; row < int(record.NumRows()); row++ {
		if row%1024 == 0 {
			if err := r.ctx.Err(); err != nil {
				return nil, err
			}
		}
		allowed := true
		for _, predicate := range r.predicates {
			matches, err := predicate.match(record, row)
			if err != nil {
				return nil, err
			}
			if !matches {
				allowed = false
				break
			}
		}
		mask.Append(allowed)
	}
	selection := mask.NewBooleanArray()
	defer selection.Release()
	// Only visible columns enter the Arrow selection kernel. Metadata was
	// removed before construction; hidden policy columns cannot be serialized.
	result, err := compute.FilterRecordBatch(r.ctx, projected, selection, &compute.FilterOptions{})
	if err != nil {
		return nil, unsupported()
	}
	return result, nil
}

var _ array.RecordReader = (*guardedReader)(nil)
