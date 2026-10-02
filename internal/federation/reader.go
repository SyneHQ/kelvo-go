// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

type batchHandoff struct {
	record   arrow.RecordBatch
	consumed chan struct{}
}
type scanReader struct {
	refs      atomic.Int64
	table     *Table
	ctx       context.Context
	cancel    context.CancelCauseFunc
	stopTable func() bool
	schema    *arrow.Schema
	batches   chan *batchHandoff
	done      chan struct{}
	resultErr error // written once before done closes
	mu        sync.Mutex
	current   *batchHandoff
	finished  bool
	err       error
}

func newScanReader(table *Table, parent context.Context, schema *arrow.Schema) *scanReader {
	ctx, cancel := context.WithCancelCause(parent)
	r := &scanReader{table: table, ctx: ctx, cancel: cancel, schema: schema, batches: make(chan *batchHandoff), done: make(chan struct{})}
	r.stopTable = context.AfterFunc(table.ctx, func() { cancel(context.Cause(table.ctx)) })
	r.refs.Store(1)
	return r
}
func (r *scanReader) produce(executor execution, request query.Request) {
	defer r.table.producers.Done()
	sink := &scanSink{reader: r}
	stats, err := executor.Execute(r.ctx, request, sink)
	r.table.sourceWireBytes.Add(stats.SourceWireBytes)
	closeErr := executor.Close()
	if err == nil {
		err = closeErr
	}
	if cause := context.Cause(r.ctx); cause != nil {
		err = query.PublicError(cause)
	}
	if err == nil && !sink.schemaSeen {
		err = query.NewError("QUERY_FAILED", "Federation scan returned no schema")
	}
	r.resultErr = err
	r.stopTable()
	r.table.budget.release()
	close(r.done)
	r.table.mu.Lock()
	delete(r.table.active, r)
	r.table.mu.Unlock()
}
func (r *scanReader) Retain() { r.refs.Add(1) }
func (r *scanReader) Release() {
	remaining := r.refs.Add(-1)
	if remaining < 0 {
		panic("federation reader released too many times")
	}
	if remaining != 0 {
		return
	}
	r.cancel(context.Canceled)
	r.mu.Lock()
	r.releaseCurrent()
	r.finished = true
	r.mu.Unlock()
	<-r.done
}
func (r *scanReader) Schema() *arrow.Schema { return r.schema }
func (r *scanReader) RecordBatch() arrow.RecordBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current == nil {
		return nil
	}
	return r.current.record
}
func (r *scanReader) Record() arrow.RecordBatch { return r.RecordBatch() }
func (r *scanReader) Err() error                { r.mu.Lock(); defer r.mu.Unlock(); return r.err }
func (r *scanReader) releaseCurrent() {
	if r.current != nil {
		r.current.record.Release()
		close(r.current.consumed)
		r.current = nil
	}
}
func (r *scanReader) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.releaseCurrent()
	if r.finished {
		return false
	}
	select {
	case batch := <-r.batches:
		r.current = batch
		if cause := context.Cause(r.ctx); cause != nil {
			r.releaseCurrent()
			r.finished, r.err = true, query.PublicError(cause)
			return false
		}
		return true
	case <-r.done:
		r.finished, r.err = true, r.resultErr
		return false
	case <-r.ctx.Done():
		r.finished, r.err = true, query.PublicError(context.Cause(r.ctx))
		return false
	}
}

type scanSink struct {
	reader      *scanReader
	schemaSeen  bool
	rows, bytes int64
}

func (s *scanSink) Schema(schema *arrow.Schema) error {
	if s.schemaSeen || schema == nil || !sameSchema(s.reader.schema, schema) {
		return query.NewError("QUERY_FAILED", "Federation source schema changed during scan")
	}
	s.schemaSeen = true
	return nil
}
func sameSchema(expected, actual *arrow.Schema) bool {
	return expected.Equal(actual) && expected.Metadata().Equal(actual.Metadata())
}
func (s *scanSink) Write(record arrow.RecordBatch) error {
	r := s.reader
	if !s.schemaSeen || record == nil || !sameSchema(r.schema, record.Schema()) {
		return query.NewError("QUERY_FAILED", "Federation source returned an invalid batch")
	}
	if cause := context.Cause(r.ctx); cause != nil {
		return query.PublicError(cause)
	}
	size := arrowutil.TotalRecordSize(record)
	if record.NumRows() > r.table.limits.MaxRows-s.rows || size > r.table.limits.MaxBytes-s.bytes {
		return query.NewError("RESOURCE_EXHAUSTED", "Federation scan exceeds its row or byte limit")
	}
	s.rows += record.NumRows()
	s.bytes += size
	r.table.rows.Add(record.NumRows())
	r.table.bytes.Add(size)
	r.table.batches.Add(1)
	record.Retain()
	batch := &batchHandoff{record: record, consumed: make(chan struct{})}
	select {
	case r.batches <- batch:
		// Acknowledge only after Next/Release releases our retained reference. This
		// stops the native executor decoding another batch while the consumer works.
		select {
		case <-batch.consumed:
			return nil
		case <-r.ctx.Done():
			return query.PublicError(context.Cause(r.ctx))
		}
	case <-r.ctx.Done():
		record.Release()
		return query.PublicError(context.Cause(r.ctx))
	}
}

var _ array.RecordReader = (*scanReader)(nil)
