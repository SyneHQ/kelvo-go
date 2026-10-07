// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package rowarrow converts bounded database rows into synchronous Arrow batches.
package rowarrow

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	batchRows       = 1024
	targetBatchRows = 65536
)

// Writer builds one batch at a time and borrows it synchronously to the sink.
// Calls must be sequential; a slow sink backpressures the source's Write call.
type Writer struct {
	schema                 *arrow.Schema
	limits                 query.Limits
	sink                   query.Sink
	b                      *array.RecordBuilder
	rows, bytes, batches   int64
	batchBytes, batchLimit int64
	batchTarget            int64
	batchRowLimit          int
	inBatch                int
	finished, closed       bool
	result                 query.Stats
	finishErr              error
	writeErr               error
}

func NewWriter(schema *arrow.Schema, limits query.Limits, sink query.Sink) (*Writer, error) {
	if schema == nil || sink == nil || len(schema.Fields()) == 0 {
		return nil, errors.New("nonempty schema and sink required")
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	for _, field := range schema.Fields() {
		if err := validType(field.Type); err != nil {
			return nil, err
		}
	}
	if err := sink.Schema(schema); err != nil {
		return nil, err
	}
	w := &Writer{schema: schema, limits: limits, sink: sink, b: array.NewRecordBuilder(memory.DefaultAllocator, schema), batchLimit: int64(limits.MemoryMB) << 18, batchRowLimit: batchRows}
	if limits.RowBatchTargetBytes > 0 {
		w.batchTarget = min(limits.RowBatchTargetBytes, w.batchLimit, limits.MaxBytes)
		w.batchRowLimit = targetBatchRows
	}
	return w, nil
}

func validType(t arrow.DataType) error {
	switch v := t.(type) {
	case *arrow.NullType, *arrow.StringType, *arrow.BinaryType, *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type, *arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type, *arrow.Float32Type, *arrow.Float64Type, *arrow.BooleanType, *arrow.Date32Type, *arrow.TimestampType:
		return nil
	case *arrow.Decimal128Type:
		if v.Precision < 1 || v.Precision > 38 || v.Scale > v.Precision || v.Scale < -128 {
			return errors.New("invalid decimal128 type")
		}
		return nil
	case *arrow.Decimal256Type:
		if v.Precision < 1 || v.Precision > 76 || v.Scale > v.Precision || v.Scale < -128 {
			return errors.New("invalid decimal256 type")
		}
		return nil
	default:
		return fmt.Errorf("unsupported Arrow type %s", t)
	}
}

func (w *Writer) Write(row []any) error {
	if w.writeErr != nil {
		return w.writeErr
	}
	if w.closed || w.finished {
		return errors.New("row writer is closed")
	}
	if len(row) != len(w.schema.Fields()) {
		return errors.New("row width mismatch")
	}
	if w.rows >= w.limits.MaxRows {
		return query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds row limit")
	}
	var size int64
	for i, value := range row {
		field := w.schema.Field(i)
		n, err := validateValue(field, value)
		if err != nil {
			return err
		}
		// Include one validity byte per value (conservative for bitmaps), four
		// offset bytes for variable-width columns, and the fixed-width value.
		n++
		switch field.Type.(type) {
		case *arrow.StringType, *arrow.BinaryType:
			n += 4
		}
		if n > w.limits.MaxBytes-w.bytes-size {
			return query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds byte limit")
		}
		size += n
	}
	if size > w.batchLimit {
		return query.NewError("RESOURCE_EXHAUSTED", "Query row exceeds batch memory limit")
	}
	if w.inBatch > 0 && (w.batchBytes+size > w.batchLimit || (w.batchTarget > 0 && w.batchBytes+size > w.targetBytes())) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	for i, value := range row {
		appendValue(w.b.Field(i), value)
	}
	w.rows++
	w.bytes += size
	w.batchBytes += size
	w.inBatch++
	if w.inBatch == w.batchRowLimit || (w.batchTarget > 0 && w.batchBytes >= w.targetBytes()) {
		return w.flush()
	}
	return nil
}

// targetBytes includes the pending batch in the remaining result budget. A row
// may exceed this soft target, but never the unchanged batch memory limit.
func (w *Writer) targetBytes() int64 {
	return min(w.batchTarget, w.limits.MaxBytes-w.bytes+w.batchBytes)
}

func validateValue(field arrow.Field, value any) (int64, error) {
	if value == nil {
		if !field.Nullable {
			return 0, fmt.Errorf("non-nullable column %q is null", field.Name)
		}
		if fixed, ok := field.Type.(arrow.FixedWidthDataType); ok {
			return int64((fixed.BitWidth() + 7) / 8), nil
		}
		return 0, nil
	}
	switch t := field.Type.(type) {
	case *arrow.NullType:
		return 0, fmt.Errorf("null column %q received non-null value", field.Name)
	case *arrow.StringType:
		if v, ok := value.(string); ok {
			return int64(len(v)), nil
		}
	case *arrow.BinaryType:
		if v, ok := value.([]byte); ok {
			return int64(len(v)), nil
		}
	case *arrow.Int8Type:
		if _, ok := value.(int8); ok {
			return 1, nil
		}
	case *arrow.Int16Type:
		if _, ok := value.(int16); ok {
			return 2, nil
		}
	case *arrow.Int32Type:
		if _, ok := value.(int32); ok {
			return 4, nil
		}
	case *arrow.Int64Type:
		if _, ok := value.(int64); ok {
			return 8, nil
		}
	case *arrow.Uint8Type:
		if _, ok := value.(uint8); ok {
			return 1, nil
		}
	case *arrow.Uint16Type:
		if _, ok := value.(uint16); ok {
			return 2, nil
		}
	case *arrow.Uint32Type:
		if _, ok := value.(uint32); ok {
			return 4, nil
		}
	case *arrow.Uint64Type:
		if _, ok := value.(uint64); ok {
			return 8, nil
		}
	case *arrow.Float32Type:
		if v, ok := value.(float32); ok && !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0) {
			return 4, nil
		}
	case *arrow.Float64Type:
		if v, ok := value.(float64); ok && !math.IsNaN(v) && !math.IsInf(v, 0) {
			return 8, nil
		}
	case *arrow.BooleanType:
		if _, ok := value.(bool); ok {
			return 1, nil
		}
	case *arrow.Date32Type:
		if _, ok := value.(time.Time); ok {
			return 4, nil
		}
	case *arrow.TimestampType:
		if v, ok := value.(time.Time); ok {
			if stamp, err := arrow.TimestampFromTime(v, t.Unit); err == nil && stamp.ToTime(t.Unit).Equal(v) {
				return 8, nil
			}
		}
	case *arrow.Decimal128Type:
		if _, ok := value.(decimal128.Num); ok {
			return 16, nil
		}
	case *arrow.Decimal256Type:
		if _, ok := value.(decimal256.Num); ok {
			return 32, nil
		}
	}
	return 0, fmt.Errorf("value does not match Arrow type %s", field.Type)
}

func appendValue(builder array.Builder, value any) {
	if value == nil {
		builder.AppendNull()
		return
	}
	switch b := builder.(type) {
	case *array.StringBuilder:
		b.Append(value.(string))
	case *array.BinaryBuilder:
		b.Append(value.([]byte))
	case *array.Int8Builder:
		b.Append(value.(int8))
	case *array.Int16Builder:
		b.Append(value.(int16))
	case *array.Int32Builder:
		b.Append(value.(int32))
	case *array.Int64Builder:
		b.Append(value.(int64))
	case *array.Uint8Builder:
		b.Append(value.(uint8))
	case *array.Uint16Builder:
		b.Append(value.(uint16))
	case *array.Uint32Builder:
		b.Append(value.(uint32))
	case *array.Uint64Builder:
		b.Append(value.(uint64))
	case *array.Float32Builder:
		b.Append(value.(float32))
	case *array.Float64Builder:
		b.Append(value.(float64))
	case *array.BooleanBuilder:
		b.Append(value.(bool))
	case *array.Date32Builder:
		b.Append(date32(value.(time.Time)))
	case *array.TimestampBuilder:
		ts, _ := arrow.TimestampFromTime(value.(time.Time), b.Type().(*arrow.TimestampType).Unit)
		b.Append(ts)
	case *array.Decimal128Builder:
		b.Append(value.(decimal128.Num))
	case *array.Decimal256Builder:
		b.Append(value.(decimal256.Num))
	}
}

func (w *Writer) flush() error {
	if w.inBatch == 0 {
		return nil
	}
	record := w.b.NewRecord()
	defer record.Release()
	if err := w.sink.Write(record); err != nil {
		// NewRecord has consumed the builders. Never retry or hide a failed
		// delivery by flushing the now-empty builders from Finish.
		w.writeErr = err
		return err
	}
	w.batches++
	w.inBatch = 0
	w.batchBytes = 0
	return nil
}
func (w *Writer) Finish() (query.Stats, error) {
	if w.finished {
		return w.result, w.finishErr
	}
	if w.closed {
		return query.Stats{}, errors.New("row writer is closed")
	}
	w.finished = true
	w.finishErr = w.writeErr
	if w.finishErr == nil {
		w.finishErr = w.flush()
	}
	w.result = query.Stats{Rows: w.rows, Bytes: w.bytes, Batches: w.batches}
	return w.result, w.finishErr
}
func (w *Writer) Close() {
	if !w.closed {
		w.closed = true
		if w.b != nil {
			w.b.Release()
			w.b = nil
		}
	}
}

func date32(t time.Time) arrow.Date32 {
	seconds := t.UTC().Unix()
	days := seconds / 86400
	if seconds < 0 && seconds%86400 != 0 {
		days--
	}
	return arrow.Date32(days)
}
