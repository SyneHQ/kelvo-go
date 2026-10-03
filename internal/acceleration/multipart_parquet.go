// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"errors"
	"io"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const multipartPartRowTarget int64 = 1 << 20

// MultipartParquetSink writes one bounded Parquet encoder at a time. Encoded
// limits include headers/footers; the uncompressed rotation target is only a
// conservative work estimate, never permission to exceed an encoded limit.
// Borrowed record slices are released synchronously. Input batch and upstream
// engine memory are still owned and bounded by their respective callers.
type MultipartParquetSink struct {
	tx                 MultipartRefreshWriter
	options            MultipartOptions
	limits             query.Limits
	expectedSchema     *arrow.Schema
	schemaEvolution    *catalog.SchemaEvolution
	schema             *arrow.Schema
	active             *ParquetSink
	total              *parquetCountingWriter
	parts              int
	rows, partRawBytes int64
	finished           bool
	err                error
}

var _ query.Sink = (*MultipartParquetSink)(nil)

func NewMultipartParquetSink(tx MultipartRefreshWriter, limits query.Limits, options MultipartOptions) *MultipartParquetSink {
	s := &MultipartParquetSink{tx: tx, limits: limits, options: options}
	if tx == nil || options.MaxParts < 1 || options.MaxParts > 256 || options.MaxPartBytes < 1 || options.MaxTotalBytes < 1 || limits.MaxBytes < 1 || limits.MaxRows < 1 || limits.MemoryMB < 1 {
		s.err = errors.New("multipart Parquet sink requires bounded writer and limits")
		return s
	}
	s.total = &parquetCountingWriter{limit: min(options.MaxTotalBytes, limits.MaxBytes)}
	return s
}
func (s *MultipartParquetSink) Schema(schema *arrow.Schema) error {
	if s.err != nil {
		return s.err
	}
	if s.finished {
		return errors.New("multipart Parquet sink is finished")
	}
	if s.schema != nil {
		if SchemaEqual(s.schema, schema) {
			return nil
		}
		return s.fail(ErrSchemaMismatch)
	}
	// Check the cross-generation contract once, before creating any part. Each
	// leaf receives this same schema; record and sealed-part checks remain exact.
	if s.expectedSchema != nil {
		if err := CheckSchemaEvolution(s.expectedSchema, schema, s.schemaEvolution); err != nil {
			return s.fail(err)
		}
	}
	s.schema = schema
	if err := s.openPart(); err != nil {
		return s.fail(err)
	}
	return nil
}
func (s *MultipartParquetSink) openPart() error {
	if s.parts >= s.options.MaxParts {
		return query.NewError("RESOURCE_EXHAUSTED", "Acceleration exceeds multipart part count")
	}
	if err := s.tx.Context().Err(); err != nil {
		return err
	}
	remaining := s.total.limit - s.total.bytes
	if remaining <= 0 {
		return query.NewError("RESOURCE_EXHAUSTED", "Acceleration exceeds multipart total encoded byte limit")
	}
	file, err := s.tx.NewPart()
	if err != nil {
		return err
	}
	// Switching the counting writer's output preserves the cumulative encoded
	// count. Its first failure remains sticky across all parts.
	s.total.out = file
	limits := s.limits
	limits.MaxBytes = min(s.options.MaxPartBytes, remaining)
	s.active = NewParquetSink(s.total, limits)
	if err := s.active.Schema(s.schema); err != nil {
		return err
	}
	s.parts++
	s.partRawBytes = 0
	return nil
}
func (s *MultipartParquetSink) sealPart() error {
	if s.active == nil {
		return nil
	}
	if err := s.active.Finish(); err != nil {
		return err
	}
	if err := s.tx.SealPart(s.active.Rows()); err != nil {
		return err
	}
	s.active = nil
	s.total.out = nil
	s.partRawBytes = 0
	return nil
}
func (s *MultipartParquetSink) Write(record arrow.RecordBatch) error {
	if s.err != nil {
		return s.err
	}
	if s.finished {
		return errors.New("multipart Parquet sink is finished")
	}
	if s.schema == nil || record == nil || record.NumRows() < 0 || !SchemaEqual(s.schema, record.Schema()) {
		return s.fail(errors.New("multipart record requires declared schema"))
	}
	if record.NumRows() > s.limits.MaxRows-s.rows {
		return s.fail(query.NewError("RESOURCE_EXHAUSTED", "Acceleration exceeds row limit"))
	}
	for start := int64(0); start < record.NumRows(); {
		if err := s.tx.Context().Err(); err != nil {
			return s.fail(err)
		}
		if s.active == nil {
			if err := s.openPart(); err != nil {
				return s.fail(err)
			}
		}
		target := max(int64(1), s.options.MaxPartBytes/2)
		remaining := target - s.partRawBytes
		if remaining <= 0 || s.active.Rows() >= multipartPartRowTarget || s.active.groups >= s.active.maxGroups {
			if err := s.sealPart(); err != nil {
				return s.fail(err)
			}
			continue
		}
		length := min(parquetRowGroupRows, record.NumRows()-start, multipartPartRowTarget-s.active.Rows())
		budget := min(remaining, s.active.groupBytes)
		for !parquetRangeFits(record, start, start+length, budget) && length > 1 {
			length = (length + 1) / 2
		}
		if !parquetRangeFits(record, start, start+length, budget) {
			if s.active.Rows() > 0 {
				if err := s.sealPart(); err != nil {
					return s.fail(err)
				}
				continue
			}
			return s.fail(query.NewError("RESOURCE_EXHAUSTED", "Acceleration row exceeds multipart uncompressed budget"))
		}
		rawBytes := multipartSliceBytes(record, start, start+length)
		part := record.NewSlice(start, start+length)
		err := s.active.Write(part)
		part.Release()
		if err != nil {
			return s.fail(err)
		}
		s.rows += length
		s.partRawBytes += rawBytes
		start += length
	}
	return nil
}

// Same scalar estimate used by parquetRangeFits after schema validation. A
// chunk never exceeds 8192 rows, so fixed-width multiplication is bounded.
func multipartSliceBytes(record arrow.RecordBatch, start, end int64) int64 {
	rows := end - start
	var total int64
	for _, column := range record.Columns() {
		switch values := column.(type) {
		case *array.String:
			offsets := values.ValueOffsets()
			total += int64(offsets[end]) - int64(offsets[start]) + (rows+1)*4 + rows
		case *array.Binary:
			offsets := values.ValueOffsets()
			total += int64(offsets[end]) - int64(offsets[start]) + (rows+1)*4 + rows
		default:
			total += rows * (int64((column.DataType().(arrow.FixedWidthDataType).BitWidth()+7)/8) + 1)
		}
	}
	return total
}
func (s *MultipartParquetSink) Finish() error {
	if s.err != nil {
		return s.err
	}
	if s.finished {
		return nil
	}
	if s.schema == nil {
		return s.fail(errors.New("multipart Parquet sink has no schema"))
	}
	if err := s.tx.Context().Err(); err != nil {
		return s.fail(err)
	}
	if err := s.sealPart(); err != nil {
		return s.fail(err)
	}
	s.finished = true
	return nil
}
func (s *MultipartParquetSink) Abort() {
	if !s.finished && s.err == nil {
		s.fail(errors.New("multipart Parquet sink aborted"))
	}
}
func (s *MultipartParquetSink) Rows() int64 { return s.rows }
func (s *MultipartParquetSink) fail(err error) error {
	if s.err == nil {
		s.err = err
		if s.active != nil {
			s.active.Abort()
		}
		if s.total != nil {
			s.total.err = err
			s.total.out = io.Discard
		}
	}
	return s.err
}
