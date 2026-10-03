// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const parquetRowGroupRows int64 = 8192
const parquetRowGroupBytes int64 = 8 << 20

// ParquetSink synchronously encodes borrowed Arrow batches. It keeps no batch
// after Write returns. Pages and row groups are bounded independently of the
// input batch, while MaxBytes includes the header, metadata and footer.
//
// This does not bound the upstream engine's query memory. Each input batch and
// the Parquet file's cumulative row-group metadata also consume memory.
type ParquetSink struct {
	expectedSchema  *arrow.Schema
	schemaEvolution *catalog.SchemaEvolution
	out             *parquetCountingWriter
	limits          query.Limits
	schema          *arrow.Schema
	writer          *pqarrow.FileWriter
	rows            int64
	groupBytes      int64
	groups          int64
	maxGroups       int64
	finished        bool
	err             error
}

var _ query.Sink = (*ParquetSink)(nil)

func NewParquetSink(out io.Writer, limits query.Limits) *ParquetSink {
	s := &ParquetSink{out: &parquetCountingWriter{out: out, limit: limits.MaxBytes}, limits: limits}
	if out == nil || limits.MaxRows <= 0 || limits.MaxBytes <= 0 || limits.MemoryMB <= 0 {
		s.err = errors.New("Parquet sink requires an output and positive row, byte and memory limits")
		return s
	}
	// Clamp before multiplying to avoid overflow for callers that have not run
	// Limits.Validate. Leave room for page encoders and the source's batch.
	s.groupBytes = min(parquetRowGroupBytes, limits.MaxBytes, int64(min(limits.MemoryMB, 32))<<18)
	return s
}

func (s *ParquetSink) Schema(schema *arrow.Schema) (err error) {
	if s.err != nil {
		return s.err
	}
	if s.finished {
		return errors.New("Parquet sink is finished")
	}
	if schema == nil || schema.NumFields() == 0 || schema.NumFields() > 4096 {
		return s.fail(errors.New("Parquet sink requires a schema with 1 to 4096 columns"))
	}
	if s.schema != nil {
		if SchemaEqual(s.schema, schema) {
			return nil
		}
		return s.fail(errors.New("Parquet schema changed"))
	}
	if s.expectedSchema != nil {
		if err := CheckSchemaEvolution(s.expectedSchema, schema, s.schemaEvolution); err != nil {
			return s.fail(err)
		}
	}
	names := make(map[string]bool, schema.NumFields())
	groupMetadata := int64(schema.NumFields()+1) * 1024
	for _, field := range schema.Fields() {
		name := strings.ToLower(field.Name)
		if name == "" || names[name] {
			return s.fail(errors.New("Parquet column names must be nonempty and distinct ignoring case; alias them in the source query"))
		}
		names[name] = true
		// Column paths are repeated in each row group's metadata.
		groupMetadata += int64(len(field.Name)) * 2
		if err := parquetFieldType(field.Type); err != nil {
			return s.fail(fmt.Errorf("Parquet column %q: %w", field.Name, err))
		}
	}
	if _, reserved := schema.Metadata().GetValue("ARROW:schema"); reserved {
		return s.fail(errors.New("Parquet input schema contains reserved ARROW:schema metadata"))
	}
	if _, err := SchemaFingerprint(schema); err != nil {
		return s.fail(err)
	}
	// Arrow retains footer metadata until Finish. Bound its growth as well as
	// each group's payload. The 1 KiB/column estimate (with capped statistics)
	// is a conservative work limit, not an allocator or process RSS guarantee.
	metadataBudget := int64(min(s.limits.MemoryMB, 1<<20)) << 17
	s.maxGroups = min(int64(65536), metadataBudget/groupMetadata)
	if s.maxGroups < 1 {
		return s.fail(query.NewError("RESOURCE_EXHAUSTED", "Acceleration schema exceeds Parquet metadata memory limit"))
	}
	// Arrow's Parquet constructor panics if writing its magic header fails.
	// Recover only that I/O failure; programming panics remain visible.
	defer func() {
		if recovered := recover(); recovered != nil {
			if s.out.err == nil {
				panic(recovered)
			}
			err = s.fail(s.out.err)
		}
	}()
	s.writer, err = pqarrow.NewFileWriter(schema, s.out, parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_6),
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(false),
		parquet.WithMaxRowGroupLength(parquetRowGroupRows),
		parquet.WithBatchSize(256),
		parquet.WithDataPageSize(64<<10),
		parquet.WithMaxStatsSize(256),
	), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		return s.fail(err)
	}
	if s.out.err != nil {
		return s.fail(s.out.err)
	}
	s.schema = schema
	return nil
}

func (s *ParquetSink) Write(record arrow.RecordBatch) error {
	if s.err != nil {
		return s.err
	}
	if s.finished {
		return errors.New("Parquet sink is finished")
	}
	if s.schema == nil || record == nil || !SchemaEqual(s.schema, record.Schema()) {
		return s.fail(errors.New("Parquet record requires the declared schema"))
	}
	if record.NumRows() > s.limits.MaxRows-s.rows {
		return s.fail(query.NewError("RESOURCE_EXHAUSTED", "Acceleration exceeds row limit"))
	}
	if err := parquetRecordValues(record); err != nil {
		return s.fail(err)
	}
	for start := int64(0); start < record.NumRows(); {
		if s.groups >= s.maxGroups {
			return s.fail(query.NewError("RESOURCE_EXHAUSTED", "Acceleration exceeds Parquet row-group metadata limit"))
		}
		length := min(parquetRowGroupRows, record.NumRows()-start)
		for !parquetRangeFits(record, start, start+length, s.groupBytes) {
			if length == 1 {
				return s.fail(query.NewError("RESOURCE_EXHAUSTED", "Acceleration row exceeds Parquet memory limit"))
			}
			length = (length + 1) / 2
		}
		part := record.NewSlice(start, start+length)
		err := s.writer.Write(part)
		part.Release()
		// Some Arrow writer paths do not propagate footer/close errors; the
		// counting writer records the first error independently.
		if s.out.err != nil {
			return s.fail(s.out.err)
		}
		if err != nil {
			return s.fail(err)
		}
		s.rows += length
		s.groups++
		start += length
	}
	return nil
}

// Finish writes the footer, including for a zero-row result. It is idempotent
// and never closes the caller's output. Failed sinks cannot finish successfully;
// callers must discard their partial output rather than publish it.
func (s *ParquetSink) Finish() error {
	if s.err != nil {
		return s.err
	}
	if s.finished {
		return nil
	}
	if s.writer == nil {
		return s.fail(errors.New("Parquet sink has no schema"))
	}
	s.finished = true
	err := s.writer.Close()
	if s.out.err != nil {
		return s.fail(s.out.err)
	}
	if err != nil {
		return s.fail(err)
	}
	return nil
}

// Abort releases encoder resources after an upstream failure. It does not write
// a footer and is safe to defer immediately after construction, including when
// Finish succeeds. The caller still owns and must discard incomplete output.
func (s *ParquetSink) Abort() {
	if !s.finished && s.err == nil {
		_ = s.fail(errors.New("Parquet sink aborted"))
	}
}

// Rows is the number of rows in successfully written row groups.
func (s *ParquetSink) Rows() int64 { return s.rows }

func (s *ParquetSink) fail(err error) error {
	if s.err == nil {
		s.err = err
		// Block any more output before closing to release encoder buffers. The
		// wrapper deliberately does not implement io.Closer.
		s.out.err = err
		if s.writer != nil {
			_ = s.writer.Close()
		}
	}
	return s.err
}

// Restrict the initial cache to scalar types whose values DuckDB's Parquet
// reader preserves. Arrow schema metadata retains integer widths, nullability,
// decimal precision/scale and supported timestamp annotations for Arrow readers.
// DuckDB promotes second/millisecond timestamps to microseconds and normalizes
// UTC aliases. It has no named-zone or zoned-nanosecond type. NULL-typed columns
// must be explicitly CAST in the source query before acceleration.
func parquetFieldType(typ arrow.DataType) error {
	switch t := typ.(type) {
	case *arrow.BooleanType, *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type,
		*arrow.Float32Type, *arrow.Float64Type, *arrow.StringType, *arrow.BinaryType, *arrow.Date32Type:
		return nil
	case *arrow.Decimal128Type:
		if t.Precision >= 1 && t.Precision <= 38 && t.Scale >= 0 && t.Scale <= t.Precision {
			return nil
		}
	case *arrow.TimestampType:
		if t.Unit < arrow.Second || t.Unit > arrow.Nanosecond {
			break
		}
		if t.TimeZone != "" && t.TimeZone != "UTC" && t.TimeZone != "Etc/UTC" {
			return errors.New("named timestamp timezones are not preserved by DuckDB; convert to UTC explicitly")
		}
		if t.TimeZone != "" && t.Unit == arrow.Nanosecond {
			return errors.New("zoned nanosecond timestamps lose precision in DuckDB")
		}
		return nil
	}
	return fmt.Errorf("Arrow type %v is not supported for lossless acceleration; CAST explicitly in the source query", typ)
}

func parquetRecordValues(record arrow.RecordBatch) error {
	for col, values := range record.Columns() {
		field := record.Schema().Field(col)
		if !field.Nullable && values.NullN() != 0 {
			return fmt.Errorf("Parquet column %q is non-nullable but contains NULL", field.Name)
		}
		switch values := values.(type) {
		case *array.Timestamp:
			factor := int64(1)
			switch field.Type.(*arrow.TimestampType).Unit {
			case arrow.Second:
				factor = 1_000_000
			case arrow.Millisecond:
				factor = 1_000
			}
			// DuckDB reserves +/- MaxInt64 as infinity. Check before Arrow's
			// unchecked second->millisecond multiplication and DuckDB's
			// second/millisecond->microsecond conversion.
			bound := (int64(math.MaxInt64) - 1) / factor
			for row, value := range values.TimestampValues() {
				if values.IsValid(row) && (int64(value) < -bound || int64(value) > bound) {
					return fmt.Errorf("Parquet timestamp column %q exceeds DuckDB's finite range", field.Name)
				}
			}
		case *array.Date32:
			for row, value := range values.Date32Values() {
				if values.IsValid(row) && (value <= -math.MaxInt32 || value >= math.MaxInt32) {
					return fmt.Errorf("Parquet date column %q exceeds DuckDB's finite range", field.Name)
				}
			}
		case *array.Decimal128:
			precision := field.Type.(*arrow.Decimal128Type).Precision
			for row, value := range values.Values() {
				if values.IsValid(row) && !value.FitsInPrecision(precision) {
					return fmt.Errorf("Parquet decimal column %q exceeds declared precision", field.Name)
				}
			}
		}
	}
	return nil
}

// Estimate a slice from Arrow offsets without allocating or visiting every cell.
// Include validity, offsets and at least one byte for a boolean value; this is
// conservative relative to Arrow's bit-packed buffers.
func parquetRangeFits(record arrow.RecordBatch, start, end, remaining int64) bool {
	rows := end - start
	for _, col := range record.Columns() {
		var size int64
		switch values := col.(type) {
		case *array.String:
			offsets := values.ValueOffsets()
			size = int64(offsets[end]) - int64(offsets[start]) + (rows+1)*4 + rows
		case *array.Binary:
			offsets := values.ValueOffsets()
			size = int64(offsets[end]) - int64(offsets[start]) + (rows+1)*4 + rows
		default:
			width := int64((col.DataType().(arrow.FixedWidthDataType).BitWidth() + 7) / 8)
			size = rows * (width + 1)
		}
		if size < 0 || size > remaining {
			return false
		}
		remaining -= size
	}
	return true
}

type parquetCountingWriter struct {
	out          io.Writer
	limit, bytes int64
	err          error
}

func (w *parquetCountingWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if int64(len(p)) > w.limit-w.bytes {
		w.err = query.NewError("RESOURCE_EXHAUSTED", "Acceleration exceeds encoded Parquet byte limit")
		return 0, w.err
	}
	n, err := w.out.Write(p)
	if n < 0 || n > len(p) {
		n, err = 0, errors.New("invalid Parquet output write count")
	}
	w.bytes += int64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}
