// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"sync"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func snapshotUnavailable() error {
	return query.NewError("DATASET_UNAVAILABLE", "Guarded snapshot data is unavailable or invalid")
}
func snapshotLimit() error {
	return query.NewError("RESOURCE_EXHAUSTED", "Snapshot scan exceeds its resource limit")
}

type snapshotTable struct {
	source      catalog.Source
	schema      *arrow.Schema
	columns     map[string]int
	limits      query.Limits
	memory      *snapshotAllocator
	parts       []catalog.LocalSnapshotPart
	scan        catalog.SnapshotScanLimits
	schemaHash  string
	mu          sync.Mutex
	rows, bytes int64
}

// NewSnapshot exposes only an authenticated immutable-generation relation. Its
// private scan executor never accepts SQL, cloud credentials or a public driver.
func NewSnapshot(ctx context.Context, source catalog.Source, limits query.Limits) (result *Table, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if result != nil {
				_ = result.Close()
				result = nil
			}
			err = snapshotPanic(recovered)
		}
		if err != nil && ctx.Err() != nil {
			err = query.PublicError(ctx.Err())
		}
	}()
	if limits.Validate() != nil || (source.LocalSnapshot == nil) == (source.ObjectSnapshot == nil) ||
		source.ValidateLocalSnapshot() != nil || source.ValidateObjectSnapshot() != nil {
		return nil, snapshotUnavailable()
	}
	policy, restricted, allowed := access.Lookup(ctx, source.ID, source.ID)
	if !restricted || !allowed {
		return nil, query.NewError("PERMISSION_DENIED", "Query access denied")
	}
	// Retain immutable provenance even if the caller later reuses its envelope.
	source.ParquetPaths = append([]string(nil), source.ParquetPaths...)
	budget := budgetFromContext(ctx)
	snapshot := &snapshotTable{source: source, columns: make(map[string]int), limits: limits,
		memory: budget.snapshotAllocator(min(64<<20, int64(limits.MemoryMB)<<19))}
	if source.LocalSnapshot != nil {
		read := *source.LocalSnapshot
		read.Parts = append([]catalog.LocalSnapshotPart(nil), read.Parts...)
		snapshot.source.LocalSnapshot = &read
		snapshot.parts, snapshot.scan, snapshot.schemaHash = read.Parts, read.Scan, read.SchemaSHA256
	} else {
		read := *source.ObjectSnapshot
		read.Parts = append([]catalog.ObjectSnapshotPart(nil), read.Parts...)
		snapshot.source.ObjectSnapshot = &read
		if source.Range != nil {
			rangeCopy := *source.Range
			snapshot.source.Range = &rangeCopy
		}
		snapshot.source.Ranges = append([]catalog.ObjectRange(nil), source.Ranges...)
		snapshot.scan, snapshot.schemaHash = read.Scan, read.SchemaSHA256
		for _, part := range read.Parts {
			snapshot.parts = append(snapshot.parts, catalog.LocalSnapshotPart{Rows: part.Rows, Bytes: part.Bytes, SHA256: part.SHA256})
		}
	}
	if snapshot.schemaHash == "" {
		return nil, snapshotUnavailable()
	}
	for index := range snapshot.parts {
		part, openErr := snapshot.open(ctx, index)
		if openErr != nil {
			return nil, openErr
		}
		if snapshot.schema == nil {
			snapshot.schema = part.schema
		}
		matches := acceleration.SchemaEqual(snapshot.schema, part.schema)
		closeErr := part.Close()
		if !matches || closeErr != nil {
			return nil, snapshotUnavailable()
		}
	}
	for index, field := range snapshot.schema.Fields() {
		snapshot.columns[field.Name] = index
	}
	lifetime, cancel := context.WithCancel(ctx)
	limits.MaxRows, limits.MaxBytes = snapshot.scan.MaxRows, snapshot.scan.MaxBytes
	result = &Table{snapshot: snapshot, ctx: lifetime, cancel: cancel, schema: snapshot.schema,
		sourceID: source.ID, selected: catalog.FederationTable{Name: source.ID}, limits: limits,
		budget: budget, active: make(map[*scanReader]struct{}), closeDone: make(chan struct{})}
	result.guard, err = access.NewRelation(snapshot.schema, policy, result.scan)
	if err != nil {
		_ = result.Close()
		return nil, err
	}
	return result, nil
}

func snapshotPanic(value any) error {
	if _, ok := value.(snapshotMemoryExceeded); ok {
		return snapshotLimit()
	}
	return snapshotUnavailable()
}

func (s *snapshotTable) charge(rows, bytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := s.scan
	if rows < 0 || bytes < 0 || rows > limit.MaxRows-s.rows || bytes > limit.MaxBytes-s.bytes {
		return snapshotLimit()
	}
	s.rows += rows
	s.bytes += bytes
	return nil
}

func (s *snapshotTable) prepare(plan duckbridge.ScanPlan) (duckbridge.ScanPlan, *arrow.Schema, error) {
	if len(plan.Filters) != 0 || len(plan.Columns) == 0 || len(plan.Columns) > access.MaxColumns {
		return plan, nil, snapshotUnavailable()
	}
	fields := make([]arrow.Field, len(plan.Columns))
	seen := make(map[string]bool, len(fields))
	for i, name := range plan.Columns {
		index, ok := s.columns[name]
		if !ok || seen[name] {
			return plan, nil, snapshotUnavailable()
		}
		seen[name] = true
		fields[i] = s.schema.Field(index)
	}
	metadata := s.schema.Metadata()
	return plan, arrow.NewSchema(fields, &metadata), nil
}

type snapshotPartReader struct {
	file    snapshotInput
	parquet *file.Reader
	schema  *arrow.Schema
}

type snapshotInput interface {
	io.ReaderAt
	io.Closer
}

func (p *snapshotPartReader) Close() error { return errors.Join(p.parquet.Close(), p.file.Close()) }

type snapshotContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r snapshotContextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

func (s *snapshotTable) open(ctx context.Context, index int) (result *snapshotPartReader, err error) {
	if err := ctx.Err(); err != nil {
		return nil, query.PublicError(err)
	}
	part := s.parts[index]
	var input snapshotInput
	if s.source.ObjectSnapshot != nil {
		input, err = newSnapshotRange(ctx, s.source.ObjectSnapshot.Parts[index].URL, part.Bytes, part.SHA256, s.memory)
	} else {
		path := s.source.Path
		if s.source.ParquetPaths != nil {
			path = s.source.ParquetPaths[index]
		}
		file, openErr := openSnapshotFile(path)
		if openErr != nil {
			return nil, snapshotUnavailable()
		}
		info, statErr := file.Stat()
		if statErr != nil || !info.Mode().IsRegular() || info.Size() != part.Bytes {
			_ = file.Close()
			return nil, snapshotUnavailable()
		}
		input = file
	}
	if err != nil {
		return nil, query.PublicError(err)
	}
	var reader *file.Reader
	defer func() {
		if recovered := recover(); recovered != nil {
			err = snapshotPanic(recovered)
			result = nil
		}
		if result == nil {
			if reader != nil {
				_ = reader.Close()
			}
			_ = input.Close()
		}
	}()
	var trailer [8]byte
	if _, err := input.ReadAt(trailer[:], part.Bytes-8); err != nil {
		return nil, snapshotUnavailable()
	}
	footer := int64(binary.LittleEndian.Uint32(trailer[:4]))
	footerLimit := min(64<<20, int64(s.limits.MemoryMB)<<17, s.scan.MaxBytes)
	if string(trailer[4:]) != "PAR1" || footer <= 0 || footer > footerLimit || footer > part.Bytes-12 {
		return nil, snapshotLimit()
	}
	if ranges, ok := input.(*snapshotRange); ok {
		if err := ranges.verifyDigest(); err != nil {
			return nil, query.PublicError(err)
		}
	} else {
		digest := sha256.New()
		if _, err := io.CopyBuffer(digest, snapshotContextReader{ctx, io.NewSectionReader(input, 0, part.Bytes)}, make([]byte, 32<<10)); err != nil {
			return nil, query.PublicError(err)
		}
		if hex.EncodeToString(digest.Sum(nil)) != part.SHA256 {
			return nil, snapshotUnavailable()
		}
	}
	schema, err := acceleration.ReadParquetSchema(input, part.Bytes)
	if err != nil || schema == nil || schema.NumFields() == 0 || schema.NumFields() > access.MaxColumns {
		return nil, snapshotUnavailable()
	}
	fingerprint, err := acceleration.SchemaFingerprint(schema)
	if err != nil || fingerprint != s.schemaHash {
		return nil, snapshotUnavailable()
	}
	// Kelvo's persisted acceleration contract is flat. Reject nested/extension
	// fields even when hidden so Arrow column positions cannot become leaf IDs.
	for _, field := range schema.Fields() {
		switch field.Type.(type) {
		case *arrow.BooleanType, *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
			*arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type, *arrow.Float32Type, *arrow.Float64Type,
			*arrow.StringType, *arrow.BinaryType, *arrow.Date32Type, *arrow.Decimal128Type, *arrow.TimestampType:
		default:
			return nil, query.NewError("UNSUPPORTED", "Snapshot schema is unsupported for guarded reads")
		}
	}
	reader, err = file.NewParquetReader(io.NewSectionReader(input, 0, part.Bytes), file.WithReadProps(parquet.NewReaderProperties(s.memory)))
	if err != nil || reader.NumRows() != part.Rows || reader.NumRowGroups() > 65536 {
		return nil, snapshotUnavailable()
	}
	groupLimit := min(32<<20, int64(s.limits.MemoryMB)<<18, s.scan.MaxBytes)
	for index := 0; index < reader.NumRowGroups(); index++ {
		group := reader.MetaData().RowGroup(index)
		if group.NumRows() < 0 || group.NumRows() > 8192 || group.NumColumns() != schema.NumFields() || group.TotalByteSize() < 0 || group.TotalByteSize() > groupLimit {
			return nil, snapshotLimit()
		}
	}
	return &snapshotPartReader{file: input, parquet: reader, schema: schema}, nil
}

type snapshotExecution struct {
	table  *snapshotTable
	plan   duckbridge.ScanPlan
	schema *arrow.Schema
}

func (*snapshotExecution) Close() error { return nil }
func (e *snapshotExecution) Execute(ctx context.Context, _ query.Request, sink query.Sink) (stats query.Stats, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = snapshotPanic(recovered)
		}
		if err != nil && ctx.Err() != nil {
			err = query.PublicError(ctx.Err())
		}
	}()
	if err := sink.Schema(e.schema); err != nil {
		return stats, err
	}
	for index := range e.table.parts {
		if err := e.part(ctx, index, sink); err != nil {
			return stats, err
		}
	}
	return stats, ctx.Err()
}
func (e *snapshotExecution) part(ctx context.Context, index int, sink query.Sink) (err error) {
	part, err := e.table.open(ctx, index)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, part.Close()) }()
	if !acceleration.SchemaEqual(part.schema, e.table.schema) {
		return snapshotUnavailable()
	}
	reader, err := pqarrow.NewFileReader(part.parquet, pqarrow.ArrowReadProperties{BatchSize: 256, Parallel: false}, e.table.memory)
	if err != nil {
		return snapshotUnavailable()
	}
	// GetRecordReader constructs fields in background goroutines even when
	// Parallel is false. Use the public sequential column API so allocation
	// limits and malformed-file panics stay inside this execution's recovery.
	columns := make([]*pqarrow.ColumnReader, len(e.plan.Columns))
	defer func() {
		for _, column := range columns {
			if column != nil {
				column.Release()
			}
		}
	}()
	for i, name := range e.plan.Columns {
		if err := ctx.Err(); err != nil {
			return query.PublicError(err)
		}
		column, err := reader.GetColumn(ctx, e.table.columns[name])
		columns[i] = column
		if err != nil || column == nil || column.Field().Name != name || !arrow.TypeEqual(column.Field().Type, e.schema.Field(i).Type) {
			return snapshotUnavailable()
		}
	}
	var rows int64
	for {
		count, err := e.batch(ctx, columns, sink)
		if err != nil {
			return err
		}
		if count == 0 {
			break
		}
		rows += count
	}
	if rows != e.table.parts[index].Rows {
		return snapshotUnavailable()
	}
	return nil
}

func (e *snapshotExecution) batch(ctx context.Context, readers []*pqarrow.ColumnReader, sink query.Sink) (int64, error) {
	columns := make([]arrow.Array, len(readers))
	defer func() {
		for _, column := range columns {
			if column != nil {
				column.Release()
			}
		}
	}()
	var rows int64 = -1
	for i, reader := range readers {
		if err := ctx.Err(); err != nil {
			return 0, query.PublicError(err)
		}
		column, err := snapshotColumn(reader, e.table.memory)
		columns[i] = column
		if err != nil {
			return 0, err
		}
		var length int64
		if column != nil {
			length = int64(column.Len())
		}
		if length > 256 || (rows != -1 && rows != length) {
			return 0, snapshotUnavailable()
		}
		rows = length
	}
	if rows <= 0 {
		return 0, nil
	}
	// Restore original Arrow metadata before the guard; Parquet annotations
	// and predicate-only columns never enter the exposed relation.
	projected := array.NewRecordBatch(e.schema, columns, rows)
	defer projected.Release()
	return rows, sink.Write(projected)
}

func snapshotColumn(reader *pqarrow.ColumnReader, allocator *snapshotAllocator) (arrow.Array, error) {
	chunks, err := reader.NextBatch(256)
	if err != nil || chunks == nil {
		return nil, snapshotUnavailable()
	}
	defer chunks.Release()
	if chunks.Len() == 0 {
		return nil, nil
	}
	if len(chunks.Chunks()) == 1 {
		column := chunks.Chunk(0)
		column.Retain()
		return column, nil
	}
	column, err := array.Concatenate(chunks.Chunks(), allocator)
	if err != nil {
		return nil, snapshotUnavailable()
	}
	return column, nil
}
