// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/util"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// RefreshInfo describes the last refresh this process ran for a dataset. It is
// memory only: after a restart the kind is unknown.
type RefreshInfo struct {
	Incremental  bool   `yaml:"incremental"`
	AppendedRows int64  `yaml:"appended_rows,omitempty"` // delta rows merged, including replacements
	Fallback     string `yaml:"fallback,omitempty"`
	// Fallbacks counts consecutive full fallbacks; EverIncremental reports that
	// some run since process start merged incrementally.
	Fallbacks       int  `yaml:"fallbacks,omitempty"`
	EverIncremental bool `yaml:"ever_incremental,omitempty"`
}

// PreviousGeneration exposes the current generation's Parquet parts as local
// files, one at a time, whichever backend stores them. Release ends the pin.
type PreviousGeneration struct {
	Snapshot Snapshot
	Parts    int
	Open     func(index int) (*os.File, func() error, error)
	Release  func() error
}

// PreviousReader is implemented by backends able to hand back the current
// generation's bytes. Backends without it always refresh fully.
type PreviousReader interface {
	OpenPrevious(ctx context.Context, dataset string) (PreviousGeneration, error)
}

func (backend *localBackend) OpenPrevious(ctx context.Context, dataset string) (PreviousGeneration, error) {
	lease, err := backend.Store.acquire(ctx, dataset, "", 0, false)
	if err != nil {
		return PreviousGeneration{}, err
	}
	files := lease.files
	if len(lease.Snapshot.Parts) == 0 {
		files = []*os.File{lease.file}
	}
	return PreviousGeneration{Snapshot: lease.Snapshot, Parts: len(files), Release: lease.Close,
		Open: func(i int) (*os.File, func() error, error) { return files[i], func() error { return nil }, nil }}, nil
}

func (backend *objectBackend) OpenPrevious(ctx context.Context, dataset string) (PreviousGeneration, error) {
	if backend.protected() {
		return PreviousGeneration{}, ErrProtectionRequired
	}
	snapshot, err := backend.Status(ctx, dataset)
	if err != nil {
		return PreviousGeneration{}, err
	}
	parts := snapshot.Parts
	if len(parts) == 0 {
		parts = []SnapshotPart{{ObjectKey: snapshot.ObjectKey, ObjectVersion: snapshot.ObjectVersion, Rows: snapshot.Rows, Bytes: snapshot.Bytes, SHA256: snapshot.SHA256}}
	}
	return PreviousGeneration{Snapshot: snapshot, Parts: len(parts), Release: func() error { return nil },
		Open: func(i int) (*os.File, func() error, error) { return backend.downloadPart(ctx, parts[i]) }}, nil
}

// downloadPart copies one immutable object to a temp file removed on close,
// checking its size and SHA-256 against the committed metadata.
// ponytail: whole-part temp copy; a range-reading ReaderAt would avoid the disk.
func (backend *objectBackend) downloadPart(ctx context.Context, part SnapshotPart) (_ *os.File, _ func() error, err error) {
	body, info, err := backend.reader.Get(ctx, part.ObjectKey, part.ObjectVersion)
	if err != nil {
		return nil, nil, err
	}
	defer body.Close()
	if info.Size != part.Bytes || info.Version != part.ObjectVersion {
		return nil, nil, fmt.Errorf("%w: snapshot object metadata changed", ErrCorrupt)
	}
	out, err := os.CreateTemp("", "kelvo-previous-*")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() error { return errors.Join(out.Close(), os.Remove(out.Name())) }
	defer func() {
		if err != nil {
			_ = cleanup()
		}
	}()
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, hash), io.LimitReader(body, part.Bytes+1))
	if err != nil {
		return nil, nil, err
	}
	if n != part.Bytes || hex.EncodeToString(hash.Sum(nil)) != part.SHA256 {
		return nil, nil, fmt.Errorf("%w: snapshot object checksum mismatch", ErrCorrupt)
	}
	return out, cleanup, nil
}

// copyPreviousRows streams every previous part into sink using the delta's
// schema. The sink applies the usual row, byte and coverage handling. The row
// total must match the committed metadata.
func copyPreviousRows(ctx context.Context, prev PreviousGeneration, schema *arrow.Schema, sink query.Sink) error {
	var total int64
	for i := 0; i < prev.Parts; i++ {
		f, closeFile, err := prev.Open(i)
		if err != nil {
			return err
		}
		n, err := eachPartBatch(ctx, f, schema, sink.Write)
		if err = errors.Join(err, closeFile()); err != nil {
			return err
		}
		total += n
	}
	if total != prev.Snapshot.Rows {
		return fmt.Errorf("%w: previous generation row count differs from its manifest", ErrCorrupt)
	}
	return nil
}

// decode reports a panic inside Parquet decoding as corruption. Only decoder
// calls run under it; sink and merge panics propagate unchanged.
func decode(f func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("%w: malformed previous Parquet part", ErrCorrupt)
		}
	}()
	return f()
}

// eachPartBatch hands every batch of one part, rebuilt on schema, to write.
func eachPartBatch(ctx context.Context, f *os.File, schema *arrow.Schema, write func(arrow.RecordBatch) error) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	var records pqarrow.RecordReader
	var parquet *file.Reader
	err = decode(func() error {
		// A section reader keeps the lease or temp file open for its owner to close.
		p, err := file.NewParquetReader(io.NewSectionReader(f, 0, info.Size()))
		if err != nil {
			return fmt.Errorf("%w: unreadable previous Parquet part", ErrCorrupt)
		}
		parquet = p
		reader, err := pqarrow.NewFileReader(p, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
		if err != nil {
			return fmt.Errorf("%w: unreadable previous Parquet part", ErrCorrupt)
		}
		if records, err = reader.GetRecordReader(ctx, nil, nil); err != nil {
			return fmt.Errorf("%w: unreadable previous Parquet part", ErrCorrupt)
		}
		return nil
	})
	if parquet != nil {
		defer parquet.Close()
	}
	if err != nil {
		return 0, err
	}
	defer records.Release()
	var rows int64
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		var batch arrow.RecordBatch
		if err := decode(func() error {
			if records.Next() {
				batch = records.RecordBatch()
			}
			return records.Err()
		}); err != nil {
			return 0, err
		}
		if batch == nil {
			return rows, nil
		}
		// Rebuild on the delta's schema object so the sink's exact check passes.
		out := array.NewRecordBatch(schema, batch.Columns(), batch.NumRows())
		err := write(out)
		out.Release()
		if err != nil {
			return 0, err
		}
		rows += batch.NumRows()
	}
}

// errIncrementalFallback never reaches callers; Refresh retries with a full run.
var errIncrementalFallback = errors.New("incremental refresh falls back to full")

// mergeSink spills the delta to a temporary Arrow IPC file, then writes the
// previous generation's rows whose key the delta does not carry, then the
// delta. The new generation therefore equals a full refresh provided the key is
// unique and immutable and deletes are soft. Any case it cannot prove (schema
// drift, missing or null key column, duplicate key in the delta, watermark
// moving backwards for a key, delta over its cap) sets fallback and the whole
// transaction is retried fully. Spilling keeps only the key map in memory and
// never holds executor-owned buffers past Write.
type mergeSink struct {
	ctx       context.Context
	inner     query.Sink
	prev      PreviousGeneration
	schema    *arrow.Schema
	key       []string
	watermark string
	maxBytes  int64 // spill cap: min(max_bytes, memory budget minus headroom)
	newSpill  func() (*os.File, error)
	rows      int64

	loaded   bool
	fallback string
	spill    *os.File
	writer   *ipc.FileWriter
	bytes    int64
	appended int64
}

// mergeCap bounds the spilled delta and, through it, the key map.
// ponytail: half of memory_mb leaves headroom for the key map and Parquet writer.
func mergeCap(limits query.Limits) int64 {
	budget := int64(limits.MemoryMB) << 19
	if limits.MaxBytes > 0 && (budget <= 0 || limits.MaxBytes < budget) {
		return limits.MaxBytes
	}
	return budget
}

func (s *mergeSink) fail(why string) error {
	if s.fallback == "" {
		s.fallback = why
	}
	return errIncrementalFallback
}

func fieldIndex(schema *arrow.Schema, name string) int {
	found := -1
	for i, f := range schema.Fields() {
		if strings.EqualFold(f.Name, name) {
			if found >= 0 {
				return -1
			}
			found = i
		}
	}
	return found
}

func (s *mergeSink) Schema(schema *arrow.Schema) error {
	if s.loaded {
		return s.inner.Schema(schema)
	}
	if s.schema == nil || !SchemaEqual(s.schema, schema) {
		return s.fail("delta schema differs from the previous generation")
	}
	for _, name := range append([]string{s.watermark}, s.key...) {
		if fieldIndex(schema, name) < 0 {
			return s.fail("key or watermark column is missing from the delta")
		}
	}
	if s.newSpill == nil {
		return s.fail("backend has no transaction spill area")
	}
	spill, err := s.newSpill()
	if err != nil {
		return s.fail("delta spill cannot be created")
	}
	s.spill = spill
	if s.writer, err = ipc.NewFileWriter(spill, ipc.WithSchema(schema)); err != nil {
		return s.fail("delta spill cannot be written")
	}
	s.loaded = true
	return s.inner.Schema(schema)
}

func (s *mergeSink) Write(record arrow.RecordBatch) error {
	if !s.loaded {
		return s.fail("delta wrote rows before its schema")
	}
	s.bytes += int64(util.TotalRecordSize(record))
	s.rows += record.NumRows()
	if s.maxBytes > 0 && s.bytes > s.maxBytes {
		return s.fail("delta exceeds the incremental merge cap (max_bytes or half of memory_mb)")
	}
	// ponytail: the key map costs well under 64 bytes per row for short keys.
	if s.maxBytes > 0 && s.rows > s.maxBytes/64 {
		return s.fail("delta exceeds the incremental merge row cap (cap/64 rows)")
	}
	// Write or dictionary-change failures only lose this attempt: fall back.
	if err := s.writer.Write(record); err != nil {
		return s.fail("delta spill cannot be written")
	}
	return nil
}

func (s *mergeSink) release() {
	// The transaction owns the spill's directory entry and removes it.
	if s.spill != nil {
		_ = s.spill.Close()
		s.spill = nil
	}
}

// finish runs the merge once the delta query has completed successfully.
func (s *mergeSink) finish() error {
	defer s.release()
	if !s.loaded {
		return s.fail("delta returned no schema")
	}
	if err := s.writer.Close(); err != nil {
		return s.fail("delta spill cannot be written")
	}
	delta, err := ipc.NewFileReader(s.spill)
	if err != nil {
		return s.fail("delta spill cannot be read")
	}
	defer delta.Close()
	keyIdx := make([]int, len(s.key))
	for i, name := range s.key {
		keyIdx[i] = fieldIndex(s.schema, name)
	}
	wmIdx := fieldIndex(s.schema, s.watermark)
	marks := map[string]watermark{}
	for i := 0; i < delta.NumRecords(); i++ {
		r, err := delta.RecordBatch(i)
		if err != nil {
			return err
		}
		for row := 0; row < int(r.NumRows()); row++ {
			k, ok := rowKey(r, keyIdx, row)
			if !ok {
				return s.fail("delta has a null key")
			}
			if _, dup := marks[k]; dup {
				return s.fail("delta repeats a key; the key is not unique")
			}
			w, ok := watermarkOf(r.Column(wmIdx), row)
			if !ok {
				return s.fail("delta watermark is null or of an unsupported type")
			}
			marks[k] = w
		}
	}
	// Previous rows: drop keys the delta replaces, writing contiguous runs.
	var copied int64
	for i := 0; i < s.prev.Parts; i++ {
		f, closeFile, err := s.prev.Open(i)
		if err != nil {
			return err
		}
		n, err := eachPartBatch(s.ctx, f, s.schema, func(r arrow.RecordBatch) error {
			start := 0
			flush := func(end int) error {
				if end > start {
					slice := r.NewSlice(int64(start), int64(end))
					defer slice.Release()
					return s.inner.Write(slice)
				}
				return nil
			}
			for row := 0; row < int(r.NumRows()); row++ {
				k, ok := rowKey(r, keyIdx, row)
				if !ok {
					return s.fail("previous generation has a null key")
				}
				next, replaced := marks[k]
				if !replaced {
					continue
				}
				if old, ok := watermarkOf(r.Column(wmIdx), row); !ok || next.less(old) {
					return s.fail("watermark moved backwards for a key")
				}
				if err := flush(row); err != nil {
					return err
				}
				start = row + 1
			}
			return flush(int(r.NumRows()))
		})
		if err = errors.Join(err, closeFile()); err != nil {
			return err
		}
		copied += n
	}
	if copied != s.prev.Snapshot.Rows {
		return fmt.Errorf("%w: previous generation row count differs from its manifest", ErrCorrupt)
	}
	for i := 0; i < delta.NumRecords(); i++ {
		r, err := delta.RecordBatch(i)
		if err != nil {
			return err
		}
		out := array.NewRecordBatch(s.schema, r.Columns(), r.NumRows())
		err = s.inner.Write(out)
		out.Release()
		if err != nil {
			return err
		}
		s.appended += r.NumRows()
	}
	return nil
}

func rowKey(r arrow.RecordBatch, columns []int, row int) (string, bool) {
	var b strings.Builder
	for _, c := range columns {
		col := r.Column(c)
		if col.IsNull(row) {
			return "", false
		}
		v := col.ValueStr(row)
		b.WriteString(strconv.Itoa(len(v)))
		b.WriteByte(':')
		b.WriteString(v)
	}
	return b.String(), true
}

type watermark struct {
	n int64
	s string
}

func (w watermark) less(o watermark) bool {
	if w.n != o.n {
		return w.n < o.n
	}
	return w.s < o.s
}

// watermarkOf accepts the types coverage records covered_until for.
func watermarkOf(col arrow.Array, row int) (watermark, bool) {
	if col.IsNull(row) {
		return watermark{}, false
	}
	switch a := col.(type) {
	case *array.Int8:
		return watermark{n: int64(a.Value(row))}, true
	case *array.Int16:
		return watermark{n: int64(a.Value(row))}, true
	case *array.Int32:
		return watermark{n: int64(a.Value(row))}, true
	case *array.Int64:
		return watermark{n: a.Value(row)}, true
	case *array.Uint8:
		return watermark{n: int64(a.Value(row))}, true
	case *array.Uint16:
		return watermark{n: int64(a.Value(row))}, true
	case *array.Uint32:
		return watermark{n: int64(a.Value(row))}, true
	case *array.Uint64:
		if v := a.Value(row); v <= math.MaxInt64 {
			return watermark{n: int64(v)}, true
		}
	// Delta and previous share one schema, so raw values compare exactly and
	// never overflow (unlike UnixNano past 2262, e.g. 9999-12-31 sentinels).
	case *array.Timestamp:
		return watermark{n: int64(a.Value(row))}, true
	case *array.Date32:
		return watermark{n: int64(a.Value(row))}, true
	case *array.Date64:
		return watermark{n: int64(a.Value(row))}, true
	}
	return watermark{}, false
}
