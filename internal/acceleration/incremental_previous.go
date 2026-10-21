// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// RefreshInfo describes the last refresh this process ran for a dataset. It is
// memory only: after a restart the kind is unknown.
type RefreshInfo struct {
	Incremental  bool   `yaml:"incremental"`
	AppendedRows int64  `yaml:"appended_rows,omitempty"`
	Fallback     string `yaml:"fallback,omitempty"`
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
func copyPreviousRows(ctx context.Context, prev PreviousGeneration, schema *arrow.Schema, sink query.Sink) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("%w: malformed previous Parquet part", ErrCorrupt)
		}
	}()
	var total int64
	for i := 0; i < prev.Parts; i++ {
		f, closeFile, err := prev.Open(i)
		if err != nil {
			return err
		}
		n, err := copyPart(ctx, f, schema, sink)
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

func copyPart(ctx context.Context, f *os.File, schema *arrow.Schema, sink query.Sink) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	// A section reader keeps the lease or temp file open for its owner to close.
	parquet, err := file.NewParquetReader(io.NewSectionReader(f, 0, info.Size()))
	if err != nil {
		return 0, fmt.Errorf("%w: unreadable previous Parquet part", ErrCorrupt)
	}
	defer parquet.Close()
	reader, err := pqarrow.NewFileReader(parquet, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		return 0, fmt.Errorf("%w: unreadable previous Parquet part", ErrCorrupt)
	}
	records, err := reader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("%w: unreadable previous Parquet part", ErrCorrupt)
	}
	defer records.Release()
	var rows int64
	for records.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		batch := records.RecordBatch()
		// Rebuild on the delta's schema object so the sink's exact check passes.
		out := array.NewRecordBatch(schema, batch.Columns(), batch.NumRows())
		err := sink.Write(out)
		out.Release()
		if err != nil {
			return 0, err
		}
		rows += batch.NumRows()
	}
	return rows, records.Err()
}

// errIncrementalFallback never reaches callers; Refresh retries with a full run.
var errIncrementalFallback = errors.New("incremental refresh falls back to full")

// preloadSink forwards the delta to inner after first writing the previous
// generation's rows, so the new generation holds both. It requires the delta
// schema to equal the previous one and otherwise asks for a full refresh.
type preloadSink struct {
	ctx      context.Context
	inner    query.Sink
	prev     PreviousGeneration
	schema   *arrow.Schema
	loaded   bool
	fallback bool
	appended int64
}

func (s *preloadSink) Schema(schema *arrow.Schema) error {
	if s.loaded {
		return s.inner.Schema(schema)
	}
	if s.schema == nil || !SchemaEqual(s.schema, schema) {
		s.fallback = true
		return errIncrementalFallback
	}
	if err := s.inner.Schema(schema); err != nil {
		return err
	}
	s.loaded = true
	return copyPreviousRows(s.ctx, s.prev, schema, s.inner)
}

func (s *preloadSink) Write(record arrow.RecordBatch) error {
	if !s.loaded {
		s.fallback = true
		return errIncrementalFallback
	}
	s.appended += record.NumRows()
	return s.inner.Write(record)
}
