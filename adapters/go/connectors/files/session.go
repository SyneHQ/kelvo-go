// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package files

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	duckengine "github.com/SYNEHQ/kelvo-go/internal/engine/duckdb"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

type Session struct {
	mu         sync.Mutex
	file       *os.File
	path       string
	name       string
	descriptor filesnapshot.Descriptor
	limits     query.Limits
}

// Open takes ownership of a read-only inherited descriptor only after verifying
// every byte. No filename or URL from a caller is used to open a source.
func Open(ctx context.Context, file *os.File, path, name string, d filesnapshot.Descriptor, limits query.Limits) (*Session, error) {
	if ctx == nil || ctx.Err() != nil || file == nil || d.Validate() != nil || limits.Validate() != nil || !catalog.ValidID(name) {
		return nil, adapter.ErrInvalid
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != d.Bytes {
		return nil, adapter.ErrInvalid
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, adapter.ErrInvalid
	}
	hash := sha256.New()
	buffer := make([]byte, 64<<10)
	var total int64
	for total < d.Bytes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := file.Read(buffer[:min(int64(len(buffer)), d.Bytes-total)])
		if n > 0 {
			hash.Write(buffer[:n])
			total += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, adapter.ErrInvalid
		}
		if n == 0 {
			return nil, io.ErrNoProgress
		}
	}
	if total != d.Bytes || hex.EncodeToString(hash.Sum(nil)) != d.SHA256 {
		return nil, adapter.ErrInvalid
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, adapter.ErrInvalid
	}
	return &Session{file: file, path: path, name: name, descriptor: d, limits: limits}, nil
}

func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *Session) Query(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	if s == nil || ctx == nil || sink == nil || q.Validate() != nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.read(ctx, q, sink)
}

func (s *Session) read(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	if s.file == nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	q.MaxRows = min(q.MaxRows, s.limits.MaxRows)
	q.MaxBytes = min(q.MaxBytes, s.limits.MaxBytes)
	if s.descriptor.Format == "sqlite" {
		return s.readSQLite(ctx, q, sink)
	}
	limits := s.limits
	limits.MaxRows = min(limits.MaxRows, q.MaxRows)
	limits.MaxBytes = min(limits.MaxBytes, q.MaxBytes)
	engine, err := duckengine.NewFileSnapshot(catalog.Source{ID: s.name, Type: s.descriptor.Format, Path: s.path}, limits)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	parameters := make([]query.Parameter, len(q.Parameters))
	for i, p := range q.Parameters {
		parameters[i] = query.Parameter{Type: p.Type, Value: p.Value}
	}
	stats, err := engine.Execute(ctx, query.Request{Mode: "federated", Sources: []string{s.name}, SQL: q.Statement, Parameters: parameters}, batchSink{Sink: sink, rows: int64(q.BatchRows)})
	return adapter.QueryStats{Rows: stats.Rows, Bytes: stats.Bytes, Elapsed: time.Duration(stats.DurationNS)}, err
}

func (s *Session) Test(ctx context.Context) error {
	_, err := s.Query(ctx, adapter.Query{Statement: "SELECT 1", MaxRows: 1, MaxBytes: s.limits.MaxBytes, BatchRows: 1}, discard{})
	return err
}

func (s *Session) Inspect(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	if s == nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	if s.descriptor.Format == "sqlite" {
		return s.inspectSQLite(ctx, spec, limits, sink)
	}
	statement, parameters, err := metadataQuery(spec, s.name, s.descriptor.Format)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	return s.Query(ctx, adapter.Query{Statement: statement, Parameters: parameters, MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}, sink)
}

type batchSink struct {
	adapter.Sink
	rows int64
}

func (s batchSink) Write(record arrow.RecordBatch) error {
	if record == nil || s.rows < 1 {
		return adapter.ErrInvalid
	}
	for start := int64(0); start < record.NumRows(); start += s.rows {
		batch := record.NewSlice(start, min(start+s.rows, record.NumRows()))
		err := s.Sink.Write(batch)
		batch.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

type discard struct{}

func (discard) Schema(*arrow.Schema) error    { return nil }
func (discard) Write(arrow.RecordBatch) error { return nil }
