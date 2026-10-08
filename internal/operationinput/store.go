// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package operationinput seals private operation payloads in the export store.
// It provides no endpoint or authorization grant. Identity must be obtained from
// current trusted authorization on every call.
package operationinput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/operations"
)

const MaxInputBytes int64 = 16 << 20
const ChunkBytes = 64 << 10

type Format string

const (
	OperationRequest Format = "operation_request_v1"
	ArrowIPC         Format = "arrow_ipc"
	SchemaPlan       Format = "schema_plan_v1"
	MigrationPlan    Format = "migration_plan_v1"
	IngestionBatch   Format = "ingestion_batch_v1"
	NativeInput      Format = "native_input_v1"
	WatchCheckpoint  Format = "watch_checkpoint_v1"
	MongoWatchResume Format = "mongo_watch_resume_v1"
)

var (
	ErrInvalid = errors.New("invalid operation input")
	ErrCorrupt = errors.New("operation input verification failed")
	ErrLimit   = errors.New("operation input limit exceeded")
	ErrCleanup = errors.New("operation input cleanup did not complete")
)

// Storage must be a dedicated private input root. Its entry, byte and TTL
// budgets are enforced persistently by exports, including across reopen.
type Config struct {
	Storage       exports.Config
	MaxInputBytes int64
}

type Store struct {
	storage *exports.Store
	maximum int64
}

func Open(config Config) (*Store, error) {
	if config.MaxInputBytes < 1 || config.MaxInputBytes > MaxInputBytes {
		return nil, ErrInvalid
	}
	storage, err := exports.Open(config.Storage)
	if err != nil {
		return nil, err
	}
	return &Store{storage: storage, maximum: config.MaxInputBytes}, nil
}

func validFormat(format Format) bool {
	switch format {
	case OperationRequest, ArrowIPC, SchemaPlan, MigrationPlan, IngestionBatch, NativeInput, WatchCheckpoint, MongoWatchResume:
		return true
	}
	return false
}

func inputSchema(format Format) *arrow.Schema {
	metadata := arrow.NewMetadata([]string{"kelvo_operation_input", "format"}, []string{"1", string(format)})
	return arrow.NewSchema([]arrow.Field{{Name: "chunk", Type: arrow.BinaryTypes.Binary, Nullable: false}}, &metadata)
}

func inputLimits(maximum int64) exports.Limits {
	parts := (maximum + ChunkBytes - 1) / ChunkBytes
	return exports.Limits{
		MaxRows: parts, MaxParts: int(parts), Compression: "none",
		MaxEncodedBytes: parts * (ChunkBytes + 8<<10), MaxPartBytes: ChunkBytes + 8<<10,
		MaxDecodedBytes: parts * (ChunkBytes + 4<<10), MaxPartDecodedBytes: ChunkBytes + 4<<10,
	}
}

// Put reserves the full input budget before consuming bytes. The reader is
// borrowed synchronously; callers must arrange cancellation of blocking reads.
// Errors retain the backend's reservation until its bounded Cleanup. A nonempty
// reference with ErrPublicationUncertain must be preserved, never re-published.
func (s *Store) Put(ctx context.Context, identity exports.Identity, expiry time.Time, format Format, input io.Reader) (ref operations.InputRef, resultErr error) {
	if s == nil || s.storage == nil || ctx == nil || input == nil || !validFormat(format) {
		return ref, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return ref, err
	}
	maximum := s.maximum
	if format == OperationRequest {
		maximum = min(maximum, int64(operations.MaxRequestBytes))
	}
	schema := inputSchema(format)
	w, err := s.storage.Begin(ctx, exports.Request{Identity: identity, ExpiresAt: expiry, Limits: inputLimits(maximum)}, schema)
	if err != nil {
		return ref, err
	}
	return s.write(ctx, identity, expiry, format, maximum, w, input)
}

func (s *Store) write(ctx context.Context, identity exports.Identity, expiry time.Time, format Format, maximum int64, w *exports.Writer, input io.Reader) (ref operations.InputRef, resultErr error) {
	defer func() {
		if err := w.Close(); err != nil {
			resultErr = errors.Join(resultErr, ErrCleanup, err)
		}
	}()
	schema := inputSchema(format)
	reader := &inputReader{ctx: ctx, expiry: expiry, reader: input}
	buffer := make([]byte, ChunkBytes)
	hash := sha256.New()
	var total int64
	var canonical []byte
	for total < maximum {
		n, readErr := io.ReadFull(reader, buffer[:min(int64(ChunkBytes), maximum-total)])
		if n > 0 {
			chunk := buffer[:n]
			if format == OperationRequest {
				canonical = append(canonical, chunk...)
			}
			builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
			builder.Field(0).(*array.BinaryBuilder).Append(chunk)
			batch := builder.NewRecordBatch()
			builder.Release()
			err := w.Write(ctx, batch)
			batch.Release()
			if err != nil {
				return ref, err
			}
			_, _ = hash.Write(chunk)
			total += int64(n)
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return ref, readErr
		}
	}
	if total == maximum {
		var excess [1]byte
		n, err := io.ReadFull(reader, excess[:])
		if n != 0 {
			return ref, ErrLimit
		}
		if !errors.Is(err, io.EOF) {
			return ref, err
		}
	}
	if total == 0 {
		return ref, ErrInvalid
	}
	if format == OperationRequest && !canonicalRequest(canonical) {
		return ref, ErrInvalid
	}
	manifest, err := w.Commit(ctx, identity)
	if manifest.ID == "" {
		return ref, err
	}
	ref = operations.InputRef{ID: manifest.ID, SHA256: hex.EncodeToString(hash.Sum(nil)), Bytes: total, Format: string(format)}
	return ref, err
}

func canonicalRequest(raw []byte) bool {
	request, err := operations.ParseRequest(raw)
	if err != nil {
		return false
	}
	canonical, err := operations.Encode(request)
	return err == nil && bytes.Equal(raw, canonical)
}

func (s *Store) PutRequest(ctx context.Context, identity exports.Identity, expiry time.Time, request operations.Request) (operations.InputRef, error) {
	raw, err := operations.Encode(request)
	if err != nil {
		return operations.InputRef{}, err
	}
	return s.Put(ctx, identity, expiry, OperationRequest, bytes.NewReader(raw))
}

func (s *Store) Cancel(ctx context.Context, identity exports.Identity, ref operations.InputRef) error {
	if err := s.validateReference(ctx, ref); err != nil {
		return err
	}
	// Verify the complete reference before allowing its ID to select a cancel.
	if _, err := s.Load(ctx, identity, ref); err != nil {
		return err
	}
	return s.storage.Cancel(ctx, ref.ID, identity)
}

func (s *Store) Cleanup(ctx context.Context, maximum int) (exports.CleanupResult, error) {
	if s == nil || s.storage == nil || ctx == nil {
		return exports.CleanupResult{}, ErrInvalid
	}
	return s.storage.Cleanup(ctx, maximum)
}

func (s *Store) Close() error {
	if s == nil || s.storage == nil {
		return nil
	}
	return s.storage.Close()
}

func (s *Store) validateReference(ctx context.Context, ref operations.InputRef) error {
	if s == nil || s.storage == nil || ctx == nil || ref.Validate() != nil || len(ref.ID) != 32 || !validFormat(Format(ref.Format)) || ref.Bytes > s.maximum || (ref.Format == string(OperationRequest) && ref.Bytes > operations.MaxRequestBytes) {
		return ErrInvalid
	}
	for _, ch := range ref.ID {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return ErrInvalid
		}
	}
	return ctx.Err()
}

type inputReader struct {
	ctx    context.Context
	expiry time.Time
	reader io.Reader
	empty  int
}

func (r *inputReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if !time.Now().Before(r.expiry) {
		return 0, exports.ErrUnavailable
	}
	n, err := r.reader.Read(p)
	if n == 0 && err == nil {
		r.empty++
		if r.empty >= 100 {
			return 0, io.ErrNoProgress
		}
	} else {
		r.empty = 0
	}
	return n, err
}
