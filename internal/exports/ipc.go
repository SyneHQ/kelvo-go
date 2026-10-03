// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exports

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/SYNEHQ/kelvo-go/internal/arrowipc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

func checksum(b []byte) string { value := sha256.Sum256(b); return hex.EncodeToString(value[:]) }

type boundedWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, ErrLimit
	}
	n, err := w.writer.Write(p)
	w.remaining -= int64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

func canonicalSchema(schema *arrow.Schema) (raw []byte, err error) {
	defer func() {
		if recover() != nil {
			raw, err = nil, ErrCorrupt
		}
	}()
	if err = validateSchemaStructure(schema); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	alloc := &boundedAllocator{base: memory.NewGoAllocator(), limit: schemaLimit * 2}
	w := ipc.NewWriter(&boundedWriter{writer: &out, remaining: schemaLimit}, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	if err = w.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// Arrow v18.5.1 ipc/metadata.go builds schemaToFB and metadataToFB in a
// flatbuffers.Builder using ordinary Go allocations. WithAllocator applies only
// to writeFBBuilder's later copy. Bound the input graph BEFORE IPC's mapper or
// builder can recurse or allocate vectors; the final copy/output have separate
// allocator/byte bounds. Count repeated references too, as IPC serializes them
// again. Cycles are rejected by the depth limit without following them forever.
const (
	schemaMaxNodes            = 4096
	schemaMaxDepth            = 32
	schemaMaxMetadataPairs    = 4096
	schemaMaxStringBytes      = 64 << 10
	schemaMaxTotalStringBytes = 512 << 10
)

type schemaBudget struct{ nodes, pairs, strings int }

func validateSchemaStructure(schema *arrow.Schema) error {
	if schema == nil {
		return ErrInvalid
	}
	b := schemaBudget{}
	if schema.NumFields() > schemaMaxNodes {
		return ErrLimit
	}
	if err := b.metadata(schema.Metadata()); err != nil {
		return err
	}
	for i := 0; i < schema.NumFields(); i++ {
		if err := b.field(schema.Field(i), 1); err != nil {
			return err
		}
	}
	return nil
}

func (b *schemaBudget) text(value string) error {
	if len(value) > schemaMaxStringBytes || len(value) > schemaMaxTotalStringBytes-b.strings {
		return ErrLimit
	}
	b.strings += len(value)
	return nil
}

func (b *schemaBudget) metadata(md arrow.Metadata) error {
	if md.Len() > schemaMaxMetadataPairs-b.pairs {
		return ErrLimit
	}
	b.pairs += md.Len()
	// Keys/Values are borrowed slices in pinned Arrow, not copying accessors.
	keys, values := md.Keys(), md.Values()
	if len(keys) != len(values) {
		return ErrInvalid
	}
	for i, key := range keys {
		if err := b.text(key); err != nil {
			return err
		}
		if err := b.text(values[i]); err != nil {
			return err
		}
	}
	return nil
}

func (b *schemaBudget) field(field arrow.Field, depth int) error {
	if err := b.text(field.Name); err != nil {
		return err
	}
	if err := b.metadata(field.Metadata); err != nil {
		return err
	}
	return b.dataType(field.Type, depth)
}

func (b *schemaBudget) dataType(dataType arrow.DataType, depth int) error {
	if dataType == nil {
		return ErrInvalid
	}
	if depth > schemaMaxDepth || b.nodes >= schemaMaxNodes {
		return ErrLimit
	}
	b.nodes++
	switch dt := dataType.(type) {
	case *arrow.DictionaryType:
		if err := b.dataType(dt.IndexType, depth+1); err != nil {
			return err
		}
		return b.dataType(dt.ValueType, depth+1)
	case arrow.ExtensionType:
		if err := b.dataType(dt.StorageType(), depth+1); err != nil {
			return err
		}
		if b.pairs > schemaMaxMetadataPairs-2 {
			return ErrLimit
		}
		b.pairs += 2
		// Extension callbacks are trusted Go code: their internal allocation is
		// outside this budget. They must be bounded and deterministic, as IPC
		// invokes them again. Returned strings still count toward our limits.
		for _, value := range []string{ipc.ExtensionTypeKeyName, ipc.ExtensionMetadataKeyName, dt.ExtensionName(), dt.Serialize()} {
			if err := b.text(value); err != nil {
				return err
			}
		}
		return nil
	case *arrow.TimestampType:
		return b.text(dt.TimeZone)
	case arrow.NestedType:
		// NumFields is checked before Fields, whose struct/union accessors copy.
		count := dt.NumFields()
		if count < 0 || count > schemaMaxNodes-b.nodes {
			return ErrLimit
		}
		if union, ok := dt.(arrow.UnionType); ok && len(union.TypeCodes()) != count {
			return ErrInvalid
		}
		fields := dt.Fields()
		if len(fields) != count {
			return ErrInvalid
		}
		for _, child := range fields {
			if err := b.field(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	case *arrow.NullType, *arrow.BooleanType,
		*arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type,
		*arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Float16Type, *arrow.Float32Type, *arrow.Float64Type,
		arrow.DecimalType, *arrow.FixedSizeBinaryType,
		*arrow.BinaryType, *arrow.LargeBinaryType, *arrow.StringType, *arrow.LargeStringType,
		*arrow.BinaryViewType, *arrow.StringViewType,
		*arrow.Date32Type, *arrow.Date64Type, *arrow.Time32Type, *arrow.Time64Type,
		*arrow.MonthIntervalType, *arrow.DayTimeIntervalType, *arrow.MonthDayNanoIntervalType, *arrow.DurationType:
		return nil
	default:
		return ErrInvalid
	}
}

// validatePart reuses the worker boundary's metadata verifier, then separately
// bounds decoded buffers. No batch escapes this routine. Canonical schemas
// include field/schema metadata, integer widths, nullability and timezones.
func validatePart(ctx context.Context, input io.Reader, encoded int64, limits Limits, schemaHash string) (observed PartInfo, resultErr error) {
	defer func() {
		if recover() != nil {
			resultErr = ErrCorrupt
		}
	}()
	alloc := &boundedAllocator{base: memory.NewGoAllocator(), limit: max(limits.MaxPartDecodedBytes*2, int64(schemaLimit*2))}
	frames := &frames{reader: &remainingReader{reader: input, remaining: encoded, ctx: ctx}, alloc: alloc}
	frames.refs.Store(1)
	reader, err := ipc.NewReaderFromMessageReader(frames, ipc.WithAllocator(alloc))
	if err != nil {
		frames.Release()
		return observed, ErrCorrupt
	}
	defer reader.Release()
	canonical, err := canonicalSchema(reader.Schema())
	if err != nil || checksum(canonical) != schemaHash {
		return observed, ErrCorrupt
	}
	for reader.Next() {
		if err = ctx.Err(); err != nil {
			return observed, err
		}
		batch := reader.RecordBatch()
		if batch == nil || batch.NumRows() < 0 || batch.NumRows() > limits.MaxRows-observed.Rows {
			return observed, ErrLimit
		}
		size := arrowutil.TotalRecordSize(batch)
		if size < 0 || size > limits.MaxPartDecodedBytes-observed.DecodedBytes {
			return observed, ErrLimit
		}
		observed.Rows += batch.NumRows()
		observed.DecodedBytes += size
		observed.Batches++
		if observed.Batches > 1 {
			return observed, ErrCorrupt
		}
	}
	if reader.Err() != nil || !frames.complete || frames.reader.remaining != 0 {
		return observed, ErrCorrupt
	}
	var extra [1]byte
	if n, e := input.Read(extra[:]); n != 0 || !errors.Is(e, io.EOF) {
		return observed, ErrCorrupt
	}
	observed.EncodedBytes = encoded
	return observed, ctx.Err()
}

type remainingReader struct {
	reader    io.Reader
	remaining int64
	ctx       context.Context
}

func (r *remainingReader) Read(p []byte) (int, error) {
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	return n, err
}

type frames struct {
	refs     atomic.Int64
	reader   *remainingReader
	alloc    *boundedAllocator
	current  ipc.MessageReader
	complete bool
}

func (r *frames) Retain() { r.refs.Add(1) }
func (r *frames) Release() {
	if r.refs.Add(-1) == 0 && r.current != nil {
		r.current.Release()
		r.current = nil
	}
}
func (r *frames) Message() (*ipc.Message, error) {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	var prefix [8]byte
	if _, err := io.ReadFull(r.reader, prefix[:]); err != nil {
		return nil, ErrCorrupt
	}
	// Our writer emits continuation framing. Reject legacy framing and require
	// a complete eight-byte EOS, avoiding ambiguous/truncated completion.
	if binary.LittleEndian.Uint32(prefix[:4]) != math.MaxUint32 {
		return nil, ErrCorrupt
	}
	n := binary.LittleEndian.Uint32(prefix[4:])
	if n == 0 {
		r.complete = true
		return nil, io.EOF
	}
	if n > schemaLimit || int64(n) > r.reader.remaining {
		return nil, ErrLimit
	}
	metadata := make([]byte, int(n))
	if _, err := io.ReadFull(r.reader, metadata); err != nil {
		return nil, ErrCorrupt
	}
	body, err := arrowipc.ValidateMessageMetadata(metadata)
	if err != nil || body < 0 || body > r.reader.remaining || body > r.alloc.limit {
		return nil, ErrCorrupt
	}
	r.current = ipc.NewMessageReader(io.MultiReader(bytes.NewReader(prefix[:]), bytes.NewReader(metadata), r.reader), ipc.WithAllocator(r.alloc))
	return r.current.Message()
}

type boundedAllocator struct {
	mu          sync.Mutex
	base        memory.Allocator
	used, limit int64
}

func (a *boundedAllocator) Allocate(n int) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n < 0 || int64(n) > a.limit-a.used {
		panic(ErrLimit)
	}
	b := a.base.Allocate(n)
	a.used += int64(len(b))
	return b
}
func (a *boundedAllocator) Reallocate(n int, b []byte) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n < 0 || int64(n)-int64(len(b)) > a.limit-a.used {
		panic(ErrLimit)
	}
	next := a.base.Reallocate(n, b)
	a.used += int64(len(next) - len(b))
	return next
}
func (a *boundedAllocator) Free(b []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.used -= int64(len(b))
	a.base.Free(b)
}
