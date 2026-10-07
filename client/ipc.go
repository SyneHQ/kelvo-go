// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type wireReader struct {
	ctx       context.Context
	r         io.Reader
	n, max    int64
	exhausted bool
}

func (r *wireReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	// Permit one probe byte to distinguish exact-budget EOF from excess data.
	if int64(len(p)) > r.max-r.n+1 {
		p = p[:max(1, r.max-r.n+1)]
	}
	n, err := r.r.Read(p)
	r.n += int64(n)
	if r.n > r.max {
		r.exhausted = true
		return n, failure("RESOURCE_EXHAUSTED")
	}
	return n, err
}

// The live Arrow-buffer cap is separate from cumulative logical result bytes.
// Codec workspaces and bounded schema metadata have fixed, separate ceilings.
type limitedAllocator struct {
	base      memory.Allocator
	live, max int64
	exhausted bool
}

func (a *limitedAllocator) Allocate(size int) []byte {
	if size < 0 || a.max-a.live < 64 || int64(size) > a.max-a.live-64 {
		a.exhausted = true
		limitedIPC()
	}
	b := a.base.Allocate(size)
	a.live += int64(cap(b)) + 64
	return b
}
func (a *limitedAllocator) Reallocate(size int, b []byte) []byte {
	if size >= 0 && size <= cap(b) {
		return b[:size]
	}
	next := a.Allocate(size) // Count both buffers during the copy.
	copy(next, b)
	a.Free(b)
	return next
}
func (a *limitedAllocator) Free(b []byte) {
	if b == nil {
		return
	}
	a.live -= int64(cap(b)) + 64
	a.base.Free(b)
}

type boundedMessages struct {
	refs         atomic.Int64
	wire         *wireReader
	alloc        *limitedAllocator
	cfg          Config
	current      *ipc.Message
	eos          bool
	problem      error
	layout       *ipcLayout
	dictionaries map[int64]ipcLayout
}

func (r *boundedMessages) Retain() { r.refs.Add(1) }
func (r *boundedMessages) Release() {
	if r.refs.Add(-1) == 0 && r.current != nil {
		r.current.Release()
		r.current = nil
	}
}
func (r *boundedMessages) Message() (message *ipc.Message, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = failure("PROTOCOL_ERROR")
			if e, ok := recovered.(*Error); ok {
				err = failure(e.Code)
			}
		}
		if err != nil && err != io.EOF {
			r.problem = err
		}
	}()
	if r.eos {
		return nil, io.EOF
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	var framing [8]byte
	if _, err = io.ReadFull(r.wire, framing[:]); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(framing[:4]) != 0xffffffff {
		return nil, failure("PROTOCOL_ERROR")
	}
	length := int64(binary.LittleEndian.Uint32(framing[4:]))
	if length == 0 {
		r.eos = true
		return nil, io.EOF
	}
	if length > maxIPCMetadata || length > r.cfg.MaxWireBytes-r.wire.n {
		return nil, failure("RESOURCE_EXHAUSTED")
	}
	if length%8 != 0 {
		return nil, failure("PROTOCOL_ERROR")
	}
	metadata := make([]byte, int(length))
	if _, err = io.ReadFull(r.wire, metadata); err != nil {
		return nil, err
	}
	bodyLength, batch := checkIPCMetadata(metadata, r.cfg)
	if batch.schema != nil {
		if r.layout != nil {
			badIPC()
		}
		r.layout, r.dictionaries = batch.schema, batch.dictionaries
	} else {
		if r.layout == nil {
			badIPC()
		}
		layout := *r.layout
		if batch.dictionary {
			var found bool
			layout, found = r.dictionaries[batch.id]
			if !found {
				badIPC()
			}
		}
		// Generated Arrow vector getters do not bounds-check their index.
		// Match the full schema layout before Arrow can access those vectors.
		if len(batch.nodes) != layout.nodes || len(batch.variadic) != layout.variadic {
			badIPC()
		}
		buffers := int64(layout.buffers)
		for _, count := range batch.variadic {
			buffers += count
		}
		if buffers != int64(len(batch.buffers)) {
			badIPC()
		}
	}
	if bodyLength > r.cfg.MaxWireBytes-r.wire.n {
		return nil, failure("RESOURCE_EXHAUSTED")
	}
	body := memory.NewResizableBuffer(r.alloc)
	defer func() { body.Release() }()
	body.Resize(int(bodyLength))
	if _, err = io.ReadFull(r.wire, body.Bytes()); err != nil {
		return nil, err
	}
	if batch != nil && batch.compressed {
		decoded, decodeErr := decompressBatch(r.wire.ctx, batch, body.Bytes(), r.alloc, r.cfg.MaxDecodedBytes)
		if decodeErr != nil {
			return nil, decodeErr
		}
		body.Release()
		body = decoded
		metadata = uncompressedMetadata(batch, int64(body.Len()))
	}
	meta := memory.NewBufferBytes(metadata)
	r.current = ipc.NewMessage(meta, body)
	return r.current, nil
}

func decodeStream(ctx context.Context, body io.Reader, sink Sink, cfg Config) (stats Stats, err error) {
	wire := &wireReader{ctx: ctx, r: body, max: cfg.MaxWireBytes}
	alloc := &limitedAllocator{base: memory.NewGoAllocator(), max: cfg.MaxDecodedBytes}
	frames := &boundedMessages{wire: wire, alloc: alloc, cfg: cfg}
	frames.refs.Store(1)
	defer func() {
		stats.WireBytes = wire.n
		if recovered := recover(); recovered != nil {
			err = failure("PROTOCOL_ERROR")
			if e, ok := recovered.(*Error); ok {
				err = failure(e.Code)
			}
		}
		// ReadFull may suppress a Read error when n fills the destination.
		// Budget exhaustion must never become success at the final EOS frame.
		if alloc.exhausted || wire.exhausted {
			err = failure("RESOURCE_EXHAUSTED")
		}
		if err != nil {
			switch {
			case ctx.Err() != nil:
				err = contextFailure(ctx)
			case alloc.exhausted || wire.exhausted:
				err = failure("RESOURCE_EXHAUSTED")
			default:
				var safe *Error
				if errors.As(err, &safe) {
					err = failure(safe.Code)
				} else {
					err = failure("PROTOCOL_ERROR")
				}
			}
		}
	}()
	reader, err := ipc.NewReaderFromMessageReader(frames, ipc.WithAllocator(alloc), ipc.WithDelayReadSchema(true))
	if err != nil {
		frames.Release()
		return stats, err
	}
	defer reader.Release()
	schema := reader.Schema()
	if schema == nil || reader.Err() != nil {
		if frames.problem != nil {
			return stats, frames.problem
		}
		return stats, failure("PROTOCOL_ERROR")
	}
	if err = callSink(func() error { return sink.Schema(schema) }); err != nil {
		return stats, err
	}
	for reader.Next() {
		if ctx.Err() != nil {
			return stats, contextFailure(ctx)
		}
		record := reader.RecordBatch()
		if record == nil || record.NumRows() < 0 {
			return stats, failure("PROTOCOL_ERROR")
		}
		if record.NumRows() > cfg.MaxRows-stats.Rows {
			return stats, failure("RESOURCE_EXHAUSTED")
		}
		remaining := cfg.MaxDecodedBytes - stats.DecodedBytes
		for _, column := range record.Columns() {
			remaining = chargeArray(column.Data(), remaining, 0)
		}
		stats.DecodedBytes = cfg.MaxDecodedBytes - remaining
		if err = callSink(func() error { return sink.Write(record) }); err != nil {
			return stats, err
		}
		stats.Rows += record.NumRows()
		stats.Batches++
	}
	if frames.problem != nil {
		return stats, frames.problem
	}
	if reader.Err() != nil || !frames.eos {
		return stats, failure("PROTOCOL_ERROR")
	}
	var extra [1]byte
	n, eof := wire.Read(extra[:])
	if n != 0 || eof != io.EOF {
		return stats, failure("PROTOCOL_ERROR")
	}
	if ctx.Err() != nil {
		return stats, contextFailure(ctx)
	}
	return stats, nil
}

func callSink(call func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = failure("SINK_FAILED")
		}
	}()
	if callErr := call(); callErr != nil {
		var e *Error
		if errors.As(callErr, &e) && e.Code == "RESOURCE_EXHAUSTED" {
			return failure(e.Code)
		}
		return failure("SINK_FAILED")
	}
	return nil
}

func chargeArray(data arrow.ArrayData, remaining int64, depth int) int64 {
	if data == nil {
		return remaining
	}
	if depth > maxIPCDepth+1 {
		limitedIPC()
	}
	for _, buffer := range data.Buffers() {
		if buffer == nil {
			continue
		}
		n := int64(buffer.Len())
		if n > remaining {
			limitedIPC()
		}
		remaining -= n
	}
	for _, child := range data.Children() {
		remaining = chargeArray(child, remaining, depth+1)
	}
	// Arrow's Dictionary accessor returns a typed nil *array.Data for ordinary
	// arrays. Inspect the data type before following that optional pointer.
	if data.DataType().ID() == arrow.DICTIONARY {
		remaining = chargeArray(data.Dictionary(), remaining, depth+1)
	}
	return remaining
}

func decompressBatch(ctx context.Context, batch *ipcBatch, raw []byte, alloc *limitedAllocator, limit int64) (result *memory.Buffer, err error) {
	// Bound every advertised expansion before allocating any decompressed body.
	lengths := make([]int64, len(batch.buffers))
	var total int64
	for i, span := range batch.buffers {
		if span[1] == 0 {
			continue
		}
		if span[1] < 8 {
			return nil, failure("PROTOCOL_ERROR")
		}
		length := int64(binary.LittleEndian.Uint64(raw[span[0] : span[0]+8]))
		if length == -1 {
			length = span[1] - 8
		}
		if length < 0 {
			return nil, failure("PROTOCOL_ERROR")
		}
		if length > limit-total || length > limit-7 {
			return nil, failure("RESOURCE_EXHAUSTED")
		}
		lengths[i] = length
		total += (length + 7) &^ 7
		if total > limit {
			return nil, failure("RESOURCE_EXHAUSTED")
		}
	}
	result = memory.NewResizableBuffer(alloc)
	owned := result
	ok := false
	defer func() {
		if !ok {
			owned.Release()
		}
	}()
	result.Resize(int(total))
	var offset int64
	for i, span := range batch.buffers {
		if ctx.Err() != nil {
			return nil, contextFailure(ctx)
		}
		length := lengths[i]
		if span[1] != 0 {
			encoded := raw[span[0] : span[0]+span[1]]
			out := result.Bytes()[offset : offset+length]
			if int64(binary.LittleEndian.Uint64(encoded[:8])) == -1 {
				copy(out, encoded[8:])
			} else if err := decompressBuffer(batch.codec, encoded[8:], out, limit); err != nil {
				return nil, err
			}
		}
		batch.buffers[i] = [2]int64{offset, length}
		offset += (length + 7) &^ 7
	}
	ok = true
	return result, nil
}

func decompressBuffer(codec byte, encoded, output []byte, limit int64) error {
	var reader io.Reader
	switch codec {
	case 0:
		// Kelvo/Arrow writes 64 KiB LZ4 frames. Reject larger workspaces and
		// legacy/skippable/concatenated frames before the codec allocates.
		if !validLZ4Frame(encoded, len(output)) {
			return failure("PROTOCOL_ERROR")
		}
		reader = lz4.NewReader(bytes.NewReader(encoded))
	case 1:
		window := uint64(max(1024, min(limit, 8<<20)))
		decoder, err := zstd.NewReader(bytes.NewReader(encoded), zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(window), zstd.WithDecoderMaxWindow(window), zstd.WithDecodeBuffersBelow(0))
		if err != nil {
			return failure("PROTOCOL_ERROR")
		}
		defer decoder.Close()
		reader = decoder
	default:
		return failure("PROTOCOL_ERROR")
	}
	if _, err := io.ReadFull(reader, output); err != nil {
		return failure("PROTOCOL_ERROR")
	}
	var extra [1]byte
	if n, err := reader.Read(extra[:]); n != 0 || err != io.EOF {
		return failure("PROTOCOL_ERROR")
	}
	return nil
}

func validLZ4Frame(encoded []byte, decodedLength int) bool {
	if len(encoded) < 7 || binary.LittleEndian.Uint32(encoded) != 0x184d2204 {
		return false
	}
	flags := encoded[4]
	if flags&0xc0 != 0x40 || flags&2 != 0 || encoded[5] != 0x40 {
		return false
	}
	position := 6
	if flags&8 != 0 {
		if len(encoded)-position < 8 || binary.LittleEndian.Uint64(encoded[position:]) != uint64(decodedLength) {
			return false
		}
		position += 8
	}
	if flags&1 != 0 {
		position += 4 // Optional dictionary ID; the codec checks its support.
	}
	position++ // Header checksum, checked by the codec.
	for {
		if position > len(encoded) || len(encoded)-position < 4 {
			return false
		}
		block := binary.LittleEndian.Uint32(encoded[position:])
		position += 4
		if block == 0 {
			if flags&4 != 0 {
				position += 4 // Content checksum, checked when reading through EOF.
			}
			return position == len(encoded)
		}
		size := int(block & 0x7fffffff)
		if size == 0 || size > 64<<10 || size > len(encoded)-position {
			return false
		}
		position += size
		if flags&16 != 0 {
			position += 4
		}
	}
}
