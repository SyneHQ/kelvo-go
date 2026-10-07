// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

const maxIPCMetadata = 1 << 20

var (
	errInvalidIPC = errors.New("invalid worker Arrow metadata")
	errIPCLimit   = errors.New("worker Arrow resource limit exceeded")
)

// readWorkerIPC treats the child process as an untrusted byte producer. Neither
// the worker's own limits nor a limiting pipe reader prevents allocations from
// forged Arrow lengths. Bound framing, allocation-critical metadata and decoded
// buffers separately, and require explicit EOS followed by pipe EOF.
func readWorkerIPC(ctx context.Context, input io.Reader, limits query.Limits, sink query.Sink) (observed query.Stats, resultErr error) {
	return readWorkerIPCObserved(ctx, input, limits, sink, nil)
}

// readWorkerIPCObserved adds call-owned diagnostics without changing the trust
// boundary or completion contract. A nil observation takes no extra clocks and
// does not wrap the sink. It never retains a borrowed batch.
func readWorkerIPCObserved(ctx context.Context, input io.Reader, limits query.Limits, sink query.Sink, observation *telemetry.IPCTransfer) (observed query.Stats, resultErr error) {
	stream := &ipcInput{reader: input, remaining: limits.MaxBytes}
	var started time.Time
	var trailingBytes int
	if observation != nil {
		*observation = telemetry.IPCTransfer{Observed: true}
		started = time.Now()
		sink = &ipcTransferSink{sink: sink, observation: observation}
	}
	defer func() {
		if recover() != nil {
			resultErr = query.NewError("QUERY_FAILED", "Query worker returned invalid Arrow data")
		}
		if observation != nil {
			observation.Duration = time.Since(started)
			// Include the one-byte post-EOS probe if it found trailing data.
			// These are pipe bytes consumed, not network or delivered bytes.
			observation.InputBytes = limits.MaxBytes - stream.remaining + int64(trailingBytes)
			observation.Complete = resultErr == nil
		}
	}()
	alloc := &ipcAllocator{base: memory.NewGoAllocator(), limit: int64(limits.MemoryMB) << 20}
	framing := &ipcFrames{stream: stream, alloc: alloc}
	framing.refs.Store(1)
	reader, err := ipc.NewReaderFromMessageReader(framing, ipc.WithAllocator(alloc))
	if err != nil {
		framing.Release()
		return observed, ipcReadError(ctx, stream, alloc, err)
	}
	defer reader.Release()
	if err := sink.Schema(reader.Schema()); err != nil {
		return observed, err
	}
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return observed, err
		}
		batch := reader.RecordBatch()
		if batch == nil || batch.NumRows() < 0 {
			return observed, ipcReadError(ctx, stream, alloc, errInvalidIPC)
		}
		if batch.NumRows() > limits.MaxRows-observed.Rows {
			return observed, query.NewError("RESOURCE_EXHAUSTED", "Worker result exceeds row limit")
		}
		size := arrowutil.TotalRecordSize(batch)
		if size < 0 || size > limits.MaxBytes-observed.Bytes {
			return observed, query.NewError("RESOURCE_EXHAUSTED", "Worker result exceeds byte limit")
		}
		if observation != nil {
			// Count validated buffers offered to the synchronous sink, including
			// its failed final call. These are not successful delivery counters.
			observation.DecodedBytes += size
			observation.Batches++
		}
		if err := sink.Write(batch); err != nil {
			return observed, err
		}
		observed.Rows += batch.NumRows()
		observed.Bytes += size
		observed.Batches++
	}
	if reader.Err() != nil || !framing.complete {
		return observed, ipcReadError(ctx, stream, alloc, reader.Err())
	}
	// At most one extra byte is read to distinguish exact-limit EOF from an
	// overflow or data appended after EOS. No appended bytes reach the sink.
	var trailing [1]byte
	n, err := io.ReadFull(input, trailing[:])
	trailingBytes = n
	if n != 0 || !errors.Is(err, io.EOF) {
		if stream.remaining == 0 && n != 0 {
			stream.exceeded = true
		}
		return observed, ipcReadError(ctx, stream, alloc, errInvalidIPC)
	}
	if err := ctx.Err(); err != nil {
		return observed, err
	}
	observed.WireBytes = limits.MaxBytes - stream.remaining
	return observed, nil
}

type ipcTransferSink struct {
	sink        query.Sink
	observation *telemetry.IPCTransfer
}

func (s *ipcTransferSink) Schema(schema *arrow.Schema) error {
	started := time.Now()
	s.observation.HasSink = true
	defer func() { s.observation.SinkDuration += time.Since(started) }()
	return s.sink.Schema(schema)
}

func (s *ipcTransferSink) Write(batch arrow.RecordBatch) error {
	started := time.Now()
	s.observation.HasSink = true
	defer func() { s.observation.SinkDuration += time.Since(started) }()
	return s.sink.Write(batch)
}

func ipcReadError(ctx context.Context, stream *ipcInput, alloc *ipcAllocator, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if stream.exceeded || alloc.exceeded.Load() || errors.Is(err, errIPCLimit) {
		return query.NewError("RESOURCE_EXHAUSTED", "Worker Arrow stream exceeds memory or byte limit")
	}
	return query.NewError("QUERY_FAILED", "Query worker did not complete a valid Arrow result")
}

type ipcInput struct {
	reader    io.Reader
	remaining int64
	exceeded  bool
}

func (r *ipcInput) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining == 0 {
		r.exceeded = true
		return 0, errIPCLimit
	}
	if int64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	return n, err
}

type ipcFrames struct {
	refs     atomic.Int64
	stream   *ipcInput
	alloc    *ipcAllocator
	current  ipc.MessageReader
	complete bool
}

func (r *ipcFrames) Retain() { r.refs.Add(1) }
func (r *ipcFrames) Release() {
	if r.refs.Add(-1) == 0 && r.current != nil {
		r.current.Release()
		r.current = nil
	}
}

func (r *ipcFrames) Message() (*ipc.Message, error) {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	var prefix [8]byte
	if _, err := io.ReadFull(r.stream, prefix[:4]); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	length, prefixSize := binary.LittleEndian.Uint32(prefix[:4]), 4
	if length == math.MaxUint32 {
		if _, err := io.ReadFull(r.stream, prefix[4:]); err != nil {
			return nil, io.ErrUnexpectedEOF
		}
		length, prefixSize = binary.LittleEndian.Uint32(prefix[4:]), 8
	}
	if length == 0 {
		r.complete = true
		return nil, io.EOF
	}
	if length > maxIPCMetadata || int64(length) > r.alloc.limit || int64(length) > r.stream.remaining {
		r.stream.exceeded = true
		return nil, errIPCLimit
	}
	meta := make([]byte, int(length))
	if _, err := io.ReadFull(r.stream, meta); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	bodyLength, err := validateIPCMetadata(meta)
	if err != nil {
		if errors.Is(err, errIPCLimit) {
			r.stream.exceeded = true
		}
		return nil, err
	}
	if bodyLength > r.alloc.limit || bodyLength > r.stream.remaining {
		r.stream.exceeded = true
		return nil, errIPCLimit
	}
	r.current = ipc.NewMessageReader(io.MultiReader(bytes.NewReader(prefix[:prefixSize]), bytes.NewReader(meta), r.stream), ipc.WithAllocator(r.alloc))
	return r.current.Message()
}

// Arrow's allocator covers body buffers and decompression. Schema objects have
// separate bounded metadata limits; this is not a hard whole-process RSS cap.
type ipcAllocator struct {
	mu       sync.Mutex
	base     memory.Allocator
	used     int64
	limit    int64
	exceeded atomic.Bool
}

func (a *ipcAllocator) Allocate(size int) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if size < 0 || int64(size) > a.limit-a.used {
		a.exceeded.Store(true)
		panic(errIPCLimit)
	}
	buf := a.base.Allocate(size)
	a.used += int64(len(buf))
	return buf
}

func (a *ipcAllocator) Reallocate(size int, buf []byte) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if size < 0 || int64(size)-int64(len(buf)) > a.limit-a.used {
		a.exceeded.Store(true)
		panic(errIPCLimit)
	}
	next := a.base.Reallocate(size, buf)
	a.used += int64(len(next)) - int64(len(buf))
	return next
}

func (a *ipcAllocator) Free(buf []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.used -= int64(len(buf))
	a.base.Free(buf)
}
