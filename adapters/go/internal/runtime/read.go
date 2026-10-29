package runtime

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sync"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// executeRead runs a query or metadata inspection and writes its Arrow result.
// It is shared by every build: reads are all a slim read worker serves.
func executeRead(ctx context.Context, input adapter.ProcessRequest, invocation adapter.Invocation, session adapter.Session, output io.Writer, receipt operations.Receipt) (operations.Receipt, error) {
	limits := adapter.Limits{MaxRows: input.Limits.MaxRows, MaxBytes: input.Limits.MaxBytes, BatchRows: input.Limits.BatchRows}
	// Arrow IPC writes each message, buffer and pad separately; buffered,
	// a small result leaves in one write (one Result frame) per batch.
	// Limits and the digest still see every byte as it is produced.
	// The 64 KiB buffer is pooled: allocating it per read was the largest single
	// allocation of a small read. Nothing touches it after executeRead returns.
	buffered := resultBuffers.Get().(*bufio.Writer)
	buffered.Reset(output)
	defer func() {
		buffered.Reset(nil) // drop the reference to output
		resultBuffers.Put(buffered)
	}()
	wire := &boundedHashWriter{writer: buffered, hash: sha256.New(), maximum: input.Limits.MaxBytes}
	sink := &arrowSink{ctx: ctx, wire: wire, flush: buffered.Flush, maxRows: input.Limits.MaxRows, batchRows: int64(input.Limits.BatchRows)}
	var stats adapter.QueryStats
	var err error
	if invocation.Query != nil {
		s, ok := session.(adapter.QuerySession)
		if !ok {
			receipt.Outcome = operations.Rejected
			receipt.ErrorCode = "UNSUPPORTED"
			return receipt, adapter.ErrUnsupported
		}
		var receiver adapter.Sink = sink
		receiver = readSink(input.Source.Engine, sink, input.Limits.MaxBytes)
		stats, err = s.Query(ctx, *invocation.Query, receiver)
	} else {
		s, ok := session.(adapter.MetadataSession)
		if !ok {
			receipt.Outcome = operations.Rejected
			receipt.ErrorCode = "UNSUPPORTED"
			return receipt, adapter.ErrUnsupported
		}
		stats, err = s.Inspect(ctx, *invocation.Metadata, limits, sink)
	}
	if sink.writer != nil {
		closeErr := sink.writer.Close()
		if closeErr == nil {
			closeErr = buffered.Flush()
		}
		if err == nil {
			err = closeErr
		}
	}
	if err != nil {
		receipt.SourceError = sourceError(err, nil)
		return receipt, err
	}
	if sink.writer == nil || stats.Rows != sink.rows || ctx.Err() != nil {
		return receipt, errors.New("adapter result incomplete")
	}
	receipt.Outcome = operations.Completed
	receipt.ErrorCode = ""
	receipt.Result = &operations.ResultRef{ID: input.OperationID, SHA256: hex.EncodeToString(wire.hash.Sum(nil)), Bytes: wire.bytes, Rows: sink.rows, Format: "arrow_ipc"}
	return receipt, nil
}

// resultBufferBytes holds a small result's IPC messages until its batch ends.
// Larger writes pass straight through, so big results stream as before.
const resultBufferBytes = 64 << 10

var resultBuffers = sync.Pool{New: func() any { return bufio.NewWriterSize(nil, resultBufferBytes) }}

type boundedHashWriter struct {
	writer         io.Writer
	hash           hash.Hash
	bytes, maximum int64
}

func (w *boundedHashWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.maximum-w.bytes {
		return 0, adapter.ErrLimit
	}
	n, err := w.writer.Write(p)
	if n < 0 || n > len(p) {
		return 0, io.ErrShortWrite
	}
	if n > 0 {
		_, _ = w.hash.Write(p[:n])
		w.bytes += int64(n)
	}
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

type arrowSink struct {
	ctx                      context.Context
	wire                     *boundedHashWriter
	writer                   *ipc.Writer
	flush                    func() error // sends what one batch buffered
	schema                   *arrow.Schema
	rows, maxRows, batchRows int64
}

func (s *arrowSink) Schema(schema *arrow.Schema) error {
	if schema == nil || s.writer != nil {
		return adapter.ErrInvalid
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	s.schema = schema
	s.writer = ipc.NewWriter(s.wire, ipc.WithSchema(schema))
	return nil
}
func (s *arrowSink) Write(record arrow.RecordBatch) error {
	if s.writer == nil || record == nil || record.Schema() == nil || !record.Schema().Equal(s.schema) || !record.Schema().Metadata().Equal(s.schema.Metadata()) {
		return adapter.ErrInvalid
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if record.NumRows() < 0 || record.NumRows() > s.maxRows-s.rows || record.NumRows() > s.batchRows {
		return adapter.ErrLimit
	}
	if err := s.writer.Write(record); err != nil {
		return err
	}
	if s.flush != nil {
		if err := s.flush(); err != nil {
			return err
		}
	}
	s.rows += record.NumRows()
	return nil
}
