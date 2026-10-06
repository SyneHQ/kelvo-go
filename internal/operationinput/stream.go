// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operationinput

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// StreamWriter reserves persistent capacity before returning. Writes use a
// synchronous pipe and one fixed-size chunk; no whole-result buffer is retained.
// The caller must Commit or Close and must serialize Write and Commit calls.
type StreamWriter struct {
	pipe   *io.PipeWriter
	cancel context.CancelFunc
	done   chan struct{}
	ref    operations.InputRef
	err    error
	once   sync.Once
}

func (s *Store) BeginStream(ctx context.Context, identity exports.Identity, expiry time.Time, format Format) (*StreamWriter, error) {
	if s == nil || s.storage == nil || ctx == nil || !validFormat(format) || ctx.Err() != nil {
		return nil, ErrInvalid
	}
	maximum := s.maximum
	if format == OperationRequest {
		maximum = min(maximum, int64(operations.MaxRequestBytes))
	}
	w, err := s.storage.Begin(ctx, exports.Request{Identity: identity, ExpiresAt: expiry, Limits: inputLimits(maximum)}, inputSchema(format))
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	stream := &StreamWriter{pipe: writer, cancel: cancel, done: make(chan struct{})}
	stop := context.AfterFunc(streamCtx, func() { _ = reader.CloseWithError(streamCtx.Err()) })
	go func() {
		defer close(stream.done)
		defer stop()
		stream.ref, stream.err = s.write(streamCtx, identity, expiry, format, maximum, w, reader)
		_ = reader.CloseWithError(stream.err)
	}()
	return stream, nil
}

func (s *StreamWriter) Write(data []byte) (int, error) { return s.pipe.Write(data) }

// Commit returns a nonempty ref on an uncertain publication as well. Callers
// must not retry publication or report that ref as a confirmed result.
func (s *StreamWriter) Commit() (operations.InputRef, error) {
	_ = s.pipe.Close()
	<-s.done
	s.cancel()
	return s.ref, s.err
}

// Close aborts an unfinished stream and joins storage cleanup. It never erases
// a committed or uncertain publication; normal retention cleanup owns those.
func (s *StreamWriter) Close() error {
	s.once.Do(func() { _ = s.pipe.CloseWithError(context.Canceled); s.cancel(); <-s.done })
	if errors.Is(s.err, context.Canceled) || errors.Is(s.err, io.ErrClosedPipe) {
		return nil
	}
	return s.err
}
