// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"io"
	"sync"
)

// uploadBody borrows a caller-owned upload reader. The transport may close its
// request body after Client.Do returns, including on error. Close seals promptly
// without closing the caller's reader or waiting on an in-flight Read. Put must
// separately wait after closing the response body before returning ownership.
type uploadBody struct {
	reader io.Reader
	readMu sync.Mutex
	mu     sync.Mutex
	reads  int
	closed bool
	done   chan struct{}
}

func newUploadBody(reader io.Reader, size int64) *uploadBody {
	return &uploadBody{reader: io.LimitReader(reader, size), done: make(chan struct{})}
}

func (b *uploadBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	b.reads++
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.reads--
		if b.closed && b.reads == 0 {
			close(b.done)
		}
		b.mu.Unlock()
	}()

	// The caller's reader and io.LimitedReader need not support concurrent Read.
	// A queued read stays charged, but must not reach either after Close seals.
	b.readMu.Lock()
	defer b.readMu.Unlock()
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return 0, io.ErrClosedPipe
	}
	return b.reader.Read(p)
}

func (b *uploadBody) Close() error {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		if b.reads == 0 {
			close(b.done)
		}
	}
	b.mu.Unlock()
	return nil
}

// Cancellation or a timeout cannot prove the transport stopped using the
// caller's reader. A stalled Close or Read deliberately retains this invocation.
func (b *uploadBody) wait() { <-b.done }
