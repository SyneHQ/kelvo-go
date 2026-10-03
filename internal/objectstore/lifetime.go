// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"context"
	"errors"
	"io"
	"sync"
)

var errClientClosed = errors.New("object storage client is closed")

// clientLifetime tracks caller-owned work, not the transport's internal
// goroutines. Records are proportional to outstanding operations/returned
// bodies; MaxConnsPerHost is not an admission or memory bound.
type clientLifetime struct {
	mu       sync.Mutex
	closed   bool
	active   map[*clientOperation]struct{}
	closeOne sync.Once
}

func (l *clientLifetime) begin(parent context.Context) (*clientOperation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, errClientClosed
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	op := &clientOperation{owner: l, ctx: ctx, cancel: cancel, methodDone: make(chan struct{}), done: make(chan struct{}), bodyEnded: true}
	if l.active == nil {
		l.active = make(map[*clientOperation]struct{})
	}
	l.active[op] = struct{}{}
	return op, nil
}

func (l *clientLifetime) close(closeIdle func()) {
	l.closeOne.Do(func() {
		l.mu.Lock()
		l.closed = true
		operations := make([]*clientOperation, 0, len(l.active))
		for op := range l.active {
			operations = append(operations, op)
		}
		l.mu.Unlock()
		// Cancel every operation first. A stalled method/Close must not prevent
		// cancellation callbacks from closing other already-returned bodies.
		for _, op := range operations {
			op.cancel()
		}
		for _, op := range operations {
			<-op.methodDone
			op.mu.Lock()
			body := op.body
			op.mu.Unlock()
			if body != nil {
				_ = body.Close()
			}
			<-op.done
		}
		closeIdle()
	})
}

type clientOperation struct {
	owner       *clientLifetime
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	body        *clientBody
	methodEnded bool
	bodyEnded   bool
	completed   bool
	methodDone  chan struct{}
	done        chan struct{}
}

// Called only after validation, with the entire decorated body. This keeps
// range framing and Azure error-redaction readers inside read serialization.
func (op *clientOperation) returnBody(body io.ReadCloser) (io.ReadCloser, error) {
	op.mu.Lock()
	if op.body != nil {
		op.mu.Unlock()
		_ = body.Close()
		return nil, errors.New("object response body was already assigned")
	}
	managed := newClientBody(op.ctx, body, func() { op.finish(false) })
	op.body, op.bodyEnded = managed, false
	op.mu.Unlock()
	if err := op.ctx.Err(); err != nil {
		_ = managed.Close()
		return nil, err
	}
	return managed, nil
}

// The method defer is registered before validation/Seek and executes after
// response Close and upload.wait. A returned body's explicit Close is the
// second completion proof; EOF or a canceled context alone is insufficient.
func (op *clientOperation) finish(method bool) {
	op.mu.Lock()
	if method {
		op.methodEnded = true
		close(op.methodDone)
	} else {
		op.bodyEnded = true
	}
	complete := op.methodEnded && op.bodyEnded && !op.completed
	if complete {
		op.completed = true
	}
	op.mu.Unlock()
	if complete {
		op.cancel()
		op.owner.mu.Lock()
		delete(op.owner.active, op)
		op.owner.mu.Unlock()
		close(op.done)
	}
}

type clientBody struct {
	body         io.ReadCloser
	readMu       sync.Mutex
	mu           sync.Mutex
	reads        int
	sealed       bool
	closeEnded   bool
	readsDone    chan struct{}
	closeOne     sync.Once
	publicOne    sync.Once
	closeErr     error
	stopCallback func() bool
	callbackDone chan struct{}
	finished     func()
}

func newClientBody(ctx context.Context, body io.ReadCloser, finished func()) *clientBody {
	b := &clientBody{body: body, readsDone: make(chan struct{}), callbackDone: make(chan struct{}), finished: finished}
	b.stopCallback = context.AfterFunc(ctx, func() {
		defer close(b.callbackDone)
		b.closeInternal()
	})
	return b
}

func (b *clientBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.sealed {
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	b.reads++
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.reads--
		if b.closeEnded && b.reads == 0 {
			close(b.readsDone)
		}
		b.mu.Unlock()
	}()
	b.readMu.Lock()
	defer b.readMu.Unlock()
	b.mu.Lock()
	sealed := b.sealed
	b.mu.Unlock()
	if sealed {
		return 0, io.ErrClosedPipe
	}
	return b.body.Read(p)
}

func (b *clientBody) closeInternal() {
	b.closeOne.Do(func() {
		b.mu.Lock()
		b.sealed = true
		b.mu.Unlock()
		// Never take readMu: provider Close must be able to interrupt Read.
		b.closeErr = b.body.Close()
		b.mu.Lock()
		b.closeEnded = true
		if b.reads == 0 {
			close(b.readsDone)
		}
		b.mu.Unlock()
	})
	<-b.readsDone
}

func (b *clientBody) Close() error {
	b.publicOne.Do(func() {
		stopped := b.stopCallback()
		b.closeInternal()
		if !stopped {
			<-b.callbackDone
		}
		b.finished()
	})
	return b.closeErr
}
