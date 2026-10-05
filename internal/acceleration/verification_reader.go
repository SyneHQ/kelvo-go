// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

var errVerificationLimit = query.NewError("RESOURCE_EXHAUSTED", "Protected verification byte budget exhausted")

// verificationReader borrows the runtime's client for one operation. The budget
// covers exposed response bytes and a conservative byte per range for the
// provider's internal EOF probe, not wire traffic or registry control requests.
// Its error ledger survives Parquet readers that replace I/O errors with corrupt
// metadata errors. Callers retain custody through every returned body's Close.
type verificationReader struct {
	client           objectstore.RangeClient
	limit            int64
	mu               sync.Mutex
	used, reserved   int64
	failure, cleanup error
}

func newVerificationReader(client objectstore.RangeClient, maxBytes int64) *verificationReader {
	r := &verificationReader{client: client, limit: maxBytes}
	if nilReaderDependency(client) || (catalog.VerificationLimits{MaxBytes: maxBytes}).Validate() != nil {
		r.failure = errReaderInvalid
	}
	return r
}

func (r *verificationReader) errorLocked() error { return errors.Join(r.failure, r.cleanup) }

func (r *verificationReader) failLocked(err error) {
	if err != nil && r.failure == nil {
		r.failure = err
	}
}

func (r *verificationReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.errorLocked()
}

func (r *verificationReader) Remaining() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.limit < 1 || r.limit > 1<<45 {
		return 0
	}
	return r.limit - r.used - r.reserved
}

// Preflight rejects a declared lower bound without charging it. Actual reads
// remain authoritative, and in-flight reservations are not available twice.
func (r *verificationReader) Preflight(bytes int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.errorLocked() == nil && (bytes < 0 || bytes > r.limit-r.used-r.reserved) {
		r.failLocked(errVerificationLimit)
	}
	return r.errorLocked()
}

func (r *verificationReader) check(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx == nil {
		r.failLocked(errReaderInvalid)
	} else {
		r.failLocked(context.Cause(ctx))
	}
	return r.errorLocked()
}

func (r *verificationReader) returned(ctx context.Context, body io.ReadCloser, info objectstore.Info, err error) (io.ReadCloser, objectstore.Info, error) {
	r.mu.Lock()
	r.failLocked(err)
	r.failLocked(context.Cause(ctx))
	var wrapped io.ReadCloser
	if !nilReaderDependency(body) {
		wrapped = &verificationBody{owner: r, body: body, ctx: ctx, readsDone: make(chan struct{})}
	} else if err == nil {
		r.failLocked(errReaderInvalid)
	}
	resultErr := r.errorLocked()
	r.mu.Unlock()
	return wrapped, info, resultErr
}

func (r *verificationReader) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	if err := r.check(ctx); err != nil {
		return nil, objectstore.Info{}, err
	}
	if err := r.Preflight(1); err != nil {
		return nil, objectstore.Info{}, err
	}
	body, info, err := r.client.Get(ctx, key, version)
	return r.returned(ctx, body, info, err)
}

func (r *verificationReader) Head(ctx context.Context, key, version string) (objectstore.Info, error) {
	if err := r.check(ctx); err != nil {
		return objectstore.Info{}, err
	}
	info, err := r.client.Head(ctx, key, version)
	r.mu.Lock()
	r.failLocked(err)
	r.failLocked(context.Cause(ctx))
	resultErr := r.errorLocked()
	r.mu.Unlock()
	return info, resultErr
}

func (r *verificationReader) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, objectstore.Info, error) {
	if err := r.check(ctx); err != nil {
		return nil, objectstore.Info{}, err
	}
	r.mu.Lock()
	if offset < 0 || length < 1 || offset >= objectstore.MaxUploadBytes || length > objectstore.MaxUploadBytes-offset {
		r.failLocked(errReaderInvalid)
	} else if available := r.limit - r.used - r.reserved; available < 1 || length > available-1 {
		r.failLocked(errVerificationLimit)
	}
	if err := r.errorLocked(); err != nil {
		r.mu.Unlock()
		return nil, objectstore.Info{}, err
	}
	// ExactRangeBody can consume one byte without exposing it to this wrapper.
	// Charge its allowance before the request, even when the eventual probe is EOF.
	r.used++
	r.mu.Unlock()
	body, info, err := r.client.GetRange(ctx, key, version, offset, length)
	return r.returned(ctx, body, info, err)
}

func (r *verificationReader) Put(context.Context, string, io.ReadSeeker, int64, string, objectstore.Condition) (objectstore.Info, error) {
	r.mu.Lock()
	r.failLocked(errReaderInvalid)
	err := r.errorLocked()
	r.mu.Unlock()
	return objectstore.Info{}, err
}

// Close does not own the shared provider. Operation/guard custody owns returned
// bodies; closing this borrowed view must not interrupt another runtime user.
func (*verificationReader) Close() {}

func (r *verificationReader) reserve(ctx context.Context, size int) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failLocked(context.Cause(ctx))
	if r.errorLocked() != nil {
		return 0, r.errorLocked()
	}
	available := r.limit - r.used - r.reserved
	if available == 0 {
		r.failLocked(errVerificationLimit)
		return 0, r.errorLocked()
	}
	size = int(min(int64(size), available))
	r.reserved += int64(size)
	return size, nil
}

func (r *verificationReader) finishRead(ctx context.Context, reserved, count int, err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reserved -= int64(reserved)
	r.used += int64(count)
	if err != io.EOF {
		r.failLocked(err)
	}
	r.failLocked(context.Cause(ctx))
	if failure := r.errorLocked(); failure != nil {
		return failure
	}
	return err
}

type verificationBody struct {
	owner     *verificationReader
	body      io.ReadCloser
	ctx       context.Context
	mu        sync.Mutex
	readMu    sync.Mutex
	reads     int
	closing   bool
	readsDone chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (b *verificationBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.mu.Lock()
	if b.closing {
		b.mu.Unlock()
		b.owner.mu.Lock()
		b.owner.failLocked(io.ErrClosedPipe)
		err := b.owner.errorLocked()
		b.owner.mu.Unlock()
		return 0, err
	}
	b.reads++
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.reads--
		if b.closing && b.reads == 0 {
			close(b.readsDone)
		}
		b.mu.Unlock()
	}()
	b.readMu.Lock()
	defer b.readMu.Unlock()
	b.mu.Lock()
	closing := b.closing
	b.mu.Unlock()
	if closing {
		b.owner.mu.Lock()
		b.owner.failLocked(io.ErrClosedPipe)
		err := b.owner.errorLocked()
		b.owner.mu.Unlock()
		return 0, err
	}
	reserved, err := b.owner.reserve(b.ctx, len(p))
	if err != nil {
		return 0, err
	}
	n, err := b.body.Read(p[:reserved])
	if n < 0 || n > reserved {
		// A broken provider cannot make counters negative or overflow the budget.
		return 0, b.owner.finishRead(b.ctx, reserved, reserved, errReaderInvalid)
	}
	return n, b.owner.finishRead(b.ctx, reserved, n, err)
}

func (b *verificationBody) Close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closing = true
		if b.reads == 0 {
			close(b.readsDone)
		}
		b.mu.Unlock()
		// Do not take a read lock: provider Close may be what releases blocked Read.
		if err := b.body.Close(); err != nil {
			b.closeErr = errReaderCleanupUnknown
			b.owner.mu.Lock()
			b.owner.cleanup = errReaderCleanupUnknown
			b.owner.mu.Unlock()
		}
		<-b.readsDone
	})
	return b.closeErr
}

var _ objectstore.RangeClient = (*verificationReader)(nil)
