// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"sync"
)

// Tracks only writer construction and transactions owned by this backend.
// Read operations and query consumers have separate lifetime contracts.
type objectWriterLifetime struct {
	mu      sync.Mutex
	pending map[*objectTransaction]struct{}
	joined  sync.WaitGroup
	err     error
}

// Reserve before constructing a client or opening staging. The same lock seals
// admission before Wait, so an Add cannot race the drain's final join.
func (backend *objectBackend) reserveWriter(ctx context.Context, dataset string) (*objectTransaction, error) {
	lifetime := &backend.writers
	lifetime.mu.Lock()
	defer lifetime.mu.Unlock()
	if backend.closed.Load() {
		return nil, errBackendClosed
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	tx := &objectTransaction{backend: backend, dataset: dataset}
	tx.ctx, tx.cancel = context.WithCancelCause(ctx)
	if lifetime.pending == nil {
		lifetime.pending = make(map[*objectTransaction]struct{})
	}
	lifetime.pending[tx] = struct{}{}
	lifetime.joined.Add(1)
	return tx, nil
}

func (backend *objectBackend) writerFinished(tx *objectTransaction, cleanupErr error) {
	lifetime := &backend.writers
	lifetime.mu.Lock()
	defer lifetime.mu.Unlock()
	if _, owned := lifetime.pending[tx]; !owned {
		return
	}
	delete(lifetime.pending, tx)
	if backend.closed.Load() {
		lifetime.err = errors.Join(lifetime.err, cleanupErr)
	}
	lifetime.joined.Done()
}

func (backend *objectBackend) writerOpeningError(ctx context.Context) error {
	if backend.closed.Load() {
		return errBackendClosed
	}
	return context.Cause(ctx)
}

func (backend *objectBackend) drainWriters() error {
	lifetime := &backend.writers
	lifetime.mu.Lock()
	backend.closed.Store(true)
	writers := make([]*objectTransaction, 0, len(lifetime.pending))
	for tx := range lifetime.pending {
		writers = append(writers, tx)
	}
	lifetime.mu.Unlock()

	// Cancellation only requests that the caller stop using File/NewPart.
	// Calling Abort here would close borrowed staging underneath that caller.
	// The caller must return it via Commit/Abort; pending Begin cleans itself.
	for _, tx := range writers {
		tx.cancel(errBackendClosed)
	}
	lifetime.joined.Wait()
	lifetime.mu.Lock()
	defer lifetime.mu.Unlock()
	return lifetime.err
}
