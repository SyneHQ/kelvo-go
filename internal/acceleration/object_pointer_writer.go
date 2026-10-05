// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

type pointerWriterState struct {
	openingDone, quiet chan struct{}
	finishOnce         sync.Once
	reader             objectstore.Client
	initial            objectState
	openErr, closeErr  error
}

// beginPointerWriter requires an already-reserved runtime operation. On error,
// a nonnil transaction may still own late construction or cleanup: only finish
// and pointerQuiesced are then safe. The caller keeps its operation charged.
func (backend *objectBackend) beginPointerWriter(ctx context.Context, dataset string, meter *verificationReader) (*objectTransaction, error) {
	if ctx == nil || meter == nil {
		return nil, errReaderInvalid
	}
	if err := backend.check(ctx, dataset); err != nil {
		return nil, err
	}
	if !backend.protected() || backend.runtime == nil || backend.runtime.registry == nil {
		return nil, ErrProtectionRequired
	}
	if err := meter.Err(); err != nil {
		return nil, err
	}
	tx, err := backend.reserveWriter(ctx, dataset)
	if err != nil {
		return nil, err
	}
	tx.pointer = &pointerWriterState{openingDone: make(chan struct{}), quiet: make(chan struct{})}
	go func() {
		tx.pointer.openErr = tx.openPointerWriter(meter)
		close(tx.pointer.openingDone)
	}()
	select {
	case <-tx.pointer.openingDone:
		err = errors.Join(tx.pointer.openErr, backend.writerOpeningError(tx.ctx))
		if err == nil {
			return tx, nil
		}
	case <-tx.ctx.Done():
		err = context.Cause(tx.ctx)
	}
	tx.startPointerFinish()
	return tx, err
}

func (tx *objectTransaction) openPointerWriter(meter *verificationReader) error {
	if err := tx.backend.writerOpeningError(tx.ctx); err != nil {
		return err
	}
	var err error
	tx.client, tx.closeClient, err = tx.backend.writeClient()
	if err != nil {
		return err
	}
	if nilReaderDependency(tx.client) {
		return errReaderInvalid
	}
	if err := tx.backend.writerOpeningError(tx.ctx); err != nil {
		return err
	}
	var owner [16]byte
	if _, err := rand.Read(owner[:]); err != nil {
		return err
	}
	tx.owner = hex.EncodeToString(owner[:])
	ref, err := readerlease.NewReference(tx.backend.config.TenantID, tx.dataset, tx.owner)
	if err != nil {
		return err
	}
	tx.readerReference = &ref
	tx.pointer.reader = meter.metadataClient(tx.client)
	if err := tx.claimWriter(tx.pointer.reader, false); err != nil {
		return err
	}
	tx.pointer.initial = tx.state
	tx.startRenewal()
	return tx.backend.writerOpeningError(tx.ctx)
}

// Successful opening publishes a value captured before renewal starts. Callers
// must not contend on stateMu while the renewer owns a provider call.
func (tx *objectTransaction) pointerInitialState() objectState { return tx.pointer.initial }

func (tx *objectTransaction) readPointerState(ctx context.Context) (objectState, error) {
	if tx.pointer == nil || ctx == nil {
		return objectState{}, errReaderInvalid
	}
	select {
	case <-tx.pointer.openingDone:
	default:
		return objectState{}, errReaderInvalid
	}
	if tx.pointer.openErr != nil {
		return objectState{}, tx.pointer.openErr
	}
	if tx.pointer.reader == nil {
		return objectState{}, errReaderInvalid
	}
	if err := context.Cause(tx.ctx); err != nil {
		return objectState{}, err
	}
	return tx.backend.readState(ctx, tx.dataset, tx.pointer.reader)
}

func (tx *objectTransaction) pointerQuiesced() <-chan struct{} { return tx.pointer.quiet }

func (tx *objectTransaction) startPointerFinish() {
	tx.pointer.finishOnce.Do(func() {
		go func() {
			<-tx.pointer.openingDone
			tx.finishMu.Lock()
			defer tx.finishMu.Unlock()
			tx.done = true
			tx.pointer.closeErr = tx.cleanupPointerWriter()
			// Writer removal precedes this transaction's handback proof.
			tx.backend.writerFinished(tx, tx.pointer.closeErr)
			close(tx.pointer.quiet)
		}()
	})
}

func (tx *objectTransaction) finishPointerWriter(ctx context.Context) error {
	tx.startPointerFinish()
	if ctx == nil {
		return errReaderInvalid
	}
	select {
	case <-tx.pointer.quiet:
		return tx.pointer.closeErr
	case <-ctx.Done():
		return errors.Join(context.Cause(ctx), errReaderCleanupUnknown)
	}
}

func (tx *objectTransaction) cleanupPointerWriter() error {
	tx.stopRenewal()
	// Preserve operational cancellation before our routine teardown cancels ctx.
	result := errors.Join(tx.pointer.openErr, context.Cause(tx.ctx))
	if tx.owned {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result = errors.Join(result, tx.releasePointerWriter(cleanup))
		cancel()
		tx.owned = false
	}
	if tx.closeClient && !nilReaderDependency(tx.client) {
		tx.client.Close()
		tx.closeClient = false
	}
	// A caller/runtime may have cancelled while release or client Close waited.
	result = errors.Join(result, context.Cause(tx.ctx))
	tx.cancel(context.Canceled)
	return result
}

// releasePointerWriter uses only fixed-size root I/O outside the exhausted
// verification budget. It cannot publish a generation or infer release from an
// unchanged generation alone. Legacy refresh/restore cleanup stays unchanged.
func (tx *objectTransaction) releasePointerWriter(ctx context.Context) error {
	state, err := tx.backend.readState(ctx, tx.dataset, tx.client)
	if err != nil {
		return errors.Join(errReaderCleanupUnknown, err)
	}
	writer := state.manifest.Writer
	if writer == nil || writer.Owner != tx.owner || !sameReaderReference(writer.ReaderReference, tx.readerReference) {
		return ErrLeaseLost
	}
	live := tx.ownsWriter(state)
	manifest := state.manifest
	manifest.Writer = nil
	_, err = tx.backend.writeState(ctx, tx.client, state, manifest)
	if errors.Is(err, objectstore.ErrConflict) {
		return errors.Join(ErrLeaseLost, err)
	}
	if err != nil {
		latest, readErr := tx.backend.readState(ctx, tx.dataset, tx.client)
		if readErr != nil || !sameReleasedPointer(latest.manifest, manifest) {
			return errors.Join(errReaderCleanupUnknown, err, readErr)
		}
	}
	if !live {
		return ErrLeaseLost
	}
	return nil
}

func sameReleasedPointer(actual, expected objectManifest) bool {
	if actual.Writer != nil || actual.Version != expected.Version || actual.Dataset != expected.Dataset ||
		actual.HistoryTruncated != expected.HistoryTruncated || len(actual.History) != len(expected.History) ||
		!sameObjectCommit(actual.Committed, expected.Committed) {
		return false
	}
	for i, candidate := range actual.History {
		if !sameObjectCommit(candidate, expected.History[i]) {
			return false
		}
	}
	return true
}
