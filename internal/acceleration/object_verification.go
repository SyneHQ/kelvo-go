// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

// Verification borrows the immutable node reader. Its operation reservation
// covers the root, every body handback, and any unfinished guard finalization.
type protectedVerification struct {
	op     *objectRuntimeOperation
	reader *verificationReader
	cancel context.CancelFunc
	guard  *ReadGuard
	done   func()
}

func (backend *objectBackend) beginProtectedVerification(ctx context.Context, dataset string) (*protectedVerification, error) {
	if ctx == nil || !backend.protected() || backend.runtime == nil {
		return nil, ErrProtectionRequired
	}
	if err := backend.check(ctx, dataset); err != nil {
		return nil, err
	}
	definition, found := backend.runtime.config.Dataset(dataset)
	if !found {
		return nil, ErrNotFound
	}
	limits, err := definition.EffectiveVerificationLimits()
	if err != nil {
		return nil, err
	}
	if err := definition.Limits.Validate(); err != nil {
		return nil, err
	}
	client, ok := backend.reader.(objectstore.RangeClient)
	if !ok || nilReaderDependency(client) {
		return nil, ErrRecoveryUnsupported
	}
	bounded, cancel := context.WithTimeout(ctx, definition.Limits.Timeout)
	op, err := backend.runtime.beginOperation(bounded)
	if err != nil {
		cancel()
		return nil, err
	}
	v := &protectedVerification{op: op, reader: newVerificationReader(client, limits.MaxBytes), cancel: cancel}
	if err := v.reader.Err(); err != nil {
		return nil, errors.Join(err, v.close())
	}
	return v, nil
}

func (v *protectedVerification) check() error {
	err := errors.Join(context.Cause(v.op.Context()), v.reader.Err())
	if v.guard != nil {
		err = errors.Join(err, v.guard.Check())
	}
	return err
}

func (v *protectedVerification) close() error {
	// This function runs after all synchronous provider/body work. Cancelling
	// this timer is not handback; pin custody has its own independent context.
	defer v.cancel()
	err := v.check()
	if v.done != nil {
		v.done()
	}
	if v.guard != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = errors.Join(err, v.guard.Close(cleanup))
		cancel()
		select {
		case <-v.guard.Quiesced():
		default:
			// Exactly one waiter for this still-charged operation (at most
			// objectRuntimeOperationLimit). It never changes returned results.
			go func() {
				<-v.guard.Quiesced()
				_ = v.op.Close()
			}()
			return errors.Join(err, errReaderCleanupUnknown)
		}
	}
	return errors.Join(err, v.op.Close())
}

// Only internal content-integrity errors may become an unverified Inventory
// entry. A mixed joined error, unknown leaf or custom Is method is insufficient.
func onlyObjectCorruption(err error) bool {
	var pure func(error, int) bool
	visits := 0
	pure = func(err error, depth int) bool {
		visits++
		if err == nil || depth > 32 || visits > 64 {
			return false
		}
		if err == ErrCorrupt {
			return true
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() []error }:
			children := wrapped.Unwrap()
			if len(children) == 0 {
				return false
			}
			for _, child := range children {
				if !pure(child, depth+1) {
					return false
				}
			}
			return true
		case interface{ Unwrap() error }:
			return pure(wrapped.Unwrap(), depth+1)
		default:
			return false
		}
	}
	return pure(err, 0)
}

func verificationSummary(selected selectedObjectSnapshot) Snapshot {
	c := selected.committed
	// A corrupt descriptor must not turn arbitrary parts or keys into a usable
	// snapshot. This summary contains only validated, canonical root metadata.
	return Snapshot{Dataset: selected.dataset, Generation: c.Generation, SchemaHash: c.SchemaHash,
		Fingerprint: c.Fingerprint, SHA256: c.SHA256, Rows: c.Rows, Bytes: c.Bytes, RefreshedAt: c.RefreshedAt,
		ageObserved: selected.observation.ageObserved, ageObservedAt: selected.observation.ageObservedAt}
}

func (backend *objectBackend) protectedVerificationResults(ctx context.Context, dataset string, inventory bool) (result []Generation, resultErr error) {
	v, err := backend.beginProtectedVerification(ctx, dataset)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, v.close())
		if resultErr != nil {
			result = nil
		}
	}()
	state, err := backend.readState(v.op.Context(), dataset, v.reader)
	if check := v.check(); err != nil || check != nil {
		return nil, errors.Join(err, check)
	}
	count := 1
	if inventory {
		count += len(state.manifest.History)
	}
	if count > ObjectHistoryLimit+1 {
		return nil, ErrCorrupt
	}
	selected := make([]selectedObjectSnapshot, 0, count)
	bindings := make([]readerlease.Binding, 0, count)
	var minimumBytes int64
	anchor := time.Now()
	reference := state.info.ServerTime.Add(anchor.Sub(state.receivedAt))
	for index := range count {
		entry, err := backend.selectObjectStateEntry(v.op.Context(), dataset, state, index, "", 0, false)
		if err != nil {
			return nil, err
		}
		entry.reference = reference
		entry.observation = Snapshot{Fingerprint: entry.committed.Fingerprint, RefreshedAt: entry.committed.RefreshedAt}.observeClock(reference)
		entry.observation.ageObservedAt = anchor
		selected = append(selected, entry)
		bindings = append(bindings, entry.binding)
		// An overflow-safe lower bound only; the reader separately charges root,
		// descriptors, checksum bytes, repeated footer reads and EOF allowances.
		for _, bytes := range []int64{entry.committed.Bytes, descriptorBytes(entry.committed)} {
			if bytes > v.reader.Remaining()-minimumBytes {
				return nil, v.reader.Preflight(v.reader.Remaining() + 1)
			}
			minimumBytes += bytes
		}
	}
	if err := v.reader.Preflight(minimumBytes); err != nil {
		return nil, err
	}
	v.guard, err = backend.runtime.acquire(v.op.Context(), bindings)
	if err != nil {
		return nil, err
	}
	if v.guard == nil {
		return nil, ErrProtectionRequired
	}
	v.done, err = v.guard.HoldConsumer()
	if err != nil {
		return nil, err
	}
	result = make([]Generation, 0, count)
	for index, entry := range selected {
		if err := v.check(); err != nil {
			return nil, err
		}
		snapshot, verifyErr := backend.loadSelectedObjectWithReader(v.guard.Context(), entry, v.reader)
		if verifyErr == nil {
			_, verifyErr = backend.objectSnapshotSchemaWithReader(v.guard.Context(), snapshot, true, v.reader)
		}
		// Parsers may wrap reader failures as corruption. The independent meter
		// and guard always win, including after the final successful body Close.
		if err := v.check(); err != nil {
			return nil, errors.Join(verifyErr, err)
		}
		if verifyErr != nil {
			if !inventory || !onlyObjectCorruption(verifyErr) {
				return nil, verifyErr
			}
			snapshot = verificationSummary(entry)
		}
		result = append(result, Generation{Snapshot: snapshot, Active: index == 0, Verified: verifyErr == nil,
			CatalogScope: "retained_manifest", CatalogTruncated: state.manifest.HistoryTruncated})
	}
	return result, v.check()
}

func descriptorBytes(committed *objectCommitted) int64 {
	if committed.Descriptor != nil {
		return committed.Descriptor.Bytes
	}
	return 0
}

func (backend *objectBackend) verifyProtectedObject(ctx context.Context, dataset string) (Snapshot, error) {
	result, err := backend.protectedVerificationResults(ctx, dataset, false)
	if err != nil {
		return Snapshot{}, err
	}
	return result[0].Snapshot, nil
}

func (backend *objectBackend) inventoryProtectedObjects(ctx context.Context, dataset string) ([]Generation, error) {
	return backend.protectedVerificationResults(ctx, dataset, true)
}
