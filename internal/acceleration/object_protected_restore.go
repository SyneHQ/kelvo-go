// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
	"github.com/apache/arrow-go/v18/arrow"
)

func (backend *objectBackend) restoreAuthority(ctx context.Context, request RestoreRequest) error {
	if ctx == nil || backend.runtime == nil {
		return ErrProtectionRequired
	}
	if err := backend.check(ctx, request.Dataset); err != nil {
		return err
	}
	if err := backend.runtime.Match(backend.runtime.config); err != nil {
		return err
	}
	fingerprint, err := backend.runtime.config.DatasetFingerprint(request.Dataset)
	if err != nil {
		return err
	}
	if fingerprint != request.Fingerprint {
		return ErrFingerprintMismatch
	}
	return nil
}

// restoreEntry never discovers arbitrary generation keys. Its index addresses
// only a validated current/retained entry in one root.
func restoreEntry(manifest objectManifest, generation string) (int, *objectCommitted) {
	if manifest.Committed != nil && manifest.Committed.Generation == generation {
		return 0, manifest.Committed
	}
	for index, candidate := range manifest.History {
		if candidate != nil && candidate.Generation == generation {
			return index + 1, candidate
		}
	}
	return -1, nil
}

func (backend *objectBackend) restoreProtectedObject(ctx context.Context, request RestoreRequest) (snapshot Snapshot, resultErr error) {
	outcome := RestoreNotAttempted
	defer func() { resultErr = WithRestoreOutcome(resultErr, outcome) }()
	if !storeDatasetID.MatchString(request.Dataset) || !storeGenerationID.MatchString(request.Generation) ||
		!storeGenerationID.MatchString(request.ExpectedGeneration) || request.Fingerprint == "" || len(request.Fingerprint) > storeFingerprintLimit {
		return Snapshot{}, errors.New("restore requires dataset, generation, current generation and authorized fingerprint")
	}
	if err := backend.restoreAuthority(ctx, request); err != nil {
		return Snapshot{}, err
	}
	v, err := backend.beginProtectedVerification(ctx, request.Dataset)
	if err != nil {
		return Snapshot{}, err
	}
	var tx *objectTransaction
	verifiedNoOp := false
	defer func() {
		// Check execution before cleanup cancels the writer's private context.
		resultErr = errors.Join(resultErr, v.check())
		if tx != nil {
			resultErr = errors.Join(resultErr, context.Cause(tx.ctx))
		}
		cleanupErr, writerErr := closeProtectedRestore(v, tx)
		if verifiedNoOp && writerErr == nil {
			outcome = RestoreVerifiedNoOp
		}
		resultErr = errors.Join(resultErr, cleanupErr)
	}()
	tx, err = backend.beginPointerWriter(v.op.Context(), request.Dataset, v.reader)
	if err != nil {
		return Snapshot{}, err
	}
	initial := tx.pointerInitialState()
	if initial.manifest.Committed == nil {
		return Snapshot{}, ErrNotFound
	}
	if initial.manifest.Committed.Generation != request.ExpectedGeneration {
		return Snapshot{}, ErrRestoreConflict
	}
	index, targetCommit := restoreEntry(initial.manifest, request.Generation)
	if targetCommit == nil {
		return Snapshot{}, ErrNotFound
	}
	current, err := backend.selectObjectStateEntry(tx.ctx, request.Dataset, initial, 0, request.Fingerprint, 0, true)
	if err != nil {
		return Snapshot{}, err
	}
	target := current
	if index != 0 {
		target, err = backend.selectObjectStateEntry(tx.ctx, request.Dataset, initial, index, request.Fingerprint, 0, true)
		if err != nil {
			return Snapshot{}, err
		}
	}
	selected := []selectedObjectSnapshot{target}
	bindings := []readerlease.Binding{target.binding}
	if index != 0 {
		selected = append(selected, current)
		bindings = append(bindings, current.binding)
	}
	var minimum int64
	for _, entry := range selected {
		for _, size := range []int64{entry.committed.Bytes, descriptorBytes(entry.committed)} {
			if size > v.reader.Remaining()-minimum {
				return Snapshot{}, v.reader.Preflight(v.reader.Remaining() + 1)
			}
			minimum += size
		}
	}
	if err := v.reader.Preflight(minimum); err != nil {
		return Snapshot{}, err
	}
	// Pin custody follows the operation, not the writer's routine cleanup cancel.
	v.guard, err = backend.runtime.acquire(v.op.Context(), bindings)
	if err != nil {
		return Snapshot{}, err
	}
	if v.guard == nil {
		return Snapshot{}, ErrProtectionRequired
	}
	v.done, err = v.guard.HoldConsumer()
	if err != nil {
		return Snapshot{}, err
	}
	var targetSchema *arrow.Schema
	for position, entry := range selected {
		if err := errors.Join(v.check(), context.Cause(tx.ctx)); err != nil {
			return snapshot, err
		}
		candidate, err := backend.loadSelectedObjectWithReader(v.guard.Context(), entry, v.reader)
		var schema *arrow.Schema
		if err == nil {
			schema, err = backend.objectSnapshotSchemaWithReader(v.guard.Context(), candidate, true, v.reader)
		}
		if err = errors.Join(err, v.check(), context.Cause(tx.ctx)); err != nil {
			return snapshot, err
		}
		if position == 0 {
			snapshot, targetSchema = candidate, schema
		} else if !SchemaEqual(targetSchema, schema) {
			return snapshot, ErrCorrupt
		}
	}
	// Renewal stays live through every checksum, footer and final body Close.
	if err := tx.stopRestoreRenewal(v.guard.Context()); err != nil {
		return snapshot, err
	}
	if err := errors.Join(v.check(), context.Cause(tx.ctx), backend.restoreAuthority(v.op.Context(), request)); err != nil {
		return snapshot, err
	}
	latest, err := tx.readPointerState(v.guard.Context())
	if err = errors.Join(err, v.check(), context.Cause(tx.ctx)); err != nil {
		return snapshot, err
	}
	if !tx.ownsWriter(latest) {
		return snapshot, ErrLeaseLost
	}
	_, latestTarget := restoreEntry(latest.manifest, request.Generation)
	if !sameObjectCommit(latest.manifest.Committed, current.committed) || !sameObjectCommit(latestTarget, target.committed) {
		return snapshot, ErrRestoreConflict
	}
	if err := errors.Join(backend.restoreAuthority(v.op.Context(), request), v.check(), context.Cause(tx.ctx)); err != nil {
		return snapshot, err
	}
	if index == 0 {
		verifiedNoOp = true
		return snapshot.observeClock(latest.now()), nil
	}
	manifest, err := nextObjectManifest(latest.manifest, target.committed)
	if err != nil {
		return snapshot, err
	}
	if !tx.ownsWriter(latest) {
		return snapshot, ErrLeaseLost
	}
	if err := errors.Join(v.check(), context.Cause(tx.ctx)); err != nil {
		return snapshot, err
	}
	// This is the only publication. Reconciliation cannot issue a second CAS.
	published, err := backend.writeState(v.guard.Context(), tx.client, latest, manifest)
	if err == nil {
		tx.owned = false
		outcome = RestoreTargetObserved
		return snapshot.observeClock(published.now()), nil
	}
	if errors.Is(err, objectstore.ErrConflict) {
		outcome = RestoreNotPublished
		return snapshot, ErrRestoreConflict
	}
	tx.owned = false
	outcome = RestoreUnknown
	// Keep the original operation deadline and pins. A response after the deadline
	// is not permission to reconnect or extend publication reconciliation.
	observed, readErr := tx.readPointerState(v.guard.Context())
	checkErr := errors.Join(v.check(), context.Cause(tx.ctx), backend.restoreAuthority(v.op.Context(), request))
	if readErr == nil && sameObjectCommit(observed.manifest.Committed, target.committed) {
		outcome = RestoreTargetObserved
		return snapshot.observeClock(observed.now()), checkErr
	}
	return snapshot, errors.Join(ErrPublicationUnknown, err, readErr, checkErr)
}

// A cancelled provider renewal can still be inside Put. Stop execution within
// the original deadline; pointer cleanup remains responsible for the real join.
func (tx *objectTransaction) stopRestoreRenewal(ctx context.Context) error {
	if tx.stopRenew == nil {
		return nil
	}
	tx.stopping.Store(true)
	tx.stopRenew()
	select {
	case <-tx.renewDone:
		return context.Cause(ctx)
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Both ownership domains must actually quiesce before the operation reservation
// is returned. The bounded waiter cannot release a late writer or pin early.
func closeProtectedRestore(v *protectedVerification, tx *objectTransaction) (resultErr, writerErr error) {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if tx != nil {
		writerErr = tx.finishPointerWriter(cleanup)
		resultErr = errors.Join(resultErr, writerErr)
	}
	quiet := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	writerQuiet := tx == nil || quiet(tx.pointerQuiesced())
	if writerQuiet && v.done != nil {
		v.done()
	}
	if v.guard != nil {
		resultErr = errors.Join(resultErr, v.guard.Close(cleanup))
	}
	if !writerQuiet || (v.guard != nil && !quiet(v.guard.Quiesced())) {
		go func() {
			if tx != nil {
				<-tx.pointerQuiesced()
			}
			if !writerQuiet && v.done != nil {
				v.done()
			}
			if v.guard != nil {
				<-v.guard.Quiesced()
			}
			_ = v.op.Close()
			v.cancel()
		}()
		return errors.Join(resultErr, errReaderCleanupUnknown), writerErr
	}
	resultErr = errors.Join(resultErr, v.op.Close())
	v.cancel()
	return resultErr, writerErr
}
