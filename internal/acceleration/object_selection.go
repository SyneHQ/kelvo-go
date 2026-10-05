// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

// Selection contains no capability or descriptor bytes. Root selection and
// policy validation happen once; the caller acquires all selected bindings
// before loadSelectedObject can perform generation I/O.
type selectedObjectSnapshot struct {
	backend     *objectBackend
	dataset     string
	committed   *objectCommitted
	binding     readerlease.Binding
	reference   time.Time
	observation Snapshot
	fingerprint string
	maxAge      time.Duration
	checkPolicy bool
}

func (backend *objectBackend) selectObject(ctx context.Context, dataset, fingerprint string, maxAge time.Duration, checkPolicy bool) (selectedObjectSnapshot, error) {
	if !backend.protected() || backend.runtime == nil {
		return selectedObjectSnapshot{}, ErrProtectionRequired
	}
	if err := backend.check(ctx, dataset); err != nil {
		return selectedObjectSnapshot{}, err
	}
	state, err := backend.readState(ctx, dataset, backend.reader)
	if err != nil {
		return selectedObjectSnapshot{}, err
	}
	return backend.selectObjectState(ctx, dataset, state, fingerprint, maxAge, checkPolicy)
}

func (backend *objectBackend) selectObjectState(ctx context.Context, dataset string, state objectState, fingerprint string, maxAge time.Duration, checkPolicy bool) (selectedObjectSnapshot, error) {
	return backend.selectObjectStateEntry(ctx, dataset, state, 0, fingerprint, maxAge, checkPolicy)
}

// Index zero is the current commit; retained entries can only be selected by
// their position in the same validated root. No caller-supplied key is adopted.
func (backend *objectBackend) selectObjectStateEntry(ctx context.Context, dataset string, state objectState, index int, fingerprint string, maxAge time.Duration, checkPolicy bool) (selectedObjectSnapshot, error) {
	selected := selectedObjectSnapshot{backend: backend, dataset: dataset,
		fingerprint: fingerprint, maxAge: maxAge, checkPolicy: checkPolicy}
	if maxAge < 0 {
		return selected, errors.New("acceleration maximum age cannot be negative")
	}
	if !backend.protected() || backend.runtime == nil || state.manifest.Version != protectedManifestVersion {
		return selected, ErrProtectionRequired
	}
	if err := validateObjectManifest(state.manifest, dataset); err != nil {
		return selected, err
	}
	if index < 0 || index > len(state.manifest.History) {
		return selected, ErrCorrupt
	}
	committed := state.manifest.Committed
	if index > 0 {
		committed = state.manifest.History[index-1]
	}
	selected.committed = cloneObjectCommit(committed)
	if selected.committed == nil {
		return selected, ErrNotFound
	}
	committed = selected.committed
	selected.reference = state.now()
	selected.observation = Snapshot{Fingerprint: committed.Fingerprint, RefreshedAt: committed.RefreshedAt}.observeClock(selected.reference)
	if err := selected.check(ctx); err != nil {
		return selected, err
	}
	if committed.ReaderBinding == nil || committed.ReaderBinding.Tenant != backend.config.TenantID || committed.ReaderBinding.Dataset != dataset {
		return selected, ErrProtectionRequired
	}
	binding, err := objectReaderBinding(committed.ReaderBinding.Reference, backend.config.ObjectStorage.ObjectLocation, committed)
	if err != nil {
		return selected, err
	}
	if binding != *committed.ReaderBinding {
		return selected, ErrCorrupt
	}
	selected.binding = binding
	return selected, nil
}

func (selected selectedObjectSnapshot) check(ctx context.Context) error {
	if selected.backend == nil || selected.committed == nil {
		return ErrProtectionRequired
	}
	if err := selected.backend.check(ctx, selected.dataset); err != nil {
		return err
	}
	if selected.checkPolicy {
		if selected.observation.Fingerprint != selected.fingerprint {
			return ErrFingerprintMismatch
		}
		if selected.maxAge > 0 && selected.observation.Age() > selected.maxAge {
			return ErrStale
		}
	}
	return nil
}

func (backend *objectBackend) loadSelectedObject(ctx context.Context, selected selectedObjectSnapshot) (Snapshot, error) {
	snapshot, err := backend.loadSelectedObjectWithReader(ctx, selected, backend.reader)
	if err != nil {
		return Snapshot{}, err
	}
	if err := backend.headObjectSnapshot(ctx, snapshot); err != nil {
		return Snapshot{}, err
	}
	if err := selected.check(ctx); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

// Maintenance validates exact Get/range responses after loading. It avoids the
// query path's parallel HEAD cancellation, which could turn content corruption
// into a sticky context failure in the operation-wide verification meter.
func (backend *objectBackend) loadSelectedObjectWithReader(ctx context.Context, selected selectedObjectSnapshot, client objectstore.Client) (Snapshot, error) {
	if selected.backend != backend || selected.committed == nil || selected.committed.ReaderBinding == nil || selected.binding != *selected.committed.ReaderBinding {
		return Snapshot{}, ErrProtectionRequired
	}
	if err := selected.check(ctx); err != nil {
		return Snapshot{}, err
	}
	snapshot, err := backend.loadObjectSnapshot(ctx, selected.dataset, selected.committed, selected.reference, client)
	if err != nil {
		return Snapshot{}, err
	}
	// Preserve the original service-clock anchor, including elapsed descriptor I/O.
	snapshot.ageObserved, snapshot.ageObservedAt = selected.observation.ageObserved, selected.observation.ageObservedAt
	if err := selected.check(ctx); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

// The operation slot covers pre-pin metadata and all synchronous provider/body
// work. The guard's preparation token prevents pin release until it has joined.
func (backend *objectBackend) readProtectedSelection(ctx context.Context, selected selectedObjectSnapshot, consume func(context.Context, Snapshot) error) (snapshot Snapshot, resultErr error) {
	guard, err := backend.runtime.acquire(ctx, []readerlease.Binding{selected.binding})
	if guard != nil {
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, guard.Close(cleanup))
			if resultErr != nil {
				snapshot = Snapshot{}
			}
		}()
	}
	if err != nil {
		return Snapshot{}, err
	}
	if guard == nil {
		return Snapshot{}, ErrProtectionRequired
	}
	done, err := guard.HoldConsumer()
	if err != nil {
		return Snapshot{}, err
	}
	defer done()
	snapshot, err = backend.loadSelectedObject(guard.Context(), selected)
	if err != nil {
		return Snapshot{}, err
	}
	if consume != nil {
		if err := consume(guard.Context(), snapshot); err != nil {
			return Snapshot{}, err
		}
	}
	if err := selected.check(guard.Context()); err != nil {
		return Snapshot{}, err
	}
	return snapshot, guard.Check()
}

func (backend *objectBackend) protectedCurrentSnapshot(ctx context.Context, dataset, fingerprint string, maxAge time.Duration, checkPolicy bool) (snapshot Snapshot, resultErr error) {
	if backend.runtime == nil {
		return Snapshot{}, ErrProtectionRequired
	}
	op, err := backend.runtime.beginOperation(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, op.Close()) }()
	selected, err := backend.selectObject(op.Context(), dataset, fingerprint, maxAge, checkPolicy)
	if err != nil {
		return Snapshot{}, err
	}
	return backend.readProtectedSelection(op.Context(), selected, nil)
}
