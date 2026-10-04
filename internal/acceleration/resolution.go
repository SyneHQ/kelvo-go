// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

// ProtectedObjects reports whether the catalog requires durable object pins.
// Legacy catalogs never acquire deletion authority through this flag.
func ProtectedObjects(c catalog.Config) bool {
	return c.Acceleration != nil && c.Acceleration.ObjectStorage != nil && c.Acceleration.ObjectStorage.ReaderRegistry != nil
}

// Resolution owns snapshot reads until all range, process and scratch consumers
// have joined. Protected Close retains one charged finalizer after a timeout.
// Legacy Close stays synchronous because it has no runtime reservation to retain.
// It must not be copied. Sources contain capabilities, never cloud credentials.
type Resolution struct {
	Sources  []catalog.Source
	Versions []query.AccelerationVersion

	ctx        context.Context
	guard      *ReadGuard
	operation  *objectRuntimeOperation
	release    func()
	rangeToken func()
	once       sync.Once
	mu         sync.Mutex
	done       chan struct{}
	err        error
	uncertain  bool
}

func (r *Resolution) Context() context.Context { return r.ctx }

func (r *Resolution) Check() error {
	if r.guard != nil {
		return r.guard.Check()
	}
	return context.Cause(r.ctx)
}

// HoldConsumer transfers one lifetime token to a trusted cleanup owner. The
// token must be returned only after that owner's work is known to have stopped.
func (r *Resolution) HoldConsumer() (func(), error) {
	if r.guard != nil {
		return r.guard.HoldConsumer()
	}
	if err := r.Check(); err != nil {
		return nil, err
	}
	return func() {}, nil
}

func (r *Resolution) Close(ctx context.Context) error {
	if !validReaderCleanup(ctx) {
		return errReaderInvalid
	}
	r.once.Do(func() {
		if r.guard == nil && r.operation == nil {
			// Preserve legacy custody: returning early would release the worker
			// reservation while an uncharged provider finalizer was still alive.
			r.finish()
			return
		}
		if r.guard != nil {
			// Seal admission and stop execution before joining potentially stalled
			// range callbacks. The guard keeps renewing while consumers remain.
			r.guard.startClose(nil)
		}
		go r.finish()
	})
	select {
	case <-r.done:
	case <-ctx.Done():
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.done:
	default:
		r.uncertain = true
	}
	if r.uncertain {
		return errors.Join(r.err, errReaderCleanupUnknown)
	}
	return r.err
}

func (r *Resolution) finish() {
	if r.release != nil {
		r.release()
	}
	if r.rangeToken != nil {
		r.rangeToken()
	}
	var result error
	if r.guard != nil {
		// This is the single charged finalizer, not a goroutine per Close call.
		// Uncertain process/scratch ownership deliberately keeps it waiting.
		<-r.guard.Quiesced()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		result = r.guard.Close(ctx)
		cancel()
	}
	if r.operation != nil {
		result = errors.Join(result, r.operation.Close())
	}
	r.mu.Lock()
	r.err = result
	close(r.done)
	r.mu.Unlock()
}

// ResolveWithRuntime selects every requested root once, acquires all exact pins,
// then permits generation metadata and payload access. The runtime is immutable
// and shared by a node's interactive, export, refresh and diagnostic consumers.
func ResolveWithRuntime(ctx context.Context, c catalog.Config, request query.Request, runtime *ObjectRuntime) (*Resolution, error) {
	if ctx == nil {
		return nil, errReaderInvalid
	}
	if !ProtectedObjects(c) {
		sources, versions, release, err := Resolve(ctx, c, request)
		if err != nil {
			return nil, err
		}
		return &Resolution{Sources: sources, Versions: versions, ctx: ctx, release: release, done: make(chan struct{})}, nil
	}
	if runtime == nil || runtime.Match(c) != nil {
		return nil, query.NewError("CONFIGURATION_ERROR", "Protected snapshots require the matching node object runtime")
	}
	ids := request.Sources
	if request.Mode == "native" {
		ids = []string{request.ConnectionID}
	}
	sources, err := c.Select(ids)
	if err != nil {
		return nil, query.NewError("INVALID_ARGUMENT", "Unknown or duplicate source")
	}
	resolution := &Resolution{Sources: sources, ctx: ctx, done: make(chan struct{})}
	fail := func(cause error) (*Resolution, error) {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return nil, errors.Join(cause, resolution.Close(cleanup))
	}
	var selected []selectedObjectSnapshot
	var bindings []readerlease.Binding
	for _, source := range sources {
		if source.Type != "accelerated" {
			continue
		}
		if request.Mode == "native" {
			return fail(query.NewError("INVALID_ARGUMENT", "Accelerated datasets require federated mode"))
		}
		if resolution.operation == nil {
			resolution.operation, err = runtime.beginOperation(ctx)
			if err != nil {
				return fail(err)
			}
			resolution.ctx = resolution.operation.Context()
		}
		dataset, ok := c.Dataset(source.ID)
		fingerprint, err := c.DatasetFingerprint(source.ID)
		if !ok || err != nil {
			return fail(query.NewError("DATASET_UNAVAILABLE", "Accelerated dataset configuration is unavailable"))
		}
		root, err := runtime.backend.selectObject(resolution.ctx, source.ID, fingerprint, dataset.MaxAge, true)
		if err != nil {
			return fail(err)
		}
		selected = append(selected, root)
		bindings = append(bindings, root.binding)
	}
	if len(selected) == 0 {
		return resolution, nil
	}
	resolution.guard, err = runtime.acquire(resolution.ctx, bindings)
	if err != nil {
		return fail(err)
	}
	resolution.ctx = resolution.guard.Context()
	prepared, err := resolution.guard.HoldConsumer()
	if err != nil {
		return fail(err)
	}
	// The preparation token spans descriptors, version checks and capability
	// setup. It is distinct from all range handlers, child tree and scratch.
	var snapshots []Snapshot
	for _, root := range selected {
		if err = resolution.Check(); err != nil {
			prepared()
			return fail(err)
		}
		snapshot, loadErr := runtime.backend.loadSelectedObject(resolution.ctx, root)
		if loadErr != nil {
			prepared()
			return fail(loadErr)
		}
		snapshots = append(snapshots, snapshot)
		resolution.Versions = append(resolution.Versions, query.AccelerationVersion{Dataset: snapshot.Dataset, Generation: snapshot.Generation, RefreshedAt: snapshot.RefreshedAt})
	}
	resolution.rangeToken, err = resolution.guard.HoldConsumer()
	if err != nil {
		prepared()
		return fail(err)
	}
	ranges, release, err := openProtectedObjectRanges(resolution.ctx, *c.Acceleration.ObjectStorage, snapshots, runtime.dataClient(), resolution.Check)
	resolution.release = release
	if err != nil {
		prepared()
		return fail(err)
	}
	for _, snapshot := range snapshots {
		dataset, configured := c.Dataset(snapshot.Dataset)
		source, exists := ranges[snapshot.Dataset]
		if !configured || !exists {
			prepared()
			return fail(query.NewError("DATASET_UNAVAILABLE", "Object snapshot provenance is unavailable"))
		}
		source, err = withObjectSnapshotProvenance(source, snapshot, dataset)
		if err != nil {
			prepared()
			return fail(err)
		}
		for i := range resolution.Sources {
			if resolution.Sources[i].ID == snapshot.Dataset {
				resolution.Sources[i] = source
			}
		}
	}
	prepared()
	if err := resolution.Check(); err != nil {
		return fail(err)
	}
	return resolution, nil
}
