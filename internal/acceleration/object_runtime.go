// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

const objectRuntimeOperationLimit = 64

// ObjectRuntime owns one immutable protected namespace for a node. Managers and
// executor copies borrow it. Endpoint, trust policy and credential changes need
// a node restart; opening replacement pools while cleanup is uncertain is not
// supported. Close retains one finalizer when its caller's deadline expires.
type ObjectRuntime struct {
	config                           catalog.Config
	authority                        string
	backend                          *objectBackend
	registry                         *readerlease.Registry
	owner                            *ReaderOwner
	budget                           *ReaderBudget
	resources                        *objectRuntimeResources
	dataTransport, registryTransport *objectstore.SharedTransport
	ctx                              context.Context
	cancel                           context.CancelCauseFunc
	mu                               sync.Mutex
	closed                           bool
	operations                       int
	operationWork                    sync.WaitGroup
	closeOnce                        sync.Once
	quiet                            chan struct{}
	outcome                          readerOutcome
}

type objectClientFactory func(catalog.ObjectLocation, catalog.ObjectCredentials, *objectstore.SharedTransport) (objectstore.Client, error)

// OpenObjectRuntime performs no provider requests. Unprotected catalogs return
// nil, nil. Once construction starts, a non-nil result remains owned even when
// err is non-nil: retain it and call bounded Close before retrying node startup.
func OpenObjectRuntime(c catalog.Config) (*ObjectRuntime, error) {
	return openObjectRuntime(c, objectstore.NewWithTransport)
}

func openObjectRuntime(c catalog.Config, factory objectClientFactory) (*ObjectRuntime, error) {
	if !ProtectedObjects(c) {
		return nil, nil
	}
	if factory == nil {
		return nil, errReaderInvalid
	}
	config, authority, err := catalog.AuthoritySnapshot(c)
	if err != nil {
		return nil, err
	}
	a := config.Acceleration
	storage := a.ObjectStorage
	if err := storage.Validate(); err != nil {
		return nil, err
	}
	spec := readerOwnerSpec{tenant: a.TenantID, directory: a.Directory, location: storage.ObjectLocation,
		readCredentials: storage.ReadCredentials, registryCredentials: storage.ReaderRegistry.Credentials,
		registry: readerlease.DefaultConfig(storage.Prefix, a.TenantID)}
	for _, dataset := range a.Datasets {
		fingerprint, err := config.DatasetFingerprint(dataset.ID)
		if err != nil {
			return nil, err
		}
		scan, err := dataset.EffectiveSnapshotScanLimits()
		if err != nil {
			return nil, err
		}
		spec.datasets = append(spec.datasets, readerDatasetPolicy{id: dataset.ID, fingerprint: fingerprint, maxAge: dataset.MaxAge, scan: scan})
	}
	if storage.Provider == "azure" {
		if err := catalog.ValidateAzureReadSAS(os.Getenv(storage.ReadCredentials.SASTokenEnv)); err != nil {
			return nil, err
		}
	}
	budget := newReaderBudget()
	owner, err := budget.newOwner(spec) // Reserve before any resource construction.
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	r := &ObjectRuntime{config: config, authority: authority, owner: owner, budget: budget, ctx: ctx,
		cancel: cancel, quiet: make(chan struct{}), resources: &objectRuntimeResources{ready: make(chan struct{}), quiet: make(chan struct{})}}
	err = owner.open(func(opening context.Context) (readerResources, error) {
		resources := r.resources
		var err error
		r.dataTransport, err = objectstore.NewSharedTransport(storage.ObjectLocation)
		if err != nil {
			return resources, err
		}
		r.registryTransport, err = objectstore.NewSharedTransport(storage.ObjectLocation)
		if err != nil {
			return resources, err
		}
		for i, credentials := range []catalog.ObjectCredentials{storage.ReadCredentials, storage.WriteCredentials, storage.ReaderRegistry.Credentials} {
			if err := context.Cause(opening); err != nil {
				return resources, err
			}
			transport := r.dataTransport
			if i == 2 {
				transport = r.registryTransport
			}
			client, err := factory(storage.ObjectLocation, credentials, transport)
			if !nilReaderDependency(client) {
				resources.clients[i] = client
			}
			if err != nil {
				return resources, err
			}
			if nilReaderDependency(client) {
				return resources, errReaderInvalid
			}
		}
		resources.data, _ = resources.clients[0].(objectstore.RangeClient)
		if nilReaderDependency(resources.data) {
			return resources, errors.New("protected object reader requires exact range support")
		}
		store, err := readerlease.NewObjectStore(resources.clients[2], storage.Prefix, a.TenantID)
		if err != nil {
			return resources, err
		}
		r.registry, err = readerlease.New(store, spec.registry)
		if err != nil {
			return resources, err
		}
		resources.registry = r.registry
		r.backend, err = newObjectBackend(*a, resources.clients[0])
		if err != nil {
			return resources, err
		}
		r.backend.runtime = r
		r.backend.writeClient = func() (objectstore.Client, bool, error) { return resources.clients[1], false, nil }
		return resources, nil
	})
	if err != nil {
		r.mu.Lock()
		r.outcome.fail(err)
		r.mu.Unlock()
		r.startClose()
		return r, err
	}
	return r, nil
}

// Match binds the complete catalog, including credential references, to this
// runtime. It does not re-resolve secrets or accept a mutable replacement.
func (r *ObjectRuntime) Match(c catalog.Config) error {
	if r == nil {
		return errReaderInvalid
	}
	_, fingerprint, err := catalog.AuthoritySnapshot(c)
	if err != nil || fingerprint != r.authority {
		return errReaderInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errReaderClosed
	}
	return nil
}

func (r *ObjectRuntime) backendFor(c catalog.Config) (Backend, error) {
	if err := r.Match(c); err != nil {
		return nil, err
	}
	return &borrowedObjectBackend{r.backend}, nil
}

// Embedding retains MultipartBackend and schema/maintenance interfaces.
type borrowedObjectBackend struct{ *objectBackend }

func (*borrowedObjectBackend) Close() error { return nil }

func (r *ObjectRuntime) Status(ctx context.Context, dataset string) (Snapshot, error) {
	if r == nil || r.backend == nil {
		return Snapshot{}, errReaderInvalid
	}
	return r.backend.Status(ctx, dataset)
}

func (r *ObjectRuntime) dataClient() objectstore.RangeClient { return r.resources.data }

func (r *ObjectRuntime) acquire(ctx context.Context, bindings []readerlease.Binding) (*ReadGuard, error) {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return nil, errReaderClosed
	}
	guard, err := r.owner.begin(ctx, bindings)
	if err != nil {
		return nil, err
	}
	return guard, guard.acquire()
}

type objectRuntimeOperation struct {
	runtime *ObjectRuntime
	ctx     context.Context
	cancel  context.CancelCauseFunc
	watch   readerCallback
	once    sync.Once
	err     error
}

func (r *ObjectRuntime) beginOperation(ctx context.Context) (*objectRuntimeOperation, error) {
	if ctx == nil {
		return nil, errReaderInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errReaderClosed
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if r.operations == objectRuntimeOperationLimit {
		return nil, errReaderCapacity
	}
	child, cancel := context.WithCancelCause(ctx)
	op := &objectRuntimeOperation{runtime: r, ctx: child, cancel: cancel}
	r.operations++
	r.operationWork.Add(1)
	op.watch = readerAfter(r.ctx, func() { cancel(context.Cause(r.ctx)) })
	return op, nil
}

func (o *objectRuntimeOperation) Context() context.Context { return o.ctx }

// Close is called only after this operation's methods, bodies and consumers
// have joined. It does not create a timeout waiter or infer cleanup from cancel.
func (o *objectRuntimeOperation) Close() error {
	o.once.Do(func() {
		o.err = context.Cause(o.ctx)
		o.watch.join()
		o.runtime.mu.Lock()
		if o.runtime.closed {
			o.err = errors.Join(o.err, errReaderOwnerClosed)
		}
		o.runtime.operations--
		o.runtime.mu.Unlock()
		o.cancel(errReaderClosed)
		o.runtime.operationWork.Done()
	})
	return o.err
}

func (r *ObjectRuntime) startClose() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		if r.backend != nil {
			r.backend.closed.Store(true)
		}
		r.mu.Unlock()
		r.cancel(errReaderOwnerClosed)
		r.owner.startClose()
		go r.finish()
	})
}

func (r *ObjectRuntime) finish() {
	var result error
	if r.backend != nil {
		result = r.backend.drainWriters()
	}
	r.operationWork.Wait()
	close(r.resources.ready) // Registry remains available until writers/operations hand back.
	<-r.owner.Quiesced()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	result = errors.Join(result, r.owner.Close(ctx))
	cancel()
	// Signal both pools without letting a stalled data dial delay registry
	// cancellation. These two fixed tasks stay owned by this one finalizer.
	var transports sync.WaitGroup
	for _, transport := range [2]*objectstore.SharedTransport{r.dataTransport, r.registryTransport} {
		if transport != nil {
			transports.Add(1)
			go func(transport *objectstore.SharedTransport) {
				defer transports.Done()
				transport.Close()
			}(transport)
		}
	}
	transports.Wait()
	r.mu.Lock()
	r.outcome.cleanupFailed(result)
	close(r.quiet)
	r.mu.Unlock()
}

func (r *ObjectRuntime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if !validReaderCleanup(ctx) {
		return errReaderInvalid
	}
	r.startClose()
	select {
	case <-r.quiet:
	case <-ctx.Done():
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.quiet:
	default:
		r.outcome.unknown = true
	}
	return r.outcome.err()
}

// Quiesced reports owned resource handback. It never clears the sticky outcome
// returned by Close after an earlier failure or uncertain cleanup.
func (r *ObjectRuntime) Quiesced() <-chan struct{} { return r.quiet }

type objectRuntimeResources struct {
	clients      [3]objectstore.Client
	data         objectstore.RangeClient
	registry     *readerlease.Registry
	ready, quiet chan struct{}
	once         sync.Once
}

func (r *objectRuntimeResources) AcquireWithLifetime(ctx, custody context.Context, binding readerlease.Binding) (readerPin, error) {
	lease, err := r.registry.AcquireWithLifetime(ctx, custody, binding)
	if lease == nil {
		return nil, err
	}
	return lease, err
}

func (r *objectRuntimeResources) Close() error {
	r.once.Do(func() {
		<-r.ready
		for _, client := range r.clients {
			if !nilReaderDependency(client) {
				client.Close()
			}
		}
		close(r.quiet)
	})
	return nil
}

func (r *objectRuntimeResources) Quiesced() <-chan struct{} { return r.quiet }

var _ MultipartBackend = (*borrowedObjectBackend)(nil)
