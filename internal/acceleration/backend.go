// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

// Backend preserves the full-refresh lifecycle independently of where the
// committed bytes live. A query must keep its Lease until execution ends.
type Backend interface {
	Begin(context.Context, string) (RefreshWriter, error)
	Acquire(context.Context, string, string, time.Duration) (*Lease, error)
	Status(context.Context, string) (Snapshot, error)
	Verify(context.Context, string) (Snapshot, error)
	Prune(context.Context, string, int) error
	Close() error
}

// RefreshWriter owns a single refresh. Context also cancels when a remote
// writer loses its fencing lease. Finish all writes before Commit or Abort.
type RefreshWriter interface {
	Context() context.Context
	File() *os.File
	Commit(string, int64) (Snapshot, error)
	Abort() error
}

// OpenBackend keeps the existing POSIX backend as the default. Object reads
// require only the read identity; the write identity is loaded lazily by Begin.
func OpenBackend(config catalog.AccelerationConfig) (Backend, error) {
	if config.ObjectStorage == nil {
		store, err := OpenStore(config.Directory, config.TenantID)
		if err != nil {
			return nil, err
		}
		return &localBackend{store}, nil
	}
	storage := *config.ObjectStorage
	reader, err := objectstore.New(storage.ObjectLocation, storage.ReadCredentials)
	if err != nil {
		return nil, err
	}
	backend, err := newObjectBackend(config, reader)
	if err != nil {
		reader.Close()
		return nil, err
	}
	backend.writeClient = func() (objectstore.Client, bool, error) {
		client, err := objectstore.New(storage.ObjectLocation, storage.WriteCredentials)
		return client, true, err
	}
	return backend, nil
}

// NewObjectBackend injects a client for protocol tests or embedding. On success
// the backend owns the client; it shares that client for reads and writes.
func NewObjectBackend(config catalog.AccelerationConfig, client objectstore.Client) (Backend, error) {
	if client == nil {
		return nil, errors.New("object snapshot backend requires a client")
	}
	return newObjectBackend(config, client)
}

type localBackend struct{ *Store }

func (backend *localBackend) Begin(ctx context.Context, dataset string) (RefreshWriter, error) {
	return backend.Store.Begin(ctx, dataset)
}

func (backend *localBackend) Status(ctx context.Context, dataset string) (Snapshot, error) {
	lease, err := backend.Store.acquire(ctx, dataset, "", 0, false)
	if err != nil {
		return Snapshot{}, err
	}
	return lease.Snapshot, lease.Close()
}

func (*localBackend) Close() error { return nil }

var _ Backend = (*localBackend)(nil)
var _ RefreshWriter = (*Transaction)(nil)
