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

var ErrRefreshCleanup = errors.New("snapshot refresh cleanup did not complete")

func refreshCleanupError(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrRefreshCleanup, err)
}

// Backend preserves the full-refresh lifecycle independently of where the
// committed bytes live. A query must keep its Lease until execution ends.
// Object backend Close seals writer admission, cancels admitted writers and
// joins their cleanup before closing its reader client. Writer callers must
// stop using staging and call Commit/Abort after cancellation; an uncooperative
// caller or provider can keep Close waiting. This is not proof that all query
// consumers or other backend read operations have quiesced.
type Backend interface {
	Begin(context.Context, string) (RefreshWriter, error)
	Acquire(context.Context, string, string, time.Duration) (*Lease, error)
	Status(context.Context, string) (Snapshot, error)
	Verify(context.Context, string) (Snapshot, error)
	Prune(context.Context, string, int) error
	Close() error
}

// RefreshWriter owns a single refresh. Context also cancels when a remote
// writer loses its fencing lease or its object backend closes. Finish all
// writes before Commit or Abort, including after cancellation. Object backend
// Close waits for that handback instead of closing a caller-borrowed File.
type RefreshWriter interface {
	Context() context.Context
	File() *os.File
	Commit(string, int64) (Snapshot, error)
	Abort() error
}

// OpenBackend keeps the existing POSIX backend as the default. Object reads
// require only the read identity; the write identity is loaded lazily by Begin.
func OpenBackend(config catalog.AccelerationConfig) (Backend, error) {
	if err := config.ValidateVerification(); err != nil {
		return nil, err
	}
	if config.ObjectStorage == nil {
		store, err := OpenStore(config.Directory, config.TenantID)
		if err != nil {
			return nil, err
		}
		return &localBackend{store}, nil
	}
	storage := *config.ObjectStorage
	if storage.ReaderRegistry != nil {
		return nil, errors.New("protected object snapshots require the node object runtime")
	}
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
	if err := config.ValidateVerification(); err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("object snapshot backend requires a client")
	}
	if config.ObjectStorage != nil && config.ObjectStorage.ReaderRegistry != nil {
		return nil, errors.New("protected object snapshots require the node object runtime")
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

// MultipartOptions bounds the number and encoded sizes of local immutable parts.
type MultipartOptions struct {
	MaxParts                    int
	MaxPartBytes, MaxTotalBytes int64
}

// MultipartRefreshWriter owns each NewPart file until SealPart or Abort.
// Finish the Parquet encoder before sealing; zero-row refreshes still seal one
// schema-only part. Commit publishes the complete generation atomically.
type MultipartRefreshWriter interface {
	Context() context.Context
	NewPart() (*os.File, error)
	SealPart(rows int64) error
	Commit(fingerprint string) (Snapshot, error)
	Abort() error
	SchemaWriter
}

// MultipartBackend extends the single-file contract with complete-generation
// publication. Both local and object backends implement its bounded parts.
type MultipartBackend interface {
	BeginMultipart(context.Context, string, MultipartOptions) (MultipartRefreshWriter, error)
}

var _ MultipartBackend = (*localBackend)(nil)
