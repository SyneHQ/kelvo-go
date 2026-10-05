// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"io"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

// verificationMetadata shares the data reader's accounting and error ledger,
// but borrows a separate metadata identity. A writer only promises Client, so
// this view deliberately does not advertise range support or forward writes.
type verificationMetadata struct {
	client objectstore.Client
	meter  *verificationReader
}

func (r *verificationReader) metadataClient(client objectstore.Client) objectstore.Client {
	if nilReaderDependency(client) {
		r.mu.Lock()
		r.failLocked(errReaderInvalid)
		r.mu.Unlock()
	}
	return &verificationMetadata{client: client, meter: r}
}

func (r *verificationMetadata) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	if err := r.meter.check(ctx); err != nil {
		return nil, objectstore.Info{}, err
	}
	if err := r.meter.Preflight(1); err != nil {
		return nil, objectstore.Info{}, err
	}
	body, info, err := r.client.Get(ctx, key, version)
	return r.meter.returned(ctx, body, info, err)
}

func (r *verificationMetadata) Head(ctx context.Context, key, version string) (objectstore.Info, error) {
	if err := r.meter.check(ctx); err != nil {
		return objectstore.Info{}, err
	}
	info, err := r.client.Head(ctx, key, version)
	r.meter.mu.Lock()
	r.meter.failLocked(err)
	r.meter.failLocked(context.Cause(ctx))
	resultErr := r.meter.errorLocked()
	r.meter.mu.Unlock()
	return info, resultErr
}

func (r *verificationMetadata) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
	return r.meter.Put(ctx, key, body, size, digest, condition)
}

func (*verificationMetadata) Close() {}

var _ objectstore.Client = (*verificationMetadata)(nil)
