// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

var (
	ErrNotFound = errors.New("snapshot object not found")
	ErrConflict = errors.New("snapshot object precondition failed")
)

// Keep one-shot uploads below every supported provider's PutObject/PutBlob
// limit. Larger datasets need partitioned generations or multipart publication.
const MaxUploadBytes int64 = 4 << 30

// Version is an opaque conditional-write token: ETag for S3/R2/Azure and the
// object generation for GCS. ETags must never be treated as content digests.
type Info struct {
	Size       int64
	Version    string
	SHA256     string
	ServerTime time.Time
}

type Condition struct {
	Absent  bool
	Version string
}

// Client implements only exact-key operations. Put must honor its precondition
// atomically or fail. It borrows the upload reader until returning, including
// all transport body reads and closure, and never closes the caller's reader.
// No operation lists or deletes snapshots.
type Client interface {
	Get(context.Context, string, string) (io.ReadCloser, Info, error)
	Head(context.Context, string, string) (Info, error)
	Put(context.Context, string, io.ReadSeeker, int64, string, Condition) (Info, error)
	Close()
}

// RangeClient returns one exact bounded byte range. Info.Size is the complete
// object's size (from Content-Range), not the response payload length. The
// implementation must reject redirects, ignored ranges and changed versions.
type RangeClient interface {
	Client
	GetRange(context.Context, string, string, int64, int64) (io.ReadCloser, Info, error)
}

func New(location catalog.ObjectLocation, credentials catalog.ObjectCredentials) (Client, error) {
	if err := location.Validate(); err != nil {
		return nil, err
	}
	if err := credentials.Validate(location.Provider); err != nil {
		return nil, err
	}
	if location.Provider == "azure" {
		return newAzure(location, credentials)
	}
	return newS3(location, credentials)
}
