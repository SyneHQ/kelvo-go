// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package readerlease

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

const objectRegistryLimit = 64 << 10

type registryObjectStore struct {
	client objectstore.Client
	scope  string
}

// NewObjectStore borrows a trusted exact-key provider client. The client must
// authenticate service time, honor conditional writes and cancellation, and
// allow a response body's Close to interrupt Read. This adapter cannot infer
// those properties from a supplied timestamp or URL. It never closes the client.
//
// Only registry document keys under prefix/tenant are accepted. One adapter may
// serve the reused Registry across datasets/generations in that configured scope.
func NewObjectStore(client objectstore.Client, prefix, tenant string) (Store, error) {
	if nilObjectValue(client) || DefaultConfig(prefix, tenant).validate() != nil {
		return nil, ErrInvalid
	}
	return &registryObjectStore{client: client, scope: prefix + "/" + tenant + "/"}, nil
}

func nilObjectValue(input any) bool {
	if input == nil {
		return true
	}
	value := reflect.ValueOf(input)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (s *registryObjectStore) validKey(key string) bool {
	if len(key) > len(s.scope)+63+len("/reader-leases/")+36 || !strings.HasPrefix(key, s.scope) {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(key, s.scope), "/")
	return len(parts) == 3 && datasetPattern.MatchString(parts[0]) && parts[1] == "reader-leases" &&
		len(parts[2]) == 36 && strings.HasSuffix(parts[2], ".yml") && hexToken(parts[2][:32], 32)
}

// A single wrapping chain may preserve a definite provider result. Joined,
// mixed, custom-Is or cyclic error graphs do not prove a no-write outcome.
func definiteObjectError(err, target error) bool {
	for range 16 {
		if err == target {
			return true
		}
		if _, custom := err.(interface{ Is(error) bool }); custom {
			return false
		}
		wrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = wrapped.Unwrap()
	}
	return false
}

func objectMetadata(info objectstore.Info) (Metadata, error) {
	if info.Size <= 0 || info.Size > objectRegistryLimit || !validVersion(info.Version) || !hexToken(info.SHA256, 64) {
		return Metadata{}, ErrCorrupt
	}
	if info.ServerTime.IsZero() {
		return Metadata{}, ErrClock
	}
	return Metadata{Size: info.Size, Version: info.Version, ServerTime: info.ServerTime}, nil
}

// closeObjectBody joins a cancellation callback as well as the provider Close.
// Even a non-cooperative body cannot outlive the caller's acquisition slot by
// leaving detached cleanup work after this function returns.
func closeObjectBody(ctx context.Context, body io.ReadCloser) func() error {
	var once sync.Once
	var closeErr error
	closeBody := func() { once.Do(func() { closeErr = body.Close() }) }
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(callbackDone)
		closeBody()
	})
	return func() error {
		stopped := stop()
		closeBody()
		if !stopped {
			<-callbackDone
		}
		return closeErr
	}
}

func (s *registryObjectStore) Get(ctx context.Context, key string) (body io.ReadCloser, metadata Metadata, resultErr error) {
	if !s.validKey(key) {
		return nil, Metadata{}, ErrBinding
	}
	if err := context.Cause(ctx); err != nil {
		return nil, Metadata{}, err
	}
	upstream, info, err := s.client.Get(ctx, key, "")
	if nilObjectValue(upstream) {
		upstream = nil
	}
	if upstream != nil {
		closeBody := closeObjectBody(ctx, upstream)
		defer func() {
			closeErr := closeBody()
			if cause := context.Cause(ctx); cause != nil {
				body, metadata, resultErr = nil, Metadata{}, cause
			} else if closeErr != nil {
				body, metadata, resultErr = nil, Metadata{}, ErrUnavailable
			}
		}()
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, Metadata{}, cause
	}
	if err != nil {
		if definiteObjectError(err, objectstore.ErrNotFound) {
			return nil, Metadata{}, ErrNotFound
		}
		return nil, Metadata{}, ErrUnavailable
	}
	if upstream == nil {
		return nil, Metadata{}, ErrCorrupt
	}
	metadata, err = objectMetadata(info)
	if err != nil {
		return nil, Metadata{}, err
	}
	// This is at most one 64 KiB control document. Materialize and verify it
	// before exposing any bytes; query data never travels through this adapter.
	raw, err := io.ReadAll(io.LimitReader(objectContextReader{ctx: ctx, reader: upstream}, info.Size+1))
	if cause := context.Cause(ctx); cause != nil {
		return nil, Metadata{}, cause
	}
	if err != nil {
		return nil, Metadata{}, ErrUnavailable
	}
	if int64(len(raw)) != info.Size {
		return nil, Metadata{}, ErrCorrupt
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != info.SHA256 {
		return nil, Metadata{}, ErrCorrupt
	}
	return &registryObjectBody{ctx: ctx, reader: bytes.NewReader(raw)}, metadata, nil
}

type objectContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r objectContextReader) Read(p []byte) (int, error) {
	if err := context.Cause(r.ctx); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if cause := context.Cause(r.ctx); cause != nil {
		return 0, cause
	}
	if n < 0 || n > len(p) {
		return 0, ErrCorrupt
	}
	return n, err
}

// The provider stream has already closed. The caller owns only bounded memory,
// and cancellation still prevents reads after Get has returned.
type registryObjectBody struct {
	ctx    context.Context
	mu     sync.Mutex
	reader *bytes.Reader
}

func (b *registryObjectBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := context.Cause(b.ctx); err != nil {
		return 0, err
	}
	if b.reader == nil {
		return 0, ErrClosed
	}
	n, err := b.reader.Read(p)
	if cause := context.Cause(b.ctx); cause != nil {
		return 0, cause
	}
	return n, err
}

func (b *registryObjectBody) Close() error {
	b.mu.Lock()
	b.reader = nil
	b.mu.Unlock()
	return context.Cause(b.ctx)
}

func (s *registryObjectStore) CompareAndSwap(ctx context.Context, key, version string, data []byte) (Metadata, error) {
	if !s.validKey(key) {
		return Metadata{}, ErrBinding
	}
	if (version != "" && !validVersion(version)) || len(data) == 0 || len(data) > objectRegistryLimit {
		return Metadata{}, ErrInvalid
	}
	if err := context.Cause(ctx); err != nil {
		return Metadata{}, err
	}
	raw := append([]byte(nil), data...)
	digest := sha256.Sum256(raw)
	checksum := hex.EncodeToString(digest[:])
	condition := objectstore.Condition{Absent: version == "", Version: version}
	info, err := s.client.Put(ctx, key, bytes.NewReader(raw), int64(len(raw)), checksum, condition)
	if cause := context.Cause(ctx); cause != nil {
		return Metadata{}, cause
	}
	if err != nil {
		if definiteObjectError(err, objectstore.ErrConflict) {
			return Metadata{}, ErrConflict
		}
		return Metadata{}, ErrUnavailable
	}
	metadata, err := objectMetadata(info)
	if err != nil {
		return Metadata{}, err
	}
	if info.Size != int64(len(raw)) || info.SHA256 != checksum || (version != "" && info.Version == version) {
		return Metadata{}, ErrCorrupt
	}
	return metadata, nil
}
