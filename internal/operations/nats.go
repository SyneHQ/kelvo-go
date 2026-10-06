// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"context"
	"errors"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

type natsBackend struct{ kv jetstream.KeyValue }

// OpenNATS borrows a tenant-authenticated connection; it never changes accounts
// or closes the connection. maxPayload must be read from that connection's
// authenticated server INFO, not a caller-selected request or larger guess.
// Initialization creates only absent resources and disables direct KV reads.
// Existing retention/storage/replication limits are never silently migrated.
func OpenNATS(parent context.Context, js jetstream.JetStream, p Policy, replicas int, maxPayload int64, initialize bool) (*Store, error) {
	if js == nil || p.Validate() != nil || (replicas != 1 && replicas != 3 && replicas != 5) || maxPayload <= MaxDocumentBytes+(64<<10) {
		return nil, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(parent, p.StorageTimeout)
	defer cancel()
	bucket := "KELVO_OPERATIONS_" + strings.ToUpper(p.Namespace)
	want := jetstream.KeyValueConfig{Bucket: bucket, History: 1, TTL: 0, Storage: jetstream.FileStorage, Replicas: replicas, MaxValueSize: MaxDocumentBytes, MaxBytes: int64(p.Shards) * (MaxDocumentBytes + 4096)}
	kv, err := js.KeyValue(ctx, bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) && initialize {
		kv, err = js.CreateKeyValue(ctx, want)
		if err != nil {
			kv, err = js.KeyValue(ctx, bucket)
		}
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	stream, err := js.Stream(ctx, "KV_"+bucket)
	if err != nil {
		return nil, ErrUnavailable
	}
	info := stream.CachedInfo()
	if info == nil {
		return nil, ErrUnavailable
	}
	if info.Config.AllowDirect {
		if !initialize {
			return nil, ErrInvalid
		}
		cfg := info.Config
		cfg.AllowDirect = false
		if _, err = js.UpdateStream(ctx, cfg); err != nil {
			return nil, ErrUnavailable
		}
		stream, err = js.Stream(ctx, "KV_"+bucket)
		if err != nil {
			return nil, ErrUnavailable
		}
		info = stream.CachedInfo()
		if info == nil {
			return nil, ErrUnavailable
		}
	}
	if info.Config.AllowDirect || info.Config.MaxAge != 0 || info.Config.Storage != jetstream.FileStorage || info.Config.Replicas != replicas || info.Config.Retention != jetstream.LimitsPolicy || info.Config.Discard != jetstream.DiscardNew || info.Config.MaxMsgsPerSubject != 1 {
		return nil, ErrInvalid
	}
	kv, err = js.KeyValue(ctx, bucket)
	if err != nil {
		return nil, ErrUnavailable
	}
	status, err := kv.Status(ctx)
	if err != nil {
		return nil, ErrUnavailable
	}
	got := status.Config()
	if got.History != want.History || got.TTL != want.TTL || got.Storage != want.Storage || got.Replicas != want.Replicas || got.MaxValueSize != want.MaxValueSize || got.MaxBytes != want.MaxBytes {
		return nil, ErrInvalid
	}
	return New(&natsBackend{kv: kv}, p)
}

func (b *natsBackend) Get(ctx context.Context, key string) (Entry, error) {
	value, err := b.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		return Entry{}, ErrMissing
	}
	if err != nil {
		return Entry{}, ErrUnavailable
	}
	return Entry{Value: append([]byte(nil), value.Value()...), Revision: value.Revision()}, nil
}
func (b *natsBackend) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	rev, err := b.kv.Create(ctx, key, value)
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return 0, ErrRevision
	}
	if err != nil {
		return 0, ErrUnavailable
	}
	return rev, nil
}
func (b *natsBackend) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	rev, err := b.kv.Update(ctx, key, value, revision)
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return 0, ErrRevision
	}
	if err != nil {
		return 0, ErrUnavailable
	}
	return rev, nil
}
