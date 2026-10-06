// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	api "github.com/SYNEHQ/kelvo-go/operations"
)

func (s *Store) identity(scope Scope, key string) (string, int) {
	raw, _ := json.Marshal(struct {
		Scope Scope  `json:"scope"`
		Key   string `json:"key"`
	}{scope, key})
	sum := sha256.Sum256(append([]byte("kelvo.operation.identity.v1\x00"), raw...))
	shard := int(uint32(sum[0])<<24|uint32(sum[1])<<16|uint32(sum[2])<<8|uint32(sum[3])) % s.policy.Shards
	return hex.EncodeToString(sum[:]), shard
}

// Lookup reads the original identity without advancing state, extending
// retention or fetching input bytes. Unlike Get, it performs no expiry writes.
// ErrNotFound includes expired retention and is never evidence of nonexecution.
func (s *Store) Lookup(parent context.Context, scope Scope, key, requestSHA256 string) (Snapshot, error) {
	if s == nil || parent == nil || !s.validScope(scope) || !safeText(key, 128) || !api.ValidDigest(requestSHA256) {
		return Snapshot{}, ErrInvalid
	}
	identity, shard := s.identity(scope, key)
	ctx, cancel := context.WithTimeout(parent, s.policy.StorageTimeout)
	defer cancel()
	if ctx.Err() != nil {
		return Snapshot{}, ErrUnavailable
	}
	entry, err := s.backend.Get(ctx, s.key(shard))
	if errors.Is(err, ErrMissing) {
		return Snapshot{}, ErrNotFound
	}
	if err != nil || ctx.Err() != nil {
		return Snapshot{}, ErrUnavailable
	}
	var doc document
	if entry.Revision == 0 || api.DecodeStrict(entry.Value, &doc, MaxDocumentBytes) != nil || s.validateDocument(doc, shard) != nil {
		return Snapshot{}, ErrUnavailable
	}
	for i := range doc.Records {
		record := &doc.Records[i]
		if record.IdentitySHA256 != identity {
			continue
		}
		if record.Scope != scope {
			return Snapshot{}, ErrNotFound
		}
		if record.Terminal() && !s.now().UTC().Before(record.RetainUntil) {
			return Snapshot{}, ErrNotFound
		}
		if record.RequestSHA256 != requestSHA256 {
			return Snapshot{}, ErrConflict
		}
		return Snapshot{Record: cloneRecord(record), Revision: entry.Revision}, nil
	}
	return Snapshot{}, ErrNotFound
}
