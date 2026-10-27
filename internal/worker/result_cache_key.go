// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash"
	"slices"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// cacheKey is everything a cached result is bound to: who asked (tenant, app
// team, principal), under which effective authorization (policy fingerprint),
// what was read (connection, database, schema, source revision) and, for
// snapshot reads, the immutable generation of every dataset read. A live
// source read has no generations and relies on its TTL.
type cacheKey struct {
	Tenant, Team, Principal                string
	Connection, Database, Schema, Revision string
	Policy                                 string // policyFingerprint
	Generations                            []datasetGeneration
}

// datasetGeneration names one immutable snapshot generation a read used.
type datasetGeneration struct {
	Dataset, Generation string
}

// keyHash writes length-prefixed raw bytes, so no two field sequences encode
// alike and invalid UTF-8 is hashed as is.
type keyHash struct{ hash.Hash }

func (h keyHash) field(b []byte) {
	_ = binary.Write(h, binary.BigEndian, uint64(len(b)))
	_, _ = h.Write(b)
}
func (h keyHash) str(s string) { h.field([]byte(s)) }
func (h keyHash) num(n int64)  { _ = binary.Write(h, binary.BigEndian, n) }

// resultCacheKey hashes k with the normalized request and its limits; "" means
// the request cannot be keyed and must not be cached. Parameters keep their
// declared type next to their compact JSON value, so int32 1, int64 1 and "1"
// are three keys; a value that is not valid JSON is never keyed. A refresh
// (BypassCache) does not change the key: it replaces the entry.
func resultCacheKey(k cacheKey, r query.Request, limits query.Limits) string {
	h := keyHash{sha256.New()}
	h.str("kelvo.result-cache.v4")
	for _, s := range []string{k.Tenant, k.Team, k.Principal, k.Connection, k.Database, k.Schema, k.Revision, k.Policy} {
		h.str(s)
	}
	generations := slices.Clone(k.Generations)
	slices.SortFunc(generations, func(a, b datasetGeneration) int {
		return cmp.Or(strings.Compare(a.Dataset, b.Dataset), strings.Compare(a.Generation, b.Generation))
	})
	h.num(int64(len(generations)))
	for _, g := range generations {
		h.str(g.Dataset)
		h.str(g.Generation)
	}
	h.str(r.Mode)
	h.str(strings.TrimSpace(r.SQL))
	h.num(int64(len(r.Parameters)))
	for _, p := range r.Parameters {
		var compact bytes.Buffer
		if json.Compact(&compact, p.Value) != nil {
			return ""
		}
		h.str(p.Type)
		h.field(compact.Bytes())
	}
	h.num(limits.MaxRows)
	h.num(limits.MaxBytes)
	h.str(limits.ResultCompression)
	return hex.EncodeToString(h.Sum(nil))
}

// policyFingerprint digests the effective authorization of a read: its row
// and column policy (none when unrestricted), the catalog binding digest and
// the access revision it was authorized under. Policy maps marshal with
// sorted keys, so equal policies give equal fingerprints. "" means the
// policy cannot be encoded and the read must not be cached.
func policyFingerprint(ctx context.Context, catalogBinding, accessRevision string) string {
	policy, restricted := access.PolicyFromContext(ctx)
	encoded, err := json.Marshal(policy)
	if err != nil {
		return ""
	}
	h := keyHash{sha256.New()}
	h.str("kelvo.result-cache.policy.v2")
	if restricted {
		h.str("restricted")
	} else {
		h.str("open")
	}
	h.field(encoded)
	h.str(catalogBinding)
	h.str(accessRevision)
	return hex.EncodeToString(h.Sum(nil))
}

func catalogBindingDigest(e *Executor) string {
	if e.catalogBinding == nil {
		return ""
	}
	return e.catalogBinding.digest
}
