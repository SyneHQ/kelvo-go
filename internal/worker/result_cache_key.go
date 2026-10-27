// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

// resultCacheKey hashes k with the normalized request and its limits.
// Parameters keep their declared type next to their compact JSON value, so
// int32 1, int64 1 and "1" are three keys. A refresh (BypassCache) does not
// change the key: it replaces the entry.
func resultCacheKey(k cacheKey, r query.Request, limits query.Limits) string {
	generations := slices.Clone(k.Generations)
	slices.SortFunc(generations, func(a, b datasetGeneration) int {
		return cmp.Or(strings.Compare(a.Dataset, b.Dataset), strings.Compare(a.Generation, b.Generation))
	})
	parameters := make([][2]any, len(r.Parameters))
	for i, p := range r.Parameters {
		parameters[i] = [2]any{p.Type, p.Value} // json.Marshal compacts RawMessage
	}
	raw, _ := json.Marshal([]any{"kelvo.result-cache.v3", k.Tenant, k.Team, k.Principal, k.Connection, k.Database, k.Schema, k.Revision, k.Policy, generations,
		r.Mode, strings.TrimSpace(r.SQL), parameters, limits.MaxRows, limits.MaxBytes, limits.ResultCompression})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// policyFingerprint digests the effective authorization of a read: its row
// and column policy (none when unrestricted), the catalog binding digest and
// the access revision it was authorized under. Policy maps marshal with
// sorted keys, so equal policies give equal fingerprints.
func policyFingerprint(ctx context.Context, catalogBinding, accessRevision string) string {
	policy, restricted := access.PolicyFromContext(ctx)
	raw, _ := json.Marshal([]any{"kelvo.result-cache.policy.v1", restricted, policy, catalogBinding, accessRevision})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
