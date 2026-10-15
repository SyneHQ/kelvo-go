// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package delegation

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/query"
)

// One read verifies its delegation about six times on the gateway and again on
// the node (submit, the store's own admission, every result read, the commit),
// and each verification decodes the token strictly and checks a signature. A
// verified grant is remembered, keyed by what the verification depended on, so
// the work is done once per token per process:
//
//   - the token itself (by digest),
//   - the trust it was checked against (issuer, audience, tenant, principal, key),
//   - for a bound verification, the exact request it was bound to.
//
// An entry is used only while the grant itself is valid: the two time checks of
// the claims are repeated on every hit, so a token is never accepted after its
// own expiry. Nothing else a grant's acceptance depends on is cached: the
// principal's current authority, the request's key authorization and the job's
// stored authority are still checked on every read by their callers. There is
// no per-token revocation to miss; a grant ends by expiry or when its principal
// or trust changes, and a changed trust is a different key here.
type GrantCache struct {
	mu  sync.Mutex
	m   map[grantKey]Claims
	max int
}

type grantKey struct {
	token, trust, request [32]byte
	bound                 bool
}

const DefaultCacheEntries = 4096

func NewGrantCache(max int) *GrantCache {
	return &GrantCache{m: make(map[grantKey]Claims), max: max}
}

// DefaultCache is shared by the gateway and node code of one process.
var DefaultCache = NewGrantCache(DefaultCacheEntries)

// rawVerifications counts the checks the cache did not answer, for tests.
var rawVerifications atomic.Int64

func rawVerify(token string, trust Trust, r query.Request, now time.Time) (Claims, error) {
	rawVerifications.Add(1)
	return Verify(token, trust, r, now)
}

func rawVerifyClaims(token string, trust Trust, now time.Time) (Claims, error) {
	rawVerifications.Add(1)
	return VerifyClaims(token, trust, now)
}

// RawVerifications is how many verifications ran uncached in this process.
// Tests read the difference around the code under test.
func RawVerifications() int64 { return rawVerifications.Load() }

// SwapCacheForTest replaces the process cache, nil to disable it, and returns
// the restore function.
func SwapCacheForTest(c *GrantCache) (restore func()) {
	previous := DefaultCache
	DefaultCache = c
	return func() { DefaultCache = previous }
}

// VerifyCached is Verify through the cache.
func VerifyCached(token string, trust Trust, r query.Request, now time.Time) (Claims, error) {
	c := DefaultCache
	if c == nil {
		return rawVerify(token, trust, r, now)
	}
	key := grantKey{token: sha256.Sum256([]byte(token)), trust: trustFingerprint(trust), request: requestFingerprint(r), bound: true}
	if claims, ok := c.lookup(key, now); ok {
		return claims, nil
	}
	claims, err := rawVerify(token, trust, r, now)
	if err == nil {
		c.store(key, claims, now)
	}
	return claims, err
}

// VerifyClaimsCached is VerifyClaims through the cache.
func VerifyClaimsCached(token string, trust Trust, now time.Time) (Claims, error) {
	c := DefaultCache
	if c == nil {
		return rawVerifyClaims(token, trust, now)
	}
	key := grantKey{token: sha256.Sum256([]byte(token)), trust: trustFingerprint(trust)}
	if claims, ok := c.lookup(key, now); ok {
		return claims, nil
	}
	claims, err := rawVerifyClaims(token, trust, now)
	if err == nil {
		c.store(key, claims, now)
	}
	return claims, err
}

// claimsTimeValid repeats the only checks of a verified grant that depend on
// the clock: not issued in the future (30 s of skew) and not expired. It must
// match the delegation package's; TestGrantCacheMatchesUncachedVerification
// compares the two across the boundaries.
func claimsTimeValid(c Claims, now time.Time) bool {
	return c.IssuedAt <= now.Unix()+30 && c.ExpiresAt > now.Unix()
}

func (c *GrantCache) lookup(key grantKey, now time.Time) (Claims, bool) {
	c.mu.Lock()
	claims, ok := c.m[key]
	c.mu.Unlock()
	if !ok || !claimsTimeValid(claims, now) {
		return Claims{}, false
	}
	return cloneClaims(claims), true
}

func (c *GrantCache) store(key grantKey, claims Claims, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) >= c.max {
		// Expired grants first; if that frees nothing, make room by dropping
		// arbitrary entries. A dropped entry only costs one more verification.
		for k, v := range c.m {
			if !claimsTimeValid(v, now) {
				delete(c.m, k)
			}
		}
		for k := range c.m {
			if len(c.m) < c.max {
				break
			}
			delete(c.m, k)
		}
	}
	c.m[key] = cloneClaims(claims)
}

func (c *GrantCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}

func trustFingerprint(t Trust) [32]byte {
	h := sha256.New()
	for _, s := range [...]string{t.Issuer, t.Audience, t.ClusterTenant, t.ServicePrincipal, string(t.PublicKey)} {
		writeField(h, s)
	}
	var out [32]byte
	h.Sum(out[:0])
	return out
}

// requestFingerprint covers everything Verify reads from the request
// (its query digest, source scope and validity) with length-prefixed fields, so
// distinct requests cannot collide by concatenation.
func requestFingerprint(r query.Request) [32]byte {
	h := sha256.New()
	writeField(h, r.Mode)
	writeField(h, r.SQL)
	writeField(h, r.ConnectionID)
	writeCount(h, len(r.Sources))
	for _, s := range r.Sources {
		writeField(h, s)
	}
	writeCount(h, len(r.Parameters))
	for _, p := range r.Parameters {
		writeField(h, p.Type)
		writeField(h, string(p.Value))
	}
	// ValidateRequest also looks at these, not at the token's content.
	var flags byte
	if r.Mongo != nil {
		flags |= 1
	}
	if r.ScanDiagnostics {
		flags |= 2
	}
	if r.Delegation != "" {
		flags |= 4
	}
	if len(r.Delegation) > 32768 {
		flags |= 8
	}
	h.Write([]byte{flags})
	var out [32]byte
	h.Sum(out[:0])
	return out
}

type byteWriter interface{ Write([]byte) (int, error) }

func writeCount(h byteWriter, n int) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	h.Write(b[:])
}

func writeField(h byteWriter, s string) {
	writeCount(h, len(s))
	h.Write([]byte(s))
}
