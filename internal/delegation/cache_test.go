// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package delegation

import (
	"crypto/ed25519"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/query"
)

func cacheFixture(t testing.TB) (Trust, Claims, query.Request, string) {
	t.Helper()
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)) // public deterministic fixture, never a deployment key
	r := query.Request{Mode: "native", ConnectionID: "source_1", SQL: "SELECT 2"}
	digest, err := QueryDigest(r)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	c := Claims{Version: 1, Issuer: "api-resolver", Audience: "analytics", ClusterTenant: "a", ServicePrincipal: "bridge", AppTeam: "team-a",
		Subject: Subject{Kind: "user", ID: "user-a"}, ID: strings.Repeat("a", 48), IssuedAt: now - 1, ExpiresAt: now + 120, QuerySHA256: digest,
		Sources: []Source{{Alias: "source_1", ConnectionID: "saved-a"}}}
	token, err := Sign(c, key)
	if err != nil {
		t.Fatal(err)
	}
	trust := Trust{Issuer: "api-resolver", Audience: "analytics", ClusterTenant: "a", ServicePrincipal: "bridge", PublicKey: key.Public().(ed25519.PublicKey)}
	return trust, c, r, token
}

func freshCache(t *testing.T, max int) {
	t.Helper()
	t.Cleanup(SwapCacheForTest(NewGrantCache(max)))
}

func TestGrantIsVerifiedOncePerTokenWhateverTheCallers(t *testing.T) {
	freshCache(t, 16)
	trust, _, r, token := cacheFixture(t)
	before := RawVerifications()
	for range 50 {
		if _, err := VerifyCached(token, trust, r, time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := VerifyClaimsCached(token, trust, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if got := RawVerifications() - before; got != 2 { // one bound, one claims-only
		t.Fatalf("%d verifications for 100 calls on one token", got)
	}
}

// Whatever the clock, the cache must answer exactly as the uncached check does,
// including at and around the token's own validity window.
func TestGrantCacheMatchesUncachedVerification(t *testing.T) {
	freshCache(t, 16)
	trust, c, r, token := cacheFixture(t)
	if _, err := VerifyCached(token, trust, r, time.Unix(c.IssuedAt, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyClaimsCached(token, trust, time.Unix(c.IssuedAt, 0)); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{-3600, -31, -30, -1, 0, 1, 60, 119, 120, 121, 3600} {
		now := time.Unix(c.IssuedAt+offset, 0)
		_, cachedErr := VerifyCached(token, trust, r, now)
		_, rawErr := Verify(token, trust, r, now)
		if (cachedErr == nil) != (rawErr == nil) {
			t.Fatalf("offset %d: cached err=%v, uncached err=%v", offset, cachedErr, rawErr)
		}
		_, cachedErr = VerifyClaimsCached(token, trust, now)
		_, rawErr = VerifyClaims(token, trust, now)
		if (cachedErr == nil) != (rawErr == nil) {
			t.Fatalf("claims-only offset %d: cached err=%v, uncached err=%v", offset, cachedErr, rawErr)
		}
	}
}

func TestGrantCacheNeverCrossesTrustRequestOrToken(t *testing.T) {
	freshCache(t, 16)
	trust, _, r, token := cacheFixture(t)
	if _, err := VerifyCached(token, trust, r, time.Now()); err != nil {
		t.Fatal(err)
	}
	other := trust
	other.PublicKey = ed25519.NewKeyFromSeed(append(make([]byte, 31), 1)).Public().(ed25519.PublicKey)
	if _, err := VerifyCached(token, other, r, time.Now()); err == nil {
		t.Fatal("a different trust accepted a cached grant")
	}
	other = trust
	other.Audience = "elsewhere"
	if _, err := VerifyCached(token, other, r, time.Now()); err == nil {
		t.Fatal("a different audience accepted a cached grant")
	}
	changed := r
	changed.SQL = "SELECT 3"
	if _, err := VerifyCached(token, trust, changed, time.Now()); err == nil {
		t.Fatal("a different request reused a bound grant")
	}
	before := RawVerifications()
	for range 3 {
		_, _ = VerifyCached("not.a.token", trust, r, time.Now())
	}
	if RawVerifications()-before != 3 {
		t.Fatal("failed verifications were cached")
	}
}

func TestGrantCacheRejectsAfterTheTokensOwnExpiry(t *testing.T) {
	freshCache(t, 16)
	trust, c, r, token := cacheFixture(t)
	if _, err := VerifyCached(token, trust, r, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyCached(token, trust, r, time.Unix(c.ExpiresAt, 0)); err == nil {
		t.Fatal("expired grant accepted from the cache")
	}
}

func TestGrantCacheIsBoundedAndHandsOutCopies(t *testing.T) {
	cache := NewGrantCache(8)
	now := time.Now()
	for i := range 100 {
		var key grantKey
		key.token[0], key.token[1] = byte(i), byte(i>>8)
		cache.store(key, Claims{IssuedAt: now.Unix(), ExpiresAt: now.Unix() + 60, Sources: []Source{{Alias: "a"}}}, now)
	}
	if cache.size() > 8 {
		t.Fatalf("cache grew to %d", cache.size())
	}
	var key grantKey
	key.token[0] = 200
	cache.store(key, Claims{IssuedAt: now.Unix(), ExpiresAt: now.Unix() + 60, Sources: []Source{{Alias: "a"}}, Subject: Subject{JobConnections: map[string]string{"c": "read"}}}, now)
	got, ok := cache.lookup(key, now)
	if !ok {
		t.Fatal("fresh entry missing")
	}
	got.Sources[0].Alias = "mutated"
	got.Subject.JobConnections["c"] = "write"
	again, _ := cache.lookup(key, now)
	if again.Sources[0].Alias != "a" || again.Subject.JobConnections["c"] != "read" {
		t.Fatal("a caller changed the cached grant")
	}
}

func TestRequestFingerprintSeparatesFields(t *testing.T) {
	a := query.Request{Mode: "federated", Sources: []string{"ab", "c"}, SQL: "x"}
	b := query.Request{Mode: "federated", Sources: []string{"a", "bc"}, SQL: "x"}
	c := query.Request{Mode: "federated", Sources: []string{"ab"}, SQL: "cx"}
	if requestFingerprint(a) == requestFingerprint(b) || requestFingerprint(a) == requestFingerprint(c) {
		t.Fatal("distinct requests share a fingerprint")
	}
	d := a
	d.BypassCache = true
	if requestFingerprint(a) != requestFingerprint(d) {
		t.Fatal("bypass_cache must not change what a grant is bound to")
	}
}

func TestGrantCacheIsSafeUnderConcurrency(t *testing.T) {
	freshCache(t, 4)
	trust, _, r, token := cacheFixture(t)
	before := RawVerifications()
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 200 {
				if _, err := VerifyCached(token, trust, r, time.Now()); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if got := RawVerifications() - before; got > 16 {
		t.Fatalf("%d verifications for 3200 calls", got)
	}
}

func BenchmarkVerifyUncachedVersusCached(b *testing.B) {
	trust, _, r, token := cacheFixture(b)
	b.Run("uncached", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = Verify(token, trust, r, time.Now())
		}
	})
	b.Run("cached", func(b *testing.B) {
		defer SwapCacheForTest(NewGrantCache(16))()
		b.ReportAllocs()
		for range b.N {
			_, _ = VerifyCached(token, trust, r, time.Now())
		}
	})
}
