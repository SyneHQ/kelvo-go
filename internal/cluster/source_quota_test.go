// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type quotaEntry struct {
	jetstream.KeyValueEntry
	raw      []byte
	revision uint64
	expires  time.Time
}

func (e quotaEntry) Value() []byte    { return append([]byte(nil), e.raw...) }
func (e quotaEntry) Revision() uint64 { return e.revision }

type quotaFixture struct {
	mu           sync.Mutex
	entries      map[string]quotaEntry
	revision     uint64
	ttl          time.Duration
	fail         bool
	blockUpdates bool
}

func (f *quotaFixture) get(key string) (quotaEntry, bool) {
	e, ok := f.entries[key]
	if ok && !time.Now().Before(e.expires) {
		delete(f.entries, key)
		ok = false
	}
	return e, ok
}
func (f *quotaFixture) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return nil, errors.New("backend sensitive diagnostic")
	}
	e, ok := f.get(key)
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return e, nil
}
func (f *quotaFixture) Create(ctx context.Context, key string, raw []byte, _ ...jetstream.KVCreateOpt) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return 0, errors.New("backend sensitive diagnostic")
	}
	if _, ok := f.get(key); ok {
		return 0, jetstream.ErrKeyExists
	}
	f.revision++
	f.entries[key] = quotaEntry{raw: append([]byte(nil), raw...), revision: f.revision, expires: time.Now().Add(f.ttl)}
	return f.revision, nil
}
func (f *quotaFixture) Update(ctx context.Context, key string, raw []byte, revision uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	if f.blockUpdates && len(raw) > 0 {
		f.mu.Unlock()
		<-ctx.Done()
		return 0, ctx.Err()
	}
	defer f.mu.Unlock()
	if f.fail {
		return 0, errors.New("backend sensitive diagnostic")
	}
	e, ok := f.get(key)
	if !ok || e.revision != revision {
		return 0, jetstream.ErrKeyRevisionMismatch
	}
	f.revision++
	f.entries[key] = quotaEntry{raw: append([]byte(nil), raw...), revision: f.revision, expires: time.Now().Add(f.ttl)}
	return f.revision, nil
}
func quotaPool(quotas map[string]int) (*SourceQuotaPool, *quotaFixture) {
	lease := 300 * time.Millisecond
	f := &quotaFixture{entries: map[string]quotaEntry{}, ttl: 2 * lease}
	return &SourceQuotaPool{kv: f, quotas: quotas, lease: lease}, f
}
func quotaDeadline(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 3*time.Second)
}

func TestSourceQuotasValidateBoundedIdentities(t *testing.T) {
	for _, q := range []map[string]int{{"bad.id": 1}, {"source": 0}, {"source": 65}} {
		if ValidateSourceQuotas(q) == nil {
			t.Fatal("invalid quotas accepted")
		}
	}
	q := map[string]int{}
	for i := 0; i < 65; i++ {
		q["source"+strings.Repeat("x", i)] = 1
	}
	if ValidateSourceQuotas(q) == nil {
		t.Fatal("too many quotas accepted")
	}
	if err := ValidateSourceQuotas(map[string]int{"orders": 64}); err != nil {
		t.Fatal(err)
	}
}

func TestSourceQuotasShareCapacityAcrossPoolInstances(t *testing.T) {
	p, fixture := quotaPool(map[string]int{"orders": 1})
	other := &SourceQuotaPool{kv: fixture, quotas: p.quotas, lease: p.lease}
	ctx, stop := quotaDeadline(t)
	defer stop()
	owned, release, err := p.Acquire(ctx, []string{"orders", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	blocked, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	_, _, err = other.Acquire(blocked, []string{"orders"})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared quota exceeded: %v", err)
	}
	if owned.Err() != nil {
		t.Fatal("waiting peer canceled owner")
	}
	release()
	release()
	_, release2, err := other.Acquire(ctx, []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	release2()
}

func TestSourceQuotaJoinRollsBackPartialAcquisition(t *testing.T) {
	p, f := quotaPool(map[string]int{"a": 1, "b": 1})
	ctx, stop := quotaDeadline(t)
	defer stop()
	_, release, err := p.Acquire(ctx, []string{"b"})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	limited, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	_, _, err = p.Acquire(limited, []string{"b", "a"})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	f.mu.Lock()
	entry, _ := f.get(sourceQuotaKey("a", 0))
	f.mu.Unlock()
	if len(entry.raw) != 0 {
		t.Fatal("join retained partial reservation while waiting")
	}
	_, releaseA, err := p.Acquire(ctx, []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	releaseA()
}

func TestSourceQuotaRenewalLossCancelsAndFencesRelease(t *testing.T) {
	p, f := quotaPool(map[string]int{"a": 1})
	ctx, stop := quotaDeadline(t)
	defer stop()
	owned, release, err := p.Acquire(ctx, []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	key := sourceQuotaKey("a", 0)
	f.mu.Lock()
	old := f.entries[key]
	f.revision++
	replacement := quotaEntry{raw: []byte(strings.Repeat("f", 32)), revision: f.revision, expires: time.Now().Add(time.Hour)}
	f.entries[key] = replacement
	f.mu.Unlock()
	select {
	case <-owned.Done():
	case <-ctx.Done():
		t.Fatal("lease loss did not cancel execution")
	}
	if !errors.Is(context.Cause(owned), ErrSourceQuotaLeaseLost) {
		t.Fatal(context.Cause(owned))
	}
	release()
	f.mu.Lock()
	current := f.entries[key]
	f.mu.Unlock()
	if current.revision == old.revision || string(current.raw) != string(replacement.raw) {
		t.Fatal("stale release overwrote successor")
	}
}

func TestSourceQuotaWatchdogCancelsBeforeBrokerExpiry(t *testing.T) {
	p, f := quotaPool(map[string]int{"a": 1})
	ctx, stop := quotaDeadline(t)
	defer stop()
	owned, release, err := p.Acquire(ctx, []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	f.mu.Lock()
	f.blockUpdates = true
	expires := f.entries[sourceQuotaKey("a", 0)].expires
	f.mu.Unlock()
	select {
	case <-owned.Done():
	case <-ctx.Done():
		t.Fatal("blocked renewal did not cancel")
	}
	if !errors.Is(context.Cause(owned), ErrSourceQuotaLeaseLost) || !time.Now().Before(expires) {
		t.Fatal("operation outlived broker lease margin")
	}
}

func TestSourceQuotaBrokerExpiryRecoversCrashedOwner(t *testing.T) {
	p, f := quotaPool(map[string]int{"a": 1})
	key := sourceQuotaKey("a", 0)
	f.entries[key] = quotaEntry{raw: []byte(strings.Repeat("a", 32)), revision: 1, expires: time.Now().Add(-time.Second)}
	f.revision = 1
	ctx, stop := quotaDeadline(t)
	defer stop()
	_, release, err := p.Acquire(ctx, []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestSourceQuotaFailureIsSanitizedAndUnconfiguredSourcesBypass(t *testing.T) {
	p, f := quotaPool(map[string]int{"a": 1})
	f.fail = true
	ctx, stop := quotaDeadline(t)
	defer stop()
	_, _, err := p.Acquire(ctx, []string{"a"})
	if !errors.Is(err, ErrSourceQuotaUnavailable) || strings.Contains(err.Error(), "sensitive") {
		t.Fatal(err)
	}
	_, release, err := p.Acquire(ctx, []string{"unconfigured"})
	if err != nil {
		t.Fatal(err)
	}
	release()
	var empty *SourceQuotaPool
	_, release, err = empty.Acquire(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestSourceQuotaRenewalKeepsSlotBeyondOriginalTTL(t *testing.T) {
	p, f := quotaPool(map[string]int{"a": 1})
	ctx, stop := quotaDeadline(t)
	defer stop()
	owned, release, err := p.Acquire(ctx, []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	timer := time.NewTimer(f.ttl + 50*time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-owned.Done():
		t.Fatal("healthy lease expired", context.Cause(owned))
	}
	f.mu.Lock()
	entry, ok := f.get(sourceQuotaKey("a", 0))
	f.mu.Unlock()
	if !ok || len(entry.raw) == 0 || entry.revision < 2 {
		t.Fatal("lease did not renew")
	}
}

func TestNATSSourceQuotaSharedAdmissionAndBrokerExpiry(t *testing.T) {
	store := openFixture(t)
	// Isolate quota resource provisioning from the shared job-policy fixture;
	// production policy binding is performed by OpenStore before this API call.
	quotaStore := *store
	quotaStore.policy.SourceQuotas = map[string]int{"quota_live_source": 1}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, err := quotaStore.OpenSourceQuotas(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := quotaStore.OpenSourceQuotas(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	_, release, err := first.Acquire(ctx, []string{"quota_live_source"})
	if err != nil {
		t.Fatal(err)
	}
	waiting, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	_, _, err = second.Acquire(waiting, []string{"quota_live_source"})
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		release()
		t.Fatalf("live shared slot exceeded: %v", err)
	}
	release()
	key := sourceQuotaKey("quota_live_source", 0)
	entry, err := first.kv.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = first.kv.Update(ctx, key, []byte(strings.Repeat("b", 32)), entry.Revision()); err != nil {
		t.Fatal(err)
	}
	// Simulate a crashed owner: no process renews or explicitly releases this
	// marker. Only broker TTL can make it reusable; host timestamps are absent.
	started := time.Now()
	_, releaseRecovered, err := second.Acquire(ctx, []string{"quota_live_source"})
	if err != nil {
		t.Fatal(err)
	}
	defer releaseRecovered()
	if time.Since(started) < 2*quotaStore.policy.LeaseDuration-250*time.Millisecond {
		t.Fatal("live occupied slot reclaimed before broker TTL")
	}
}
