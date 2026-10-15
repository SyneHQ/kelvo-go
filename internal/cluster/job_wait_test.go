// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// waitStore serves one job and counts reads. With watch set it also offers the
// optional change signal.
type waitStore struct {
	gatewayStore
	mu    sync.Mutex
	job   Snapshot
	gets  atomic.Int32
	watch bool
	ch    chan struct{}
}

func (s *waitStore) Get(_ context.Context, id string) (Snapshot, error) {
	s.gets.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.job, nil
}

func (s *waitStore) set(state string) {
	s.mu.Lock()
	s.job.Job.State = state
	s.job.Revision++
	s.mu.Unlock()
	if s.ch != nil {
		select {
		case s.ch <- struct{}{}:
		default:
		}
	}
}

type watchingStore struct{ *waitStore }

func (s watchingStore) WatchJob(context.Context, string) (<-chan struct{}, func(), error) {
	return s.ch, func() {}, nil
}

func countVerifications(t *testing.T) (*atomic.Int32, func()) {
	t.Helper()
	var n atomic.Int32
	original := verifyDelegation
	verifyDelegation = func(token string, trust delegation.Trust, r query.Request, now time.Time) (delegation.Claims, error) {
		n.Add(1)
		return original(token, trust, r, now)
	}
	return &n, func() { verifyDelegation = original }
}

// delegatedWait runs a result wait on a delegated, queued job until it expires.
func delegatedWait(t *testing.T, jobLives, grantLives time.Duration) (int, *waitStore, *atomic.Int32) {
	t.Helper()
	p, key, c, r, a := delegatedTestGrant(t)
	c.IssuedAt = time.Now().Unix() - 1
	c.ExpiresAt = time.Now().Add(grantLives).Unix() + 1
	var err error
	if r.Delegation, err = delegation.Sign(c, key); err != nil {
		t.Fatal(err)
	}
	bound, _, err := delegatedAuthority(p, a, r, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	store := &waitStore{gatewayStore: gatewayStore{policy: p}, job: Snapshot{Revision: 1, Job: Job{ID: "owned", TenantID: p.TenantID, Authority: &bound, Request: r, State: Queued, ExpiresAt: time.Now().Add(jobLives)}}}
	verifications, restore := countVerifications(t)
	defer restore()
	req := httptest.NewRequest(http.MethodGet, "/v1/queries/owned/results", nil).WithContext(delegatedRequestContext(a, r.Delegation))
	w := httptest.NewRecorder()
	(&Gateway{}).results(w, req, gatewayTenant{store: store}, "owned", nil)
	return w.Code, store, verifications
}

// The signature check is per request, not per poll: the count must not grow
// with how long the request waits.
func TestResultWaitVerifiesTheGrantOncePerRequestWhateverTheWait(t *testing.T) {
	_, _, short := delegatedWait(t, 200*time.Millisecond, time.Minute)
	code, store, long := delegatedWait(t, 1500*time.Millisecond, time.Minute)
	if code != http.StatusNotFound {
		t.Fatalf("status=%d", code)
	}
	if short.Load() != long.Load() || long.Load() > 2 {
		t.Fatalf("verifications: 200ms wait=%d, 1500ms wait=%d (want equal and at most 2)", short.Load(), long.Load())
	}
	t.Logf("verifications for a 200 ms wait=%d and a 1500 ms wait=%d; store reads over 1500 ms=%d", short.Load(), long.Load(), store.gets.Load())
}

// A cached authorization must still end when the delegation expires.
func TestCachedAuthorizationStopsWhenTheGrantExpires(t *testing.T) {
	started := time.Now()
	code, _, _ := delegatedWait(t, 10*time.Second, time.Second)
	if code != http.StatusNotFound || time.Since(started) > 4*time.Second {
		t.Fatalf("status=%d after %s", code, time.Since(started))
	}
}

// Without a watch the wait backs off instead of reading every 50 ms.
func TestResultWaitBacksOffWithoutAWatch(t *testing.T) {
	_, store, _ := delegatedWait(t, 1500*time.Millisecond, time.Minute)
	if got := store.gets.Load(); got > 10 {
		t.Fatalf("%d store reads in 1.5 s; the 50 ms poll made about 30", got)
	}
}

// With a watch the wait sleeps until the store signals, so a state change is
// seen at once and the store is barely read.
func TestResultWaitWakesOnStoreSignalInsteadOfPolling(t *testing.T) {
	store := &waitStore{gatewayStore: gatewayStore{policy: Policy{TenantID: "a"}}, ch: make(chan struct{}, 1), watch: true,
		job: Snapshot{Revision: 1, Job: Job{ID: "w", TenantID: "a", State: Queued, ExpiresAt: time.Now().Add(time.Minute)}}}
	watching := watchingStore{store}
	changed := make(chan time.Time, 1)
	go func() {
		time.Sleep(600 * time.Millisecond)
		changed <- time.Now()
		store.set(Failed)
	}()
	w := httptest.NewRecorder()
	(&Gateway{}).results(w, httptest.NewRequest(http.MethodGet, "/v1/queries/w/results", nil), gatewayTenant{store: watching}, "w", nil)
	seen := time.Now()
	if w.Code != http.StatusConflict {
		t.Fatalf("status=%d", w.Code)
	}
	if lag := seen.Sub(<-changed); lag > 300*time.Millisecond {
		t.Fatalf("wake took %s; the safety timer is 1 s, so this was not the signal", lag)
	}
	if got := store.gets.Load(); got > 4 {
		t.Fatalf("%d store reads while waiting 600 ms", got)
	}
}
