// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type nodeTestStore struct {
	mu    sync.Mutex
	p     Policy
	jobs  map[string]Snapshot
	queue chan Delivery
	acks  atomic.Int32
}
type nodeTestDelivery struct {
	id    string
	store *nodeTestStore
}

func (d nodeTestDelivery) ID() string                  { return d.id }
func (d nodeTestDelivery) Ack(context.Context) error   { d.store.acks.Add(1); return nil }
func (d nodeTestDelivery) Retry(context.Context) error { return nil }
func testPolicy() Policy {
	return Policy{TenantID: "a", MaxQueries: 8, JobTTL: time.Minute, LeaseDuration: 5 * time.Second, Replicas: 1, Limits: query.DefaultLimits(), Workers: map[string]int{"a1": 1}}
}
func (s *nodeTestStore) Policy() Policy { return s.p }
func (s *nodeTestStore) Submit(context.Context, query.Request) (Snapshot, error) {
	return Snapshot{}, ErrCapacity
}
func (s *nodeTestStore) Get(_ context.Context, id string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.jobs[id]
	if !ok {
		return v, ErrNotFound
	}
	return v, nil
}
func (s *nodeTestStore) CompareAndSwap(_ context.Context, old Snapshot, next Job) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.jobs[old.Job.ID]
	if !ok || v.Revision != old.Revision {
		return Snapshot{}, ErrConflict
	}
	v = Snapshot{Job: next, Revision: v.Revision + 1}
	s.jobs[next.ID] = v
	return v, nil
}
func (s *nodeTestStore) Enqueue(ctx context.Context, id string) error {
	select {
	case s.queue <- nodeTestDelivery{id, s}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *nodeTestStore) Next(ctx context.Context) (Delivery, error) {
	select {
	case d := <-s.queue:
		return d, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (s *nodeTestStore) Reconcile(context.Context) error                       { return nil }
func (s *nodeTestStore) ClaimWorker(context.Context, string, string) error     { return nil }
func (s *nodeTestStore) HeartbeatWorker(context.Context, string, string) error { return nil }
func (s *nodeTestStore) Close() error                                          { return nil }
func (s *nodeTestStore) add(id string) {
	s.mu.Lock()
	s.jobs[id] = Snapshot{Revision: 1, Job: Job{ID: id, TenantID: s.p.TenantID, State: Queued, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute), Request: query.Request{Mode: "federated", SQL: "SELECT 2"}}}
	s.mu.Unlock()
	_ = s.Enqueue(context.Background(), id)
}
func waitFor(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

type heldExecutor struct {
	started   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	calls     atomic.Int32
}

func (e *heldExecutor) Execute(ctx context.Context, r query.Request, _ query.Sink) (query.Stats, error) {
	if r.SQL == "SELECT 1" {
		return query.Stats{}, nil
	}
	e.calls.Add(1)
	close(e.started)
	<-ctx.Done()
	close(e.cancelled)
	<-e.release
	return query.Stats{}, ctx.Err()
}

func TestNodeCancellationHoldsPermitUntilExecutorExits(t *testing.T) {
	s := &nodeTestStore{p: testPolicy(), jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}
	e := &heldExecutor{started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	n, err := newNode(NodeConfig{Policy: s.p, WorkerID: "a1"}, s, e)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	s.add(id)
	waitFor(t, func() bool { v, _ := s.Get(context.Background(), id); return v.Job.State == Assigned })
	v, _ := s.Get(context.Background(), id)
	next := v.Job
	next.State = Claimed
	next.Claim = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err = s.CompareAndSwap(context.Background(), v, next); err != nil {
		t.Fatal(err)
	}
	uri, _ := url.Parse(GatewayIdentity)
	cert := &x509.Certificate{URIs: []*url.URL{uri}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, "/internal/queries/"+id+"/results", nil).WithContext(ctx)
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}
	r.Header.Set("X-Kelvo-Claim", next.Claim)
	done := make(chan struct{})
	go func() { defer close(done); n.ServeHTTP(httptest.NewRecorder(), r) }()
	select {
	case <-e.started:
	case <-time.After(3 * time.Second):
		t.Fatal("execution did not start")
	}
	s.add("1-cccccccccccccccccccccccccccccccc")
	cancel()
	select {
	case <-e.cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not reach executor")
	}
	// A cancelled query can still have native cleanup in flight. Its permit
	// must remain occupied until that cleanup has actually completed.
	time.Sleep(30 * time.Millisecond)
	v, _ = s.Get(context.Background(), "1-cccccccccccccccccccccccccccccccc")
	if v.Job.State != Queued {
		t.Errorf("next job assigned before executor stopped: %s", v.Job.State)
	}
	close(e.release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not finish")
	}
	waitFor(t, func() bool {
		v, _ := s.Get(context.Background(), "1-cccccccccccccccccccccccccccccccc")
		return v.Job.State == Assigned
	})
}

type probeExecutor struct{}

func (probeExecutor) Execute(context.Context, query.Request, query.Sink) (query.Stats, error) {
	return query.Stats{}, nil
}
func TestNodeDuplicateDeliveryDoesNotExecuteOrReassign(t *testing.T) {
	p := testPolicy()
	p.Workers["a1"] = 2
	s := &nodeTestStore{p: p, jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}
	n, err := newNode(NodeConfig{Policy: p, WorkerID: "a1"}, s, probeExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	s.add(id)
	waitFor(t, func() bool { v, _ := s.Get(context.Background(), id); return v.Job.State == Assigned })
	first, _ := s.Get(context.Background(), id)
	_ = s.Enqueue(context.Background(), id)
	waitFor(t, func() bool { return s.acks.Load() >= 2 })
	last, _ := s.Get(context.Background(), id)
	if first.Revision != last.Revision || first.Job.Owner != last.Job.Owner {
		t.Fatal("redelivery changed assignment")
	}
	if len(n.permits) > 2 {
		t.Fatal("capacity exceeded")
	}
}

func TestNodeRequiresMatchingPolicyAndGatewayIdentity(t *testing.T) {
	p := testPolicy()
	s := &nodeTestStore{p: p, jobs: map[string]Snapshot{}, queue: make(chan Delivery)}
	mismatch := p
	mismatch.MaxQueries++
	if _, err := newNode(NodeConfig{Policy: mismatch, WorkerID: "a1"}, s, probeExecutor{}); err == nil {
		t.Fatal("mismatched policy accepted")
	}
	n, err := newNode(NodeConfig{Policy: p, WorkerID: "a1"}, s, probeExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	w := httptest.NewRecorder()
	n.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unverified caller: %d", w.Code)
	}
	if _, err := NewNode(NodeConfig{}, s, nil); err == nil || errors.Is(err, ErrNoJob) {
		t.Fatal("nil executor accepted")
	}
}
