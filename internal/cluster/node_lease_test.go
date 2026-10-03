// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type leaseCountingStore struct {
	*nodeTestStore
	reads  atomic.Int32
	writes atomic.Int32
}

func (s *leaseCountingStore) Get(ctx context.Context, id string) (Snapshot, error) {
	s.reads.Add(1)
	return s.nodeTestStore.Get(ctx, id)
}

func (s *leaseCountingStore) CompareAndSwap(ctx context.Context, old Snapshot, next Job) (Snapshot, error) {
	s.writes.Add(1)
	return s.nodeTestStore.CompareAndSwap(ctx, old, next)
}

func TestNodePollsCancellationWithoutRewritingFreshLease(t *testing.T) {
	p := testPolicy()
	p.LeaseDuration = 30 * time.Second
	const id = "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	s := &leaseCountingStore{nodeTestStore: &nodeTestStore{p: p, jobs: map[string]Snapshot{
		id: {Revision: 1, Job: Job{ID: id, State: Running, Owner: "owner", WorkerID: "a1", HeartbeatAt: time.Now()}},
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rctx, rcancel := context.WithCancel(ctx)
	defer rcancel()
	r := &reservation{ctx: rctx, cancel: rcancel, done: make(chan struct{}), started: true}
	n := &Node{cfg: NodeConfig{Policy: p, WorkerID: "a1"}, store: s, owner: "owner", ctx: ctx, jobs: map[string]*reservation{id: r}}
	n.wg.Add(1)
	go n.watch(id, r)
	defer n.wg.Wait()
	defer close(r.done)
	waitFor(t, func() bool { return s.reads.Load() >= 2 })
	if count := s.writes.Load(); count != 0 {
		t.Errorf("fresh lease rewrote durable state %d times while polling", count)
	}
	s.mu.Lock()
	v := s.jobs[id]
	v.Job.State = Cancelled
	v.Revision++
	s.jobs[id] = v
	s.mu.Unlock()
	select {
	case <-rctx.Done():
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("durable cancellation waited for the lease renewal interval")
	}
}

type overlappingMutationStore struct {
	*nodeTestStore
	entered chan struct{}
	release chan struct{}
	active  atomic.Int32
	overlap atomic.Bool
	first   atomic.Bool
	delay   time.Duration
}

func (s *overlappingMutationStore) wait(ctx context.Context, delay time.Duration) error {
	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *overlappingMutationStore) Get(ctx context.Context, id string) (Snapshot, error) {
	if err := s.wait(ctx, s.delay); err != nil {
		return Snapshot{}, err
	}
	return s.nodeTestStore.Get(ctx, id)
}

func (s *overlappingMutationStore) CompareAndSwap(ctx context.Context, old Snapshot, next Job) (Snapshot, error) {
	if s.active.Add(1) > 1 {
		s.overlap.Store(true)
	}
	defer s.active.Add(-1)
	if s.first.CompareAndSwap(false, true) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return Snapshot{}, ctx.Err()
		}
	}
	// NATS CAS performs a second Get followed by its revision-checked Update.
	if err := s.wait(ctx, 2*s.delay); err != nil {
		return Snapshot{}, err
	}
	return s.nodeTestStore.CompareAndSwap(ctx, old, next)
}

func TestNodeFinalizationDoesNotCompeteWithItsLeaseMutation(t *testing.T) {
	const id = "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	p := testPolicy()
	s := &overlappingMutationStore{nodeTestStore: &nodeTestStore{p: p, jobs: map[string]Snapshot{
		id: {Revision: 1, Job: Job{ID: id, State: Running, Owner: "owner", WorkerID: "a1", HeartbeatAt: time.Now().Add(-time.Minute)}},
	}}, entered: make(chan struct{}), release: make(chan struct{}), delay: 200 * time.Millisecond}
	r := &reservation{done: make(chan struct{})}
	n := &Node{cfg: NodeConfig{Policy: p, WorkerID: "a1"}, store: s, owner: "owner", jobs: map[string]*reservation{id: r}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	updated := make(chan error, 1)
	go func() { updated <- n.renewLease(ctx, id) }()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("heartbeat mutation did not start")
	}
	finished := make(chan error, 1)
	go func() { finished <- n.finish(id, ResultReady, query.Stats{Rows: 42}, nil) }()
	time.Sleep(100 * time.Millisecond)
	close(s.release)
	for _, result := range []<-chan error{updated, finished} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("mutation did not complete")
		}
	}
	if s.overlap.Load() {
		t.Fatal("finalization and lease renewal issued competing CAS writes")
	}
	v, err := s.Get(ctx, id)
	if err != nil || v.Job.State != ResultReady || v.Job.Stats.Rows != 42 {
		t.Fatalf("final result was not preserved: %+v %v", v.Job, err)
	}
}

func leaseTestNode() (*Node, *leaseCountingStore, string) {
	const id = "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	p := testPolicy()
	p.LeaseDuration = 30 * time.Second
	s := &leaseCountingStore{nodeTestStore: &nodeTestStore{p: p, jobs: map[string]Snapshot{
		id: {Revision: 1, Job: Job{ID: id, State: Running, Owner: "owner", WorkerID: "a1", HeartbeatAt: time.Now()}},
	}}}
	n := &Node{cfg: NodeConfig{Policy: p, WorkerID: "a1"}, store: s, owner: "owner", jobs: map[string]*reservation{id: {done: make(chan struct{})}}}
	return n, s, id
}

func TestNodeLeaseFreshnessUsesObservedClockDirection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		age    time.Duration
		writes int32
	}{
		{"fresh", time.Second, 0}, {"expired", time.Minute, 1}, {"future", -time.Minute, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, s, id := leaseTestNode()
			v := s.jobs[id]
			v.Job.HeartbeatAt = time.Now().Add(-tc.age)
			s.jobs[id] = v
			if err := n.renewLease(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			if s.reads.Load() != 1 || s.writes.Load() != tc.writes {
				t.Fatalf("reads=%d writes=%d", s.reads.Load(), s.writes.Load())
			}
		})
	}
}

func TestNodeFinalizationWaitingForRenewalRetainsDeadline(t *testing.T) {
	n, s, id := leaseTestNode()
	unlock, err := n.lockLeaseMutation(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	started := time.Now()
	err = n.finish(id, ResultReady, query.Stats{Rows: 42}, nil)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 5*time.Second {
		t.Fatalf("unbounded finalization: %v", err)
	}
	if s.reads.Load() != 0 || s.writes.Load() != 0 {
		t.Fatal("expired finalizer accessed durable state")
	}
}

func TestNodeCancellationAndOwnershipFenceWaitingFinalizer(t *testing.T) {
	for _, outcome := range []string{Cancelled, Failed, Succeeded, "different-owner", "different-worker"} {
		t.Run(outcome, func(t *testing.T) {
			n, s, id := leaseTestNode()
			unlock, err := n.lockLeaseMutation(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			finished := make(chan error, 1)
			go func() { finished <- n.finish(id, ResultReady, query.Stats{Rows: 42}, nil) }()
			if outcome == Cancelled || outcome == Failed {
				// Cancellation and failure must publish even while renewal holds the gate.
				if err := n.finish(id, outcome, query.Stats{}, query.NewError("CANCELLED", "stopped")); err != nil {
					unlock()
					t.Fatal(err)
				}
			} else {
				s.mu.Lock()
				v := s.jobs[id]
				if outcome == "different-owner" {
					v.Job.Owner = "other"
				} else if outcome == "different-worker" {
					v.Job.WorkerID = "a2"
				} else {
					v.Job.State = outcome
				}
				v.Revision++
				s.jobs[id] = v
				s.mu.Unlock()
			}
			unlock()
			select {
			case err := <-finished:
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("waiting finalizer overwrote authority change: %v", err)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("waiting finalizer did not exit")
			}
			if err := n.renewLease(context.Background(), id); !errors.Is(err, ErrConflict) {
				t.Fatalf("renewal ignored terminal/owner state: %v", err)
			}
			v, _ := s.nodeTestStore.Get(context.Background(), id)
			if v.Job.Stats.Rows != 0 {
				t.Fatal("failed finalization changed result statistics")
			}
		})
	}
}

func TestNodeExpiredRenewalWaitDoesNotWriteLater(t *testing.T) {
	n, s, id := leaseTestNode()
	unlock, err := n.lockLeaseMutation(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := n.renewLease(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
		unlock()
		t.Fatalf("renewal wait ignored context: %v", err)
	}
	unlock()
	if s.reads.Load() != 0 || s.writes.Load() != 0 {
		t.Fatal("expired renewal accessed durable state")
	}
	if err := n.renewLease(context.Background(), id); err != nil {
		t.Fatal(err)
	}
}

type observedGateContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *observedGateContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestNodeLeaseGateRevalidatesReservationLifetime(t *testing.T) {
	for _, change := range []string{"removed", "replaced", "done", "cancelled"} {
		t.Run(change, func(t *testing.T) {
			n, s, id := leaseTestNode()
			r := n.jobs[id]
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r.ctx = ctx
			unlock, err := n.lockLeaseMutation(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			finished := make(chan error, 1)
			waitctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			observed := &observedGateContext{Context: waitctx, entered: make(chan struct{})}
			go func() {
				release, err := n.lockLeaseMutation(observed, id)
				if release != nil {
					release()
				}
				finished <- err
			}()
			select {
			case <-observed.entered:
			case <-waitctx.Done():
				unlock()
				t.Fatal("reservation gate wait did not begin")
			}
			n.mu.Lock()
			switch change {
			case "removed":
				delete(n.jobs, id)
			case "replaced":
				n.jobs[id] = &reservation{ctx: ctx, done: make(chan struct{})}
			case "done":
				close(r.done)
			case "cancelled":
				cancel()
			}
			n.mu.Unlock()
			unlock()
			select {
			case err := <-finished:
				if !errors.Is(err, ErrConflict) && !errors.Is(err, context.Canceled) {
					t.Fatalf("reservation lifetime ignored: %v", err)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("finalizer remained blocked")
			}
			if s.reads.Load() != 0 || s.writes.Load() != 0 {
				t.Fatal("ended reservation accessed durable state")
			}
		})
	}
}
