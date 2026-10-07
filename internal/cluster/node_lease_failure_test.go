// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type failingWorkerLeaseStore struct {
	*nodeTestStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
	failure error
}

func (s *failingWorkerLeaseStore) HeartbeatWorker(ctx context.Context, _, _ string) error {
	s.calls.Add(1)
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		if s.failure != nil {
			return s.failure
		}
		return errors.New("private broker detail must not escape")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func leaseFailureFixture(t *testing.T) (*Node, *failingWorkerLeaseStore) {
	t.Helper()
	store := &failingWorkerLeaseStore{
		nodeTestStore: &nodeTestStore{p: testPolicy(), jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)},
		entered:       make(chan struct{}), release: make(chan struct{}),
	}
	node, err := newNode(NodeConfig{Policy: store.p, WorkerID: "a1"}, store, probeExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	})
	return node, store
}

func TestNodeLeaseFailureFencesPermanentlyAndNotifiesLateSubscriber(t *testing.T) {
	node, store := leaseFailureFixture(t)
	const first = "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const queued = "1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store.add(first)
	waitFor(t, func() bool { s, _ := store.Get(context.Background(), first); return s.Job.State == Assigned })
	select {
	case <-store.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("renewal did not start")
	}
	close(store.release)
	select {
	case <-node.LeaseFailure():
	case <-time.After(time.Second):
		t.Fatal("permanent lease failure not signalled")
	}
	if node.ctx.Err() == nil {
		t.Fatal("failure notification preceded fencing")
	}
	if got := node.LeaseFailureCause(); got.Stage != "worker_renew" || got.Reason != "other" {
		t.Fatalf("unknown provider failure was not safely classified: %+v", got)
	}
	select {
	case <-node.dispatchDone:
	case <-time.After(time.Second):
		t.Fatal("fenced owner still dispatches")
	}
	store.add(queued)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Get(context.Background(), queued)
	if state.Job.State != Queued || store.calls.Load() != 1 {
		t.Fatal("failed owner resumed or retried")
	}
	// Observers attached after shutdown must still see the original failure.
	select {
	case <-node.LeaseFailure():
	default:
		t.Fatal("late subscriber lost permanent failure")
	}
}

func TestNodeNormalCloseDuringRenewalDoesNotSignalLeaseFailure(t *testing.T) {
	node, store := leaseFailureFixture(t)
	select {
	case <-store.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("renewal did not start")
	}
	node.BeginDrain()
	select {
	case <-node.LeaseFailure():
		t.Fatal("normal drain became a lease failure")
	default:
	}
	// Close cancels an in-flight store call; its context error is shutdown,
	// rather than an independent loss of coordination ownership.
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-node.LeaseFailure():
		t.Fatal("ordinary cancellation became lease failure")
	default:
	}
	if node.LeaseFailureCause() != (CoordinationFailure{}) {
		t.Fatal("normal close manufactured a lease failure cause")
	}
}

func TestNodeLeaseFailureCauseIsImmutableAndRedacted(t *testing.T) {
	node, store := leaseFailureFixture(t)
	store.failure = fmt.Errorf("private broker URI and token: %w", coordinationUnavailable("worker_renew_write", context.DeadlineExceeded))
	select {
	case <-store.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("renewal did not start")
	}
	close(store.release)
	select {
	case <-node.LeaseFailure():
	case <-time.After(time.Second):
		t.Fatal("renewal failure did not fence owner")
	}
	got := node.LeaseFailureCause()
	if got.Stage != "worker_renew_write" || got.Reason != "deadline_exceeded" || strings.Contains(got.Diagnostic(), "private") || node.ctx.Err() == nil {
		t.Fatalf("lease cause was lost or exposed: %+v", got)
	}
	got.Reason = "changed"
	if node.LeaseFailureCause().Reason != "deadline_exceeded" {
		t.Fatal("caller changed retained failure evidence")
	}
}
