//go:build linux

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

// Pause only the first selected transition, outside the underlying store lock.
// The gateway remains able to perform a real legal renewal while assignment is
// holding its older revision. The same barrier also wraps the real NATS store.
type exportDispatchProbe struct {
	ExportStore
	target      string
	entered     chan ExportSnapshot
	resume      chan struct{}
	decisions   chan exportDispatchDecision
	deliveries  chan time.Time
	gate        atomic.Bool
	conflicts   atomic.Int32
	assignments atomic.Int32
	before      func() error
	redeliver   func(context.Context, Delivery) error
}

func newExportDispatchProbe(store ExportStore, target string) *exportDispatchProbe {
	return &exportDispatchProbe{ExportStore: store, target: target, entered: make(chan ExportSnapshot, 1),
		resume: make(chan struct{}), decisions: make(chan exportDispatchDecision, 16), deliveries: make(chan time.Time, 16)}
}

type exportDispatchDecision struct {
	action string
	at     time.Time
}

func (s *exportDispatchProbe) CompareAndSwapExport(ctx context.Context, old ExportSnapshot, next ExportJob) (ExportSnapshot, error) {
	if old.Job.State == ExportQueued && next.State == s.target && s.gate.CompareAndSwap(false, true) {
		select {
		case s.entered <- old:
		case <-ctx.Done():
			return ExportSnapshot{}, ctx.Err()
		}
		select {
		case <-s.resume:
		case <-ctx.Done():
			return ExportSnapshot{}, ctx.Err()
		}
		if s.before != nil {
			if err := s.before(); err != nil {
				return ExportSnapshot{}, err
			}
		}
	}
	result, err := s.ExportStore.CompareAndSwapExport(ctx, old, next)
	if errors.Is(err, ErrExportConflict) {
		s.conflicts.Add(1)
	}
	if err == nil && old.Job.State == ExportQueued && next.State == ExportAssigned {
		s.assignments.Add(1)
	}
	return result, err
}

func (s *exportDispatchProbe) NextExport(ctx context.Context) (Delivery, error) {
	d, err := s.ExportStore.NextExport(ctx)
	if err != nil {
		return nil, err
	}
	select {
	case s.deliveries <- time.Now():
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return exportDispatchObservedDelivery{Delivery: d, probe: s}, nil
}

type exportDispatchObservedDelivery struct {
	Delivery
	probe *exportDispatchProbe
}

func (d exportDispatchObservedDelivery) Ack(ctx context.Context) error {
	if err := d.Delivery.Ack(ctx); err != nil {
		return err
	}
	select {
	case d.probe.decisions <- exportDispatchDecision{"ack", time.Now()}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (d exportDispatchObservedDelivery) Retry(ctx context.Context) error {
	started := time.Now()
	if err := d.Delivery.Retry(ctx); err != nil {
		return err
	}
	select {
	case d.probe.decisions <- exportDispatchDecision{"retry", started}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if d.probe.redeliver != nil {
		return d.probe.redeliver(ctx, d.Delivery)
	}
	return nil
}

func openDispatchRuntime(t *testing.T, cfg NodeConfig, store ExportStore, calls *atomic.Int32) *ExportRuntime {
	t.Helper()
	pool, err := cfg.Resources.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	r, err := newExportRuntime(cfg, store, runtimeExportExecutor(func(ctx context.Context, q query.Request, sink query.Sink) (query.Stats, error) {
		calls.Add(1)
		return runtimeExportRows(ctx, q, sink)
	}), pool, exportTestOwner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}

func dispatchDecision(t *testing.T, probe *exportDispatchProbe, want string) time.Time {
	t.Helper()
	select {
	case got := <-probe.decisions:
		if got.action != want {
			t.Fatalf("dispatch decision = %s, want %s", got.action, want)
		}
		return got.at
	case <-time.After(3 * time.Second):
		t.Fatalf("dispatch did not %s", want)
	}
	return time.Time{}
}

func dispatchSnapshot(t *testing.T, probe *exportDispatchProbe) ExportSnapshot {
	t.Helper()
	select {
	case snapshot := <-probe.entered:
		return snapshot
	case <-time.After(3 * time.Second):
		t.Fatal("dispatch did not reach the CAS barrier")
		return ExportSnapshot{}
	}
}

func dispatchDeliveryTime(t *testing.T, ctx context.Context, probe *exportDispatchProbe) time.Time {
	t.Helper()
	select {
	case delivered := <-probe.deliveries:
		return delivered
	case <-ctx.Done():
		t.Fatal("dispatch delivery observation did not arrive", ctx.Err())
		return time.Time{}
	}
}

func dispatchAuthority(t *testing.T, ctx context.Context, p Policy) context.Context {
	t.Helper()
	auth := bareAuthenticator(t)
	auth.config.ReloadInterval = time.Minute
	if !auth.apply(principalKeySet(t, 1, map[string][]string{"reports": {exportGatewayReportsKey}}, map[string][]string{}), time.Now()) {
		t.Fatal("fixture authority rejected")
	}
	_, key, ok := auth.lookup(exportGatewayReportsKey)
	if !ok {
		t.Fatal("fixture authority unavailable")
	}
	a, ok := authorityForPrincipal(p, "reports")
	if !ok {
		t.Fatal("fixture principal unavailable")
	}
	ctx = context.WithValue(ctx, jobAuthorityKey{}, a)
	return context.WithValue(ctx, keyAuthorizationContext{}, keyAuthorization{auth, key})
}

func dispatchMemoryFixture(t *testing.T, target string, before func() error, age time.Duration) (*ExportRuntime, *exportRuntimeStore, *exportDispatchProbe, *atomic.Int32, context.Context) {
	t.Helper()
	cfg := runtimeExportConfigFixture(t)
	cfg.WorkerID = "a1"
	s := &exportRuntimeStore{p: cfg.Policy, jobs: make(map[string]ExportSnapshot), queue: make(chan Delivery, 16)}
	probe := newExportDispatchProbe(s, target)
	probe.before = before
	probe.redeliver = func(ctx context.Context, d Delivery) error {
		select {
		case s.queue <- d:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	calls := &atomic.Int32{}
	r := openDispatchRuntime(t, cfg, probe, calls)
	ctx := dispatchAuthority(t, context.Background(), cfg.Policy)
	now := time.Now().UTC().Add(-age)
	job, err := normalizeExportSubmission(ctx, cfg.Policy, ExportSubmission{Request: query.Request{Mode: "federated", SQL: "SELECT 7"},
		SupervisorOwner: exportTestOwner, AuthorityUntil: now.Add(cfg.Policy.LeaseDuration / 3)}, now)
	if err != nil {
		t.Fatal(err)
	}
	job.ID = exportGatewayID
	s.mu.Lock()
	s.jobs[job.ID] = ExportSnapshot{Job: job, Revision: 1}
	s.mu.Unlock()
	if err := s.EnqueueExport(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	return r, s, probe, calls, ctx
}

func TestExportDispatchRenewalConflictRetainsQueuedDelivery(t *testing.T) {
	r, s, probe, calls, ctx := dispatchMemoryFixture(t, ExportAssigned, nil, 0)
	var resume sync.Once
	defer resume.Do(func() { close(probe.resume) })
	stale := dispatchSnapshot(t, probe)
	g := &Gateway{exportOwner: exportTestOwner}
	if err := g.renewExportAuthority(ctx, s, stale); err != nil {
		t.Fatal("legal queued renewal", err)
	}
	renewed, err := s.GetExport(ctx, stale.Job.ID)
	if err != nil || renewed.Job.State != ExportQueued || renewed.Revision == stale.Revision || !renewed.Job.AuthorityUntil.After(stale.Job.AuthorityUntil) {
		t.Fatal("renewal did not retain Queued with a newer revision", err)
	}
	t.Log("KELVO_EXPORT_DISPATCH_CONTROL queued_authority_renewed")
	resume.Do(func() { close(probe.resume) })
	dispatchDecision(t, probe, "retry")
	dispatchDecision(t, probe, "ack")
	claimed := claimRuntimeExport(t, s, stale.Job.ID)
	if _, err := r.RunExport(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	readyRuntimeExport(t, s, stale.Job.ID)
	if err := s.EnqueueExport(ctx, stale.Job.ID); err != nil {
		t.Fatal(err)
	}
	dispatchDecision(t, probe, "ack")
	if probe.conflicts.Load() != 1 || probe.assignments.Load() != 1 || calls.Load() != 1 {
		t.Fatal("renewal lost work or a terminal duplicate replayed execution")
	}
	assertDispatchDrained(t, r)
}

func TestExportDispatchFailureCASRetriesUntilConfirmed(t *testing.T) {
	for _, cause := range []error{ErrExportConflict, errors.New("transient store unavailable")} {
		t.Run(cause.Error(), func(t *testing.T) {
			r, s, probe, calls, _ := dispatchMemoryFixture(t, ExportFailed, func() error { return cause }, 10*time.Second)
			close(probe.resume)
			dispatchDecision(t, probe, "retry")
			dispatchDecision(t, probe, "ack")
			got, err := s.GetExport(context.Background(), exportGatewayID)
			if err != nil || got.Job.State != ExportFailed || calls.Load() != 0 || probe.assignments.Load() != 0 {
				t.Fatal("failure was not durably confirmed without execution", err)
			}
			assertDispatchDrained(t, r)
		})
	}
}

func TestExportDispatchAssignmentConflictAcknowledgesObservedAssignment(t *testing.T) {
	r, s, probe, calls, ctx := dispatchMemoryFixture(t, ExportAssigned, nil, 0)
	var resume sync.Once
	defer resume.Do(func() { close(probe.resume) })
	stale := dispatchSnapshot(t, probe)
	next := stale.Job
	next.State, next.WorkerID, next.WorkerOwner = ExportAssigned, "a1", exportTestOwner
	if _, err := s.CompareAndSwapExport(ctx, stale, next); err != nil {
		t.Fatal(err)
	}
	resume.Do(func() { close(probe.resume) })
	dispatchDecision(t, probe, "retry")
	dispatchDecision(t, probe, "ack")
	if probe.conflicts.Load() != 1 || probe.assignments.Load() != 0 || calls.Load() != 0 {
		t.Fatal("observed assignment was reassigned or executed")
	}
	assertDispatchDrained(t, r)
}

func assertDispatchDrained(t *testing.T, r *ExportRuntime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Drain(ctx); err != nil {
		t.Fatal("dispatch capacity did not drain", err)
	}
	if len(r.permits) != 0 || r.pool.Snapshot().Active != 0 {
		t.Fatal("dispatch leaked a permit or admission reservation")
	}
}

func TestExportDeliveryRetryIsDelayedBoundedAndCancellationAware(t *testing.T) {
	for _, delay := range []time.Duration{time.Millisecond, 250 * time.Millisecond} {
		msg := &refreshTestMessage{}
		d := exportDelivery{delivery: delivery{id: exportGatewayID, msg: msg}, retryDelay: delay}
		if err := d.Retry(context.Background()); err != nil || msg.naks.Load() != 1 || msg.delay != delay || msg.acks.Load() != 0 {
			t.Fatal("retry did not request exactly one bounded delayed NAK", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := d.Retry(ctx); !errors.Is(err, context.Canceled) || msg.naks.Load() != 1 {
			t.Fatal("cancelled retry reached broker", err)
		}
	}
	for _, delay := range []time.Duration{-time.Second, 0, 251 * time.Millisecond} {
		msg := &refreshTestMessage{}
		d := exportDelivery{delivery: delivery{msg: msg}, retryDelay: delay}
		if err := d.Retry(context.Background()); err == nil || msg.naks.Load() != 0 {
			t.Fatal("invalid retry delay reached broker")
		}
	}
}
