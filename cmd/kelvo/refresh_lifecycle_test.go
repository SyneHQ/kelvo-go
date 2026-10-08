// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/operationrun"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

type lifecycleTestNode struct{ entered, release chan struct{} }

func (*lifecycleTestNode) ServeHTTP(http.ResponseWriter, *http.Request) {}
func (*lifecycleTestNode) BeginDrain()                                  {}
func (n *lifecycleTestNode) Drain(ctx context.Context) error {
	close(n.entered)
	select {
	case <-n.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type failedDrainFixture struct {
	failure         error
	waitForDeadline bool
}

func (h failedDrainFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }
func (h failedDrainFixture) Drain(ctx context.Context) error {
	if h.waitForDeadline {
		<-ctx.Done()
		return errors.Join(ctx.Err(), h.failure)
	}
	return h.failure
}

func TestServeClusterPreservesLifecycleFailuresThroughShutdown(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure error
		wait    bool
		want    error
	}{
		{"clean", nil, false, nil},
		{"grace expires", nil, true, nil},
		{"delivery failure", operationrun.ErrDelivery, false, operationrun.ErrDelivery},
		{"delivery failure with grace expiry", operationrun.ErrDelivery, true, operationrun.ErrDelivery},
		{"joined server closed", errors.Join(http.ErrServerClosed, operationrun.ErrCleanup), false, operationrun.ErrCleanup},
	} {
		t.Run(test.name, func(t *testing.T) {
			template := httptest.NewTLSServer(http.NotFoundHandler())
			tc := template.TLS.Clone()
			template.Close()
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			closed := false
			err = serveCluster(ctx, ln, tc, failedDrainFixture{test.failure, test.wait}, func() { closed = true }, 20*time.Millisecond)
			if !errors.Is(err, test.want) || !closed {
				t.Fatal("shutdown lost its lifecycle failure or skipped close", err, closed)
			}
		})
	}
}
func TestNodeLifecycleKeepsAcceptedWorkAlive(t *testing.T) {
	for _, withPool := range []bool{false, true} {
		t.Run(map[bool]string{false: "without_resources", true: "with_resources"}[withPool], func(t *testing.T) {
			gate := newRefreshGate()
			release, err := gate.Enter()
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			node := &lifecycleTestNode{make(chan struct{}), make(chan struct{})}
			var pool *admission.Pool
			if withPool {
				pool, err = admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 10})
				if err != nil {
					t.Fatal(err)
				}
			}
			stopped := make(chan struct{})
			l := &nodeLifecycle{Node: node, pool: pool, refresh: gate, stopRefresh: func() { close(stopped) }}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- l.Drain(ctx) }()
			<-node.entered
			if _, err := gate.Enter(); !errors.Is(err, admission.ErrDraining) {
				t.Fatalf("refresh admission during drain: %v", err)
			}
			if pool != nil {
				r, err := pool.TryAcquire(admission.Request{MemoryBytes: 1})
				if err != nil {
					t.Fatalf("accepted query cannot acquire during grace: %v", err)
				}
				r.Release()
			}
			close(node.release)
			select {
			case <-stopped:
				t.Fatal("active refresh canceled before grace")
			case <-time.After(20 * time.Millisecond):
			}
			release()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("drain did not complete")
			}
			select {
			case <-stopped:
			default:
				t.Fatal("refresh not stopped after graceful completion")
			}
			if pool != nil && !pool.Snapshot().Draining {
				t.Fatal("pool not drained after accepted work completed")
			}
		})
	}
}
func TestNodeLifecycleDeadlineCancelsRefresh(t *testing.T) {
	gate := newRefreshGate()
	release, _ := gate.Enter()
	defer release()
	node := &lifecycleTestNode{make(chan struct{}), make(chan struct{})}
	stopped := false
	l := &nodeLifecycle{Node: node, refresh: gate, stopRefresh: func() { stopped = true }}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := l.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain: %v", err)
	}
	if !stopped {
		t.Fatal("deadline did not cancel refresh")
	}
}
func TestRefreshReservationCoversPublicationAndCancellationMetrics(t *testing.T) {
	pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	limits := query.Limits{MemoryMB: 1, Timeout: time.Second}
	metrics := telemetry.New()
	err = withRefreshReservation(context.Background(), pool, 0, limits, metrics, func(context.Context) error {
		if pool.Snapshot().Active != 1 {
			t.Fatal("publication callback lacks reservation")
		}
		return nil
	})
	if err != nil || pool.Snapshot().Active != 0 {
		t.Fatalf("reservation cleanup: %v", err)
	}
	held, err := pool.TryAcquire(admission.Request{MemoryBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = withRefreshReservation(ctx, pool, 0, limits, metrics, func(context.Context) error { t.Fatal("canceled waiter executed"); return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	snapshot := metrics.Snapshot()
	if snapshot.Outcomes[telemetry.KindRefresh][telemetry.OutcomeCanceled] != 1 {
		t.Fatal("cancellation missing from outcomes")
	}
	if snapshot.Rejections[telemetry.KindRefresh][telemetry.RejectionCapacity] != 0 {
		t.Fatal("cancellation misreported as capacity rejection")
	}
	if snapshot.QueueWait[telemetry.KindRefresh].SumSeconds < 0.01 {
		t.Fatal("admission wait not recorded")
	}
	if snapshot.Duration[telemetry.KindRefresh].SumSeconds >= snapshot.QueueWait[telemetry.KindRefresh].SumSeconds {
		t.Fatal("execution duration includes queue wait")
	}
}

// Validate the HTTP boundary too: cancellation of the process signal must start
// grace, rather than immediately canceling in-flight result requests.
type httpDrainFixture struct{ started, draining, finish, canceled chan struct{} }

func (h *httpDrainFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/wait" {
		close(h.started)
		<-r.Context().Done()
		close(h.canceled)
	}
	w.WriteHeader(http.StatusOK)
}
func (h *httpDrainFixture) Drain(ctx context.Context) error {
	close(h.draining)
	select {
	case <-h.finish:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestServeClusterSignalPreservesHTTPDuringGrace(t *testing.T) {
	template := httptest.NewTLSServer(http.NotFoundHandler())
	tc, client := template.TLS.Clone(), template.Client()
	template.Close()
	defer client.CloseIdleConnections()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	h := &httpDrainFixture{make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var finish sync.Once
	defer finish.Do(func() { close(h.finish) })
	done := make(chan error, 1)
	go func() { done <- serveCluster(ctx, ln, tc, h, func() {}, time.Second) }()
	resultDone := make(chan error, 1)
	go func() {
		r, e := client.Get("https://" + ln.Addr().String() + "/wait")
		if r != nil {
			r.Body.Close()
		}
		resultDone <- e
	}()
	select {
	case <-h.started:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP request did not start")
	}
	cancel()
	select {
	case <-h.draining:
	case <-time.After(time.Second):
		t.Fatal("signal did not begin grace")
	}
	select {
	case <-h.canceled:
		t.Fatal("signal canceled result before grace ended")
	default:
	}
	response, err := client.Get("https://" + ln.Addr().String() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("HTTP unavailable during grace")
	}
	finish.Do(func() { close(h.finish) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	select {
	case <-h.canceled:
	default:
		t.Fatal("request not canceled after grace")
	}
	select {
	case <-resultDone:
	case <-time.After(time.Second):
		t.Fatal("result HTTP request did not finish")
	}
}

func TestRefreshReservationWaitsForProcessCustodyAfterPublication(t *testing.T) {
	pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 2 << 30, ScratchBytes: 2 << 30})
	if err != nil {
		t.Fatal(err)
	}
	limits := query.DefaultLimits()
	limits.MemoryMB = 64
	limits.MaxTempMB = 16
	limits.MaxBytes = 1 << 20
	var releaseProcess func()
	err = withRefreshReservation(context.Background(), pool, 64<<20, limits, nil, func(ctx context.Context) error {
		custody := containment.FromContext(ctx)
		if custody == nil {
			t.Fatal("refresh did not carry custody to source executor")
		}
		releaseProcess, err = custody.Hold()
		if err != nil {
			t.Fatal(err)
		}
		if pool.Snapshot().Active != 1 {
			t.Fatal("reservation did not span publication")
		}
		return nil
	})
	if err != nil || pool.Snapshot().Active != 1 {
		t.Fatal("reservation released despite unresolved native child", err)
	}
	releaseProcess()
	if pool.Snapshot().Active != 0 {
		t.Fatal("verified process cleanup did not release reservation")
	}
}

func TestRefreshCleanupUncertaintyRetainsCapacityAndDrainsAdmission(t *testing.T) {
	pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 2 << 30, ScratchBytes: 2 << 30})
	if err != nil {
		t.Fatal(err)
	}
	limits := query.DefaultLimits()
	limits.MemoryMB, limits.MaxTempMB, limits.MaxBytes = 64, 16, 1<<20
	err = withRefreshReservation(context.Background(), pool, 64<<20, limits, nil, func(context.Context) error {
		return errors.Join(errors.New("fixture source failure"), acceleration.ErrRefreshCleanup)
	})
	if !errors.Is(err, acceleration.ErrRefreshCleanup) {
		t.Fatal("cleanup failure was hidden", err)
	}
	if got := pool.Snapshot(); got.Active != 1 || !got.Draining {
		t.Fatal("uncertain refresh released reusable capacity", got)
	}
	if _, err := pool.TryAcquire(admission.Request{MemoryBytes: 1}); !errors.Is(err, admission.ErrDraining) {
		t.Fatal("uncertain refresh admitted more work", err)
	}
}
