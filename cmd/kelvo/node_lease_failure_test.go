// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestLeaseFailureWatcherHandlesEarlyFailureAndOrdinaryCancellation(t *testing.T) {
	failure := make(chan struct{})
	close(failure)
	called := make(chan struct{})
	stop := watchLeaseFailure(context.Background(), failure, func() { close(called) })
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("early failure was lost")
	}
	stop()
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var unexpected atomic.Bool
		stop := watchLeaseFailure(ctx, failure, func() { unexpected.Store(true) })
		stop()
		if unexpected.Load() {
			t.Fatal("normal parent cancellation triggered failure callback")
		}
	}
	err := query.PublicError(workerLeaseFailure())
	if err.Code != "UNAVAILABLE" || err.Message != "Worker coordination lease lost" {
		t.Fatal("unsafe lease failure classification")
	}
}

func TestNodeLeaseFailureSkipsGraceAndCancelsRefresh(t *testing.T) {
	gate := newRefreshGate()
	release, err := gate.Enter()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := pool.TryAcquire(admission.Request{MemoryBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()
	node := &lifecycleTestNode{make(chan struct{}), make(chan struct{})}
	failure := make(chan struct{})
	refresh, cancelRefresh := context.WithCancel(context.Background())
	defer cancelRefresh()
	lifecycle := &nodeLifecycle{Node: node, pool: pool, refresh: gate, stopRefresh: cancelRefresh, leaseFailure: failure}
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- lifecycle.Drain(ctx) }()
	<-node.entered
	close(failure)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("lease loss did not cancel drain", err)
		}
	case <-time.After(time.Second):
		t.Fatal("permanent lease loss waited for ordinary grace")
	}
	if refresh.Err() == nil || !pool.Snapshot().Draining {
		t.Fatal("fenced refresh/admission remains active")
	}
}

func TestServeClusterLeaseFailureJoinsCleanupBeforeReturning(t *testing.T) {
	template := httptest.NewTLSServer(http.NotFoundHandler())
	tc, client := template.TLS.Clone(), template.Client()
	template.Close()
	defer client.CloseIdleConnections()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	node := &leaseFailureHTTPNode{&httpDrainFixture{make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})}}
	failure := make(chan struct{})
	refresh, cancelRefresh := context.WithCancel(context.Background())
	defer cancelRefresh()
	lifecycle := &nodeLifecycle{Node: node, refresh: newRefreshGate(), stopRefresh: cancelRefresh, leaseFailure: failure}
	run, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopWatch := watchLeaseFailure(run, failure, func() { lifecycle.fence(); cancel() })
	defer stopWatch()
	cleanupEntered, cleanupRelease := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(cleanupRelease) })
	done := make(chan error, 1)
	go func() {
		done <- serveCluster(run, ln, tc, lifecycle, func() { close(cleanupEntered); <-cleanupRelease }, 24*time.Hour)
	}()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		response, _ := client.Get("https://" + ln.Addr().String() + "/wait")
		if response != nil {
			response.Body.Close()
		}
	}()
	select {
	case <-node.started:
	case <-time.After(time.Second):
		t.Fatal("HTTP request did not start")
	}
	close(failure)
	select {
	case <-cleanupEntered:
	case <-time.After(time.Second):
		t.Fatal("fenced service did not start bounded cleanup")
	}
	select {
	case <-node.canceled:
	case <-time.After(time.Second):
		t.Fatal("fenced request was not canceled")
	}
	if refresh.Err() == nil {
		t.Fatal("refresh survived permanent node fencing")
	}
	select {
	case <-done:
		t.Fatal("service returned before owned cleanup joined")
	default:
	}
	release.Do(func() { close(cleanupRelease) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("service did not return after cleanup")
	}
	<-requestDone
}

type leaseFailureHTTPNode struct{ *httpDrainFixture }

func (*leaseFailureHTTPNode) BeginDrain() {}
