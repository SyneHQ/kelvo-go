// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type clientGate struct {
	ready chan struct{}
	once  sync.Once
}

func newClientGate() *clientGate { return &clientGate{ready: make(chan struct{})} }
func (g *clientGate) open()      { g.once.Do(func() { close(g.ready) }) }

type clientTestBody struct {
	reader       *bytes.Reader
	readEntered  *clientGate
	readAllowed  *clientGate
	closeEntered *clientGate
	closeAllowed *clientGate
	closeDone    *clientGate
	reads        atomic.Int32
	closes       atomic.Int32
	closeErr     error
}

func newClientTestBody() *clientTestBody {
	return &clientTestBody{reader: bytes.NewReader([]byte("data")), readEntered: newClientGate(), readAllowed: newClientGate(), closeEntered: newClientGate(), closeAllowed: newClientGate(), closeDone: newClientGate()}
}

func (b *clientTestBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	b.readEntered.open()
	<-b.readAllowed.ready
	return b.reader.Read(p)
}

func (b *clientTestBody) Close() error {
	b.closes.Add(1)
	b.closeEntered.open()
	<-b.closeAllowed.ready
	b.closeDone.open()
	return b.closeErr
}

func (b *clientTestBody) unblock() { b.readAllowed.open(); b.closeAllowed.open() }

func clientWait(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("client fixture did not reach its required boundary")
	}
}

func clientPending(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
		t.Fatal("object client returned before response ownership ended")
	case <-time.After(20 * time.Millisecond):
	}
}

func clientEventually(t *testing.T, condition func() bool) {
	t.Helper()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-timeout.C:
			t.Fatal("client fixture did not reach its required state")
		case <-tick.C:
		}
	}
}

func TestClientBodyJoinsQueuedReadsAndCancellationCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	upstream := newClientTestBody()
	upstream.closeErr = errors.New("fixture close failed")
	completed := make(chan struct{})
	body := newClientBody(ctx, upstream, func() { close(completed) })
	var joins []<-chan struct{}
	t.Cleanup(func() {
		upstream.unblock()
		cancel()
		_ = body.Close()
		uploadCleanupJoins(t, joins...)
	})
	first, second, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() { defer close(first); _, _ = body.Read(make([]byte, 1)) }()
	joins = append(joins, first)
	clientWait(t, upstream.readEntered.ready)
	go func() {
		defer close(second)
		if _, err := body.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
			t.Error("queued read reached a sealed body")
		}
	}()
	joins = append(joins, second)
	clientEventually(t, func() bool { body.mu.Lock(); defer body.mu.Unlock(); return body.reads == 2 })
	cancel()
	clientWait(t, upstream.closeEntered.ready)
	if _, err := body.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("cancellation did not seal new reads before provider Close returned")
	}
	go func() {
		defer close(closed)
		if !errors.Is(body.Close(), upstream.closeErr) {
			t.Error("body lost the once-only provider Close result")
		}
	}()
	joins = append(joins, closed)
	clientPending(t, closed)
	upstream.closeAllowed.open()
	clientWait(t, upstream.closeDone.ready)
	clientPending(t, closed)
	clientPending(t, body.callbackDone)
	upstream.readAllowed.open()
	clientWait(t, first)
	clientWait(t, second)
	clientWait(t, closed)
	clientWait(t, completed)
	clientWait(t, body.callbackDone)
	if upstream.reads.Load() != 1 || upstream.closes.Load() != 1 || !errors.Is(body.Close(), upstream.closeErr) {
		t.Fatal("read/Close serialization or sticky completion changed")
	}
}

func TestClientLifetimeRetainsEOFBodyUntilExplicitClose(t *testing.T) {
	var lifetime clientLifetime
	op, err := lifetime.begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	body, err := op.returnBody(io.NopCloser(bytes.NewReader(nil)))
	if err != nil {
		t.Fatal(err)
	}
	op.finish(true)
	if _, err := body.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatal("fixture did not reach EOF")
	}
	lifetime.mu.Lock()
	active := len(lifetime.active)
	lifetime.mu.Unlock()
	if active != 1 {
		t.Fatal("EOF released a body without closure proof")
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	clientWait(t, op.done)
	var idle atomic.Int32
	lifetime.close(func() { idle.Add(1) })
	lifetime.close(func() { idle.Add(1) })
	if idle.Load() != 1 {
		t.Fatal("transport idle cleanup was not once-only")
	}
	if _, err := lifetime.begin(context.Background()); !errors.Is(err, errClientClosed) {
		t.Fatal("closed client admitted an operation")
	}
}

func TestClientLifetimeAdoptsLateBodyDuringClose(t *testing.T) {
	var lifetime clientLifetime
	op, err := lifetime.begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	upstream := newClientTestBody()
	closed, attached := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { upstream.unblock(); uploadCleanupJoins(t, closed, attached) })
	go func() { lifetime.close(func() {}); close(closed) }()
	clientWait(t, op.ctx.Done())
	go func() {
		defer close(attached)
		defer op.finish(true)
		body, err := op.returnBody(upstream)
		if body != nil || !errors.Is(err, context.Canceled) {
			t.Error("late response escaped canceled operation ownership")
		}
	}()
	clientWait(t, upstream.closeEntered.ready)
	clientPending(t, closed)
	upstream.unblock()
	clientWait(t, attached)
	clientWait(t, closed)
	if upstream.closes.Load() != 1 {
		t.Fatal("late response body was not closed exactly once")
	}
}

func TestClientLifetimeDoesNotUseConnectionLimitAsAdmissionCap(t *testing.T) {
	var lifetime clientLifetime
	var operations []*clientOperation
	for range 12 {
		op, err := lifetime.begin(context.Background())
		if err != nil {
			t.Fatal("lifetime added an implicit operation cap")
		}
		operations = append(operations, op)
	}
	closed := make(chan struct{})
	go func() { lifetime.close(func() {}); close(closed) }()
	for _, op := range operations {
		clientWait(t, op.ctx.Done())
	}
	for _, op := range operations {
		op.finish(true)
	}
	clientWait(t, closed)
	lifetime.mu.Lock()
	defer lifetime.mu.Unlock()
	if len(lifetime.active) != 0 {
		t.Fatal("completed operation metadata remained attached")
	}
}
