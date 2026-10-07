//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// The proxy drops bytes without closing TCP. This prevents the NATS client
// from escaping the short outage through discovered peers before fencing.
// Only this test connection is affected; no broker or shared service stops.
type coordinationOutageProxy struct {
	listener net.Listener
	upstream string
	paused   atomic.Bool
	mu       sync.Mutex
	closed   bool
	peers    []net.Conn
	wg       sync.WaitGroup
}

func newCoordinationOutageProxy(t *testing.T, upstream string) *coordinationOutageProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &coordinationOutageProxy{listener: listener, upstream: upstream}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.DialTimeout("tcp", upstream, time.Second)
			if err != nil {
				_ = client.Close()
				continue
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				_ = client.Close()
				_ = server.Close()
				return
			}
			p.peers = append(p.peers, client, server)
			p.wg.Add(2)
			p.mu.Unlock()
			go p.relay(client, server)
			go p.relay(server, client)
		}
	}()
	t.Cleanup(p.close)
	return p
}

func (p *coordinationOutageProxy) relay(source, destination net.Conn) {
	defer p.wg.Done()
	defer source.Close()
	defer destination.Close()
	buffer := make([]byte, 32<<10)
	for {
		n, err := source.Read(buffer)
		if n > 0 && !p.paused.Load() {
			written, writeErr := destination.Write(buffer[:n])
			if writeErr != nil || written != n {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *coordinationOutageProxy) close() {
	_ = p.listener.Close()
	p.mu.Lock()
	p.closed = true
	for _, peer := range p.peers {
		_ = peer.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

type coordinationOutageStore struct {
	*NATSStore
	renewed chan struct{}
	once    sync.Once
}

func (s *coordinationOutageStore) HeartbeatWorker(ctx context.Context, id, owner string) error {
	err := s.NATSStore.HeartbeatWorker(ctx, id, owner)
	if err == nil {
		s.once.Do(func() { close(s.renewed) })
	}
	return err
}

func TestNATSWorkerOutageFencesOwnerAndRecoversOnlyWithNewOwner(t *testing.T) {
	if os.Getenv("KELVO_TEST_COORDINATION_OUTAGE") != "1" {
		t.Skip("set KELVO_TEST_COORDINATION_OUTAGE=1 with an isolated store-test account")
	}
	cfg, policy := fixtureConfig(t)
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") || u.Port() == "" {
		t.Fatal("outage fixture requires a loopback NATS endpoint")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	proxy := newCoordinationOutageProxy(t, u.Host)
	u.Host = proxy.listener.Addr().String()
	cfg.URL = u.String()
	base, err := OpenStore(ctx, cfg, policy, true)
	if err != nil {
		t.Fatal("isolated NATS fixture initialization failed")
	}
	defer base.Close()
	store := &coordinationOutageStore{NATSStore: base, renewed: make(chan struct{})}
	node, err := newNode(NodeConfig{Policy: policy, WorkerID: "worker-a"}, store, probeExecutor{})
	if err != nil {
		t.Fatal("isolated worker startup failed")
	}
	defer node.Close()
	select {
	case <-store.renewed:
	case <-ctx.Done():
		t.Fatal("healthy worker did not renew")
	}
	proxy.paused.Store(true)
	select {
	case <-node.LeaseFailure():
	case <-ctx.Done():
		t.Fatal("broker outage did not permanently fence worker")
	}
	failure := node.LeaseFailureCause()
	if failure.Stage != "worker_renew_read" && failure.Stage != "worker_renew_write" {
		t.Fatalf("missing renewal stage: %s", failure.Diagnostic())
	}
	if failure.Reason != "deadline_exceeded" && failure.Reason != "timeout" {
		t.Fatalf("missing bounded outage cause: %s", failure.Diagnostic())
	}
	recorder := httptest.NewRecorder()
	node.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatal("fenced worker remained ready")
	}
	if err := node.Close(); err != nil {
		t.Fatal("fenced worker did not close")
	}
	_ = base.Close() // Discard the broken connection, never replay its buffered requests.
	proxy.paused.Store(false)
	recovered, err := OpenStore(ctx, cfg, policy, false)
	if err != nil {
		t.Fatal("fixture connection did not recover")
	}
	defer recovered.Close()
	// Let any successfully persisted last heartbeat expire. The old owner stays
	// fenced; a new process identity may claim only after this existing bound.
	select {
	case <-time.After(policy.LeaseDuration + 100*time.Millisecond):
	case <-ctx.Done():
		t.Fatal("fixture recovery deadline exceeded")
	}
	queued, err := recovered.Submit(ctx, query.Request{Mode: "federated", SQL: "SELECT 1"})
	if err != nil {
		t.Fatal("fresh isolated account has no admission capacity")
	}
	if err := recovered.Enqueue(ctx, queued.Job.ID); err != nil {
		t.Fatal("could not enqueue the fixture read")
	}
	before, err := recovered.Get(ctx, queued.Job.ID)
	if err != nil || before.Job.State != Queued || node.LeaseFailureCause() != failure {
		t.Fatal("failed owner resumed or lost its permanent failure")
	}
	replacement, err := newNode(NodeConfig{Policy: policy, WorkerID: "worker-a"}, recovered, probeExecutor{})
	if err != nil {
		t.Fatal("new owner could not reclaim an expired fixture lease")
	}
	defer replacement.Close()
	if replacement.owner == node.owner {
		t.Fatal("recovery reused the fenced owner")
	}
	waitFor(t, func() bool {
		value, err := recovered.Get(ctx, queued.Job.ID)
		return err == nil && value.Job.State == Assigned && value.Job.Owner == replacement.owner
	})
	if err := recovered.HeartbeatWorker(ctx, "worker-a", node.owner); !errors.Is(err, ErrConflict) {
		t.Fatal("old owner regained lease after replacement")
	}
}
