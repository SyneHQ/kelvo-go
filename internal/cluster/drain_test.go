// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func gatewayDrainFixture() *Gateway {
	p := testPolicy()
	store := &gatewayStore{policy: p, jobs: map[string]Snapshot{"existing": {Job: Job{ID: "existing", TenantID: "a", State: Queued}}}}
	return &Gateway{tenants: map[string]gatewayTenant{"a": {store: store}}, tokens: map[[32]byte]string{sha256.Sum256([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")): "a"}, permits: make(chan struct{}, 1), ctx: context.Background(), reconcileOK: map[string]bool{"a": true}}
}
func TestGatewayProbesBypassSaturatedRequests(t *testing.T) {
	g := gatewayDrainFixture()
	g.permits <- struct{}{}
	for _, path := range []string{"/health", "/ready"} {
		w := httptest.NewRecorder()
		g.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("%s under saturation: %d", path, w.Code)
		}
	}
	g.BeginDrain()
	for path, want := range map[string]int{"/health": 200, "/ready": 503} {
		w := httptest.NewRecorder()
		g.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != want {
			t.Fatalf("%s during drain: %d", path, w.Code)
		}
	}
}
func TestGatewayDrainPreservesHandlesRejectsSubmission(t *testing.T) {
	g := gatewayDrainFixture()
	g.BeginDrain()
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{"POST", "/v1/queries", 503}, {"GET", "/v1/queries/existing", 200},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		r.Header.Set("Authorization", "Bearer aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Drain(ctx) }()
	select {
	case <-done:
		t.Fatal("gateway drain returned before grace ended")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("gateway drain exceeded grace")
	}
}
func nodeRequest(method, path string) *http.Request {
	r := httptest.NewRequest(method, path, nil)
	uri, _ := url.Parse(GatewayIdentity)
	cert := &x509.Certificate{URIs: []*url.URL{uri}}
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}
	return r
}
func TestNodeDrainStopsDispatchAndRenewsAcceptedLease(t *testing.T) {
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
	before, _ := s.Get(context.Background(), id)
	n.BeginDrain()
	select {
	case <-n.dispatchDone:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not stop")
	}
	s.add("1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	for path, want := range map[string]int{"/health": 200, "/ready": 503} {
		w := httptest.NewRecorder()
		n.ServeHTTP(w, nodeRequest("GET", path))
		if w.Code != want {
			t.Fatalf("%s: %d", path, w.Code)
		}
	}
	waitFor(t, func() bool {
		v, _ := s.Get(context.Background(), id)
		return v.Job.HeartbeatAt.After(before.Job.HeartbeatAt)
	})
	queued, _ := s.Get(context.Background(), "1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if queued.Job.State != Queued {
		t.Fatal("assigned job during drain")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := n.Drain(ctx); err != context.DeadlineExceeded {
		t.Fatalf("pending reservation drain: %v", err)
	}
	if n.ctx.Err() != nil {
		t.Fatal("grace cancelled worker before forced shutdown")
	}
	w := httptest.NewRecorder()
	n.ServeHTTP(w, nodeRequest("POST", "/internal/queries/"+id+"/cancel"))
	if w.Code != 204 {
		t.Fatalf("cancel during drain: %d", w.Code)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := n.Drain(ctx2); err != nil {
		t.Fatal(err)
	}
}
