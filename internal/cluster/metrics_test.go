// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

func diagnosticsNode(t *testing.T) *Node {
	t.Helper()
	pool, err := admission.New(admission.Limits{MaxConcurrent: 2, MemoryBytes: 1024, ScratchBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	return &Node{ctx: context.Background(), cfg: NodeConfig{RuntimeMetrics: telemetry.New(), RuntimeResources: pool}}
}

func TestNodeDiagnosticsRequireVerifiedGateway(t *testing.T) {
	n := diagnosticsNode(t)
	uri, _ := url.Parse(WorkerIdentity("private-tenant", "private-worker"))
	wrong := &x509.Certificate{URIs: []*url.URL{uri}}
	valid := nodeRequest(http.MethodGet, "/metrics").TLS.PeerCertificates[0]
	for _, path := range []string{"/metrics", "/resources"} {
		for name, state := range map[string]*tls.ConnectionState{
			"plaintext":       nil,
			"unverified":      {PeerCertificates: []*x509.Certificate{valid}},
			"no peer":         {VerifiedChains: [][]*x509.Certificate{{valid}}},
			"nil peer":        {VerifiedChains: [][]*x509.Certificate{{valid}}, PeerCertificates: []*x509.Certificate{nil}},
			"worker identity": {VerifiedChains: [][]*x509.Certificate{{wrong}}, PeerCertificates: []*x509.Certificate{wrong}},
		} {
			t.Run(path+name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, path, nil)
				req.TLS = state
				w := httptest.NewRecorder()
				n.ServeHTTP(w, req)
				if w.Code != http.StatusUnauthorized {
					t.Fatalf("diagnostics exposed: %d", w.Code)
				}
				if strings.Contains(w.Body.String(), "kelvo_jobs") || strings.Contains(w.Body.String(), "MemoryBytes") {
					t.Fatal("unauthorized response leaked diagnostics")
				}
			})
		}
	}
}

func TestNodeResourcesSnapshotAndMethods(t *testing.T) {
	n := diagnosticsNode(t)
	reservation, err := n.cfg.RuntimeResources.TryAcquire(admission.Request{MemoryBytes: 256, ScratchBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()
	n.cfg.RuntimeResources.Drain()
	// Diagnostics remain available after execution cancellation, for cleanup.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n.ctx = ctx
	w := httptest.NewRecorder()
	n.ServeHTTP(w, nodeRequest(http.MethodGet, "/resources"))
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("response: %+v", w.Result())
	}
	var snapshot admission.Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot != n.cfg.RuntimeResources.Snapshot() {
		t.Fatalf("incorrect snapshot: %+v", snapshot)
	}
	w = httptest.NewRecorder()
	n.ServeHTTP(w, nodeRequest(http.MethodPost, "/resources"))
	if w.Code != 405 || w.Header().Get("Allow") != "GET" {
		t.Fatal("mutation method accepted")
	}
}

func TestNodeMetricsFixedLabels(t *testing.T) {
	n := diagnosticsNode(t)
	n.cfg.Policy.TenantID = "private-tenant"
	n.cfg.WorkerID = "private-worker"
	n.cfg.RuntimeMetrics.Observe(telemetry.KindQuery, telemetry.OutcomeSuccess, time.Second, 2*time.Second)
	req := nodeRequest(http.MethodGet, "/metrics?query_id=private-query&sql=secret-sql")
	req.Header.Set("Authorization", "Bearer secret-token")
	w := httptest.NewRecorder()
	n.ServeHTTP(w, req)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "kelvo_jobs_completed_total{kind=\"query\",outcome=\"success\"} 1") {
		t.Fatalf("invalid metrics: %s", w.Body.String())
	}
	for _, secret := range []string{"private-tenant", "private-worker", "private-query", "secret-sql", "secret-token", "query_id="} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("unbounded label or secret leaked: %s", secret)
		}
	}
	w = httptest.NewRecorder()
	n.ServeHTTP(w, nodeRequest(http.MethodHead, "/metrics"))
	if w.Code != 200 || w.Body.Len() != 0 {
		t.Fatal("HEAD metrics failed")
	}
}

func TestNodeDiagnosticsDisabled(t *testing.T) {
	n := &Node{ctx: context.Background()}
	for _, path := range []string{"/metrics", "/resources"} {
		w := httptest.NewRecorder()
		n.ServeHTTP(w, nodeRequest(http.MethodGet, path))
		if w.Code != 404 {
			t.Fatalf("disabled %s returned %d", path, w.Code)
		}
	}
}
