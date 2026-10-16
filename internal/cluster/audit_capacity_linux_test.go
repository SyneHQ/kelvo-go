//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
)

// Against a real journal: a full pending queue answers 429 with a retry hint
// and admits nothing; once a reservation frees, the same request is admitted; a
// closed journal (a real fault) stays 503.
func TestGatewayAuditQueueFullIs429AndJournalFaultIs503(t *testing.T) {
	cfg := serviceAuditConfig(t)
	cfg.MaxPending = 1
	a, err := OpenServiceAudit(cfg, "gateway", []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.CloseBounded() })
	s := &auditedStore{nodeTestStore: &nodeTestStore{p: testPolicy(), jobs: map[string]Snapshot{}, queue: make(chan Delivery, 16)}}
	g := &Gateway{audit: a, tenants: map[string]gatewayTenant{"a": {store: s}}, tokens: map[[32]byte]string{sha256.Sum256([]byte(rotationOld)): "a"}, permits: make(chan struct{}, 4), resultWaiters: make(chan struct{}, 4), ctx: context.Background(), reconcileOK: map[string]bool{"a": true}}
	post := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		g.ServeHTTP(w, auditRequest(http.MethodPost, "/v1/queries", `{"mode":"federated","sql":"select 1"}`))
		return w
	}

	held, err := a.journal.Begin(context.Background(), a.binding("a", nil), audit.QuerySubmit) // the one reservation
	if err != nil {
		t.Fatal(err)
	}
	w := post()
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" || !strings.Contains(w.Body.String(), `"code":"RESOURCE_EXHAUSTED"`) || s.submitted.Load() != 0 {
		t.Fatalf("a full audit queue: status=%d retry=%q submitted=%d body=%s", w.Code, w.Header().Get("Retry-After"), s.submitted.Load(), w.Body.String())
	}
	if err := held.Finish(context.Background(), audit.Succeeded, audit.None); err != nil {
		t.Fatal(err)
	}
	if w := post(); w.Code != http.StatusCreated {
		t.Fatalf("after the reservation freed: status=%d body=%s", w.Code, w.Body.String())
	}

	if err := a.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	w = post()
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "" || !strings.Contains(w.Body.String(), "Audit storage unavailable") {
		t.Fatalf("a closed journal: status=%d retry=%q body=%s", w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
}
