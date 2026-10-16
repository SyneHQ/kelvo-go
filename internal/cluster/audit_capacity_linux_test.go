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

// A node whose audit queue is full refuses a result request BEFORE it consumes
// the claim or touches the job, so the retry the 429 invites can succeed.
func TestAuditNodeFullQueueRefusesBeforeConsumingTheClaimThenRetrySucceeds(t *testing.T) {
	cfg := serviceAuditConfig(t)
	cfg.MaxPending = 1
	store := &auditedStore{nodeTestStore: &nodeTestStore{p: testPolicy(), jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}}
	executor := &auditSource{}
	n, err := newNode(NodeConfig{Policy: store.p, WorkerID: "a1", Audit: cfg}, store, executor)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	store.add(id)
	waitFor(t, func() bool { v, _ := store.Get(context.Background(), id); return v.Job.State == Assigned })
	old, _ := store.Get(context.Background(), id)
	claimed := old.Job
	claimed.State, claimed.Claim = Claimed, strings.Repeat("b", 32)
	if _, err = store.CompareAndSwap(context.Background(), old, claimed); err != nil {
		t.Fatal(err)
	}
	request := func() *httptest.ResponseRecorder {
		r := auditNodeRequest("/internal/queries/" + id + "/results")
		r.Header.Set("X-Kelvo-Claim", claimed.Claim)
		w := httptest.NewRecorder()
		n.ServeHTTP(w, r)
		return w
	}

	held, err := n.audit.journal.Begin(context.Background(), n.audit.binding("a", nil), audit.QueryExecution) // the one reservation
	if err != nil {
		t.Fatal(err)
	}
	w := request()
	after, _ := store.Get(context.Background(), id)
	n.mu.Lock()
	reserved, started := n.jobs[id], n.jobs[id] != nil && n.jobs[id].started
	n.mu.Unlock()
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("full queue: status=%d retry=%q", w.Code, w.Header().Get("Retry-After"))
	}
	if after.Job.State != Claimed || after.Job.Claim != claimed.Claim || reserved == nil || started || executor.calls.Load() != 0 {
		t.Fatalf("the refusal changed the job: state=%s reservation=%v started=%v executions=%d", after.Job.State, reserved != nil, started, executor.calls.Load())
	}

	if err := held.Finish(context.Background(), audit.Succeeded, audit.None); err != nil {
		t.Fatal(err)
	}
	if w := request(); w.Code != http.StatusOK || executor.calls.Load() != 1 {
		t.Fatalf("retry after the slot freed: status=%d executions=%d", w.Code, executor.calls.Load())
	}
	done, _ := store.Get(context.Background(), id)
	if done.Job.State != ResultReady {
		t.Fatalf("retry did not publish the result: %s", done.Job.State)
	}
}

// Cancel on a node with a full audit queue is capacity too, and cancels nothing.
func TestAuditNodeCancelWithFullQueueIs429AndCancelsNothing(t *testing.T) {
	cfg := serviceAuditConfig(t)
	cfg.MaxPending = 1
	store := &auditedStore{nodeTestStore: &nodeTestStore{p: testPolicy(), jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}}
	n, err := newNode(NodeConfig{Policy: store.p, WorkerID: "a1", Audit: cfg}, store, &auditSource{})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	store.add(id)
	waitFor(t, func() bool { v, _ := store.Get(context.Background(), id); return v.Job.State == Assigned })
	held, err := n.audit.journal.Begin(context.Background(), n.audit.binding("a", nil), audit.QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Finish(context.Background(), audit.Succeeded, audit.None)
	r := auditNodeRequest("/internal/queries/" + id + "/cancel")
	r.Method = http.MethodPost
	w := httptest.NewRecorder()
	n.ServeHTTP(w, r)
	got, _ := store.Get(context.Background(), id)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" || got.Job.State != Assigned {
		t.Fatalf("status=%d retry=%q state=%s", w.Code, w.Header().Get("Retry-After"), got.Job.State)
	}
}
