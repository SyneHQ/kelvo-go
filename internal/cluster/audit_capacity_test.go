// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// A full audit queue is capacity; a journal fault is an outage.
func TestAuditBeginFailureIsCapacityOnlyWhenTheQueueIsFull(t *testing.T) {
	g := &Gateway{}
	for _, c := range []struct {
		err    error
		status int
		retry  string
		code   string
	}{
		{audit.ErrBusy, http.StatusTooManyRequests, "1", "RESOURCE_EXHAUSTED"},
		{audit.ErrUncertain, http.StatusServiceUnavailable, "", "UNAVAILABLE"},
		{audit.ErrUnavailable, http.StatusServiceUnavailable, "", "UNAVAILABLE"},
		{audit.ErrCorrupt, http.StatusServiceUnavailable, "", "UNAVAILABLE"},
		{audit.ErrClosed, http.StatusServiceUnavailable, "", "UNAVAILABLE"},
		{audit.ErrFull, http.StatusServiceUnavailable, "", "UNAVAILABLE"},
		{errors.New("anything else"), http.StatusServiceUnavailable, "", "UNAVAILABLE"},
	} {
		w := httptest.NewRecorder()
		g.auditBeginFailed(w, c.err)
		if w.Code != c.status || w.Header().Get("Retry-After") != c.retry || !strings.Contains(w.Body.String(), `"code":"`+c.code+`"`) {
			t.Errorf("%v: status=%d retry=%q body=%s", c.err, w.Code, w.Header().Get("Retry-After"), w.Body.String())
		}
	}
}

func auditTestConfig(pending int) *ServiceAuditConfig {
	return &ServiceAuditConfig{ServiceID: "replica-one", Config: audit.Config{Directory: filepath.Join("/var/lib/kelvo", "audit"), MaxEntries: 4096, MaxPending: pending, Retention: time.Hour, WriteTimeout: 2 * time.Second}}
}

// An audit queue smaller than what the gateway admits would refuse accepted
// work; that is a static configuration error.
func TestGatewayAuditQueueMustCoverAdmittedRequests(t *testing.T) {
	cfg := GatewayConfig{MaxHTTPRequests: 64}
	for _, c := range []struct {
		audit *ServiceAuditConfig
		ok    bool
	}{{nil, true}, {auditTestConfig(64), true}, {auditTestConfig(4096), true}, {auditTestConfig(63), false}, {auditTestConfig(8), false}} {
		cfg.Audit = c.audit
		err := validateGatewayAudit(cfg)
		if (err == nil) != c.ok {
			t.Errorf("max_pending %v with max_http_requests 64: %v", c.audit, err)
		}
		if err != nil && !errors.Is(err, errAuditCapacity) {
			t.Errorf("not the static capacity error: %v", err)
		}
	}
	if strings.ContainsAny(errAuditCapacity.Error(), "0123456789") {
		t.Fatal("the capacity error must be static text, with no configured values")
	}
}

// A node needs a reservation per query its workers run and per concurrent
// operation.
func TestNodeAuditQueueMustCoverWorkersAndOperations(t *testing.T) {
	node := func(pending, workers, operations int) NodeConfig {
		p := testPolicy()
		p.Workers = map[string]int{"a1": workers}
		c := NodeConfig{Policy: p, WorkerID: "a1", Audit: auditTestConfig(pending)}
		if operations > 0 {
			c.Operations = &OperationNodeConfig{MaxConcurrent: operations}
		}
		return c
	}
	for _, c := range []struct {
		pending, workers, operations int
		ok                           bool
	}{
		{4, 4, 0, true}, {3, 4, 0, false},
		{7, 4, 3, true}, {6, 4, 3, false},
		{64, 16, 16, true}, {31, 16, 16, false},
	} {
		err := validateNodeAudit(node(c.pending, c.workers, c.operations))
		if (err == nil) != c.ok {
			t.Errorf("pending %d, workers %d, operations %d: %v", c.pending, c.workers, c.operations, err)
		}
		if err != nil && !errors.Is(err, errAuditCapacity) {
			t.Errorf("not the static capacity error: %v", err)
		}
	}
}

// A worker that refused because its own audit queue was full is capacity to the
// caller too: 429 with a retry hint, not a 503 outage.
func TestResultRelayMapsAWorkerAuditQueueRefusalTo429(t *testing.T) {
	store := &compressionRelayStore{
		gatewayStore: gatewayStore{policy: Policy{TenantID: "relay", Limits: query.DefaultLimits()}},
		snapshot: Snapshot{Revision: 1, Job: Job{ID: "busy-result", TenantID: "relay", State: Assigned,
			Owner: "worker-owner", WorkerID: "worker-one", ExpiresAt: time.Now().Add(time.Minute)}},
	}
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "audit queue full", http.StatusTooManyRequests)
	}))
	defer upstream.Close()
	endpoint, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	tenant := gatewayTenant{store: store, workers: map[string]workerEndpoint{"worker-one": {url: endpoint, client: upstream.Client()}}}
	w := httptest.NewRecorder()
	(&Gateway{}).results(w, httptest.NewRequest(http.MethodGet, "/v1/queries/busy-result/results", nil), tenant, "busy-result", nil)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" || !strings.Contains(w.Body.String(), `"code":"RESOURCE_EXHAUSTED"`) {
		t.Fatalf("status=%d retry=%q body=%s", w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
	// The retry the 429 invites needs an assignable job, not a consumed claim.
	got, _ := store.Get(context.Background(), "busy-result")
	if got.Job.State != Assigned || got.Job.Claim != "" {
		t.Fatalf("job after the refusal: state=%s claim=%q, want Assigned with no claim", got.Job.State, got.Job.Claim)
	}
}

// The other audit paths split capacity from fault the same way.
func TestOperationAndExportAuditBeginFailuresSplitCapacityFromFault(t *testing.T) {
	g := &Gateway{}
	for name, respond := range map[string]func(http.ResponseWriter, error){
		"operations": func(w http.ResponseWriter, err error) { g.operationError(w, err) },
		"exports":    func(w http.ResponseWriter, err error) { g.exportHTTPError(w, nil, err) },
	} {
		busy, fault := httptest.NewRecorder(), httptest.NewRecorder()
		respond(busy, audit.ErrBusy)
		respond(fault, audit.ErrUncertain)
		if busy.Code != http.StatusTooManyRequests || busy.Header().Get("Retry-After") != "1" || !strings.Contains(busy.Body.String(), "RESOURCE_EXHAUSTED") {
			t.Errorf("%s busy: status=%d retry=%q body=%s", name, busy.Code, busy.Header().Get("Retry-After"), busy.Body.String())
		}
		if fault.Code != http.StatusServiceUnavailable || fault.Header().Get("Retry-After") != "" {
			t.Errorf("%s fault: status=%d retry=%q", name, fault.Code, fault.Header().Get("Retry-After"))
		}
	}
	busy, fault := httptest.NewRecorder(), httptest.NewRecorder()
	auditBeginHTTPError(busy, audit.ErrBusy, "audit storage unavailable")
	auditBeginHTTPError(fault, audit.ErrCorrupt, "audit storage unavailable")
	if busy.Code != http.StatusTooManyRequests || busy.Header().Get("Retry-After") != "1" || fault.Code != http.StatusServiceUnavailable || fault.Header().Get("Retry-After") != "" {
		t.Errorf("node endpoints: busy=%d/%q fault=%d/%q", busy.Code, busy.Header().Get("Retry-After"), fault.Code, fault.Header().Get("Retry-After"))
	}
}

func TestNodeAuditDemandCountsExports(t *testing.T) {
	p := testPolicy()
	p.Workers = map[string]int{"a1": 2}
	c := NodeConfig{Policy: p, WorkerID: "a1", Audit: auditTestConfig(0), Operations: &OperationNodeConfig{MaxConcurrent: 3}, Exports: &ExportNodeConfig{MaxConcurrent: 4, MaxDownloads: 5}}
	if got := nodeAuditDemand(c); got != 14 {
		t.Fatalf("demand %d, want workers 2 + operations 3 + export executions 4 + downloads 5", got)
	}
	c.Audit = auditTestConfig(13)
	if err := validateNodeAudit(c); !errors.Is(err, errAuditCapacity) {
		t.Fatalf("13 < 14 accepted: %v", err)
	}
	c.Audit = auditTestConfig(14)
	if err := validateNodeAudit(c); err != nil {
		t.Fatal(err)
	}
}

// A node's audit refusal on an export part is capacity to the downloader too,
// end to end through the relay (not by feeding an error to the mapper).
func TestExportPartRelayPassesOnANodeAuditRefusalAs429(t *testing.T) {
	g, store, _ := exportGatewayFixture(t, "none")
	wire, stats := exportGatewayWire(t, "none")
	seedExportGatewayReady(t, g, store, wire, stats)
	hits := 0
	installExportGatewayWorker(t, g, func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Retry-After", "1")
		http.Error(w, "audit queue full", http.StatusTooManyRequests)
	})
	w := httptest.NewRecorder()
	if panicked := exportGatewayCall(g, w, exportGatewayRequest(http.MethodGet, "/v1/exports/"+exportGatewayID+"/parts/0", rotationOld, "")); panicked != nil {
		t.Fatalf("relay aborted: %v", panicked)
	}
	if hits != 1 || w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" || !strings.Contains(w.Body.String(), "RESOURCE_EXHAUSTED") {
		t.Fatalf("worker hits=%d status=%d retry=%q body=%s", hits, w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
}

func TestApplicationStorageErrorSplitsAuditCapacityFromFault(t *testing.T) {
	busy, fault := httptest.NewRecorder(), httptest.NewRecorder()
	applicationStorageError(busy, audit.ErrBusy)
	applicationStorageError(fault, audit.ErrUncertain)
	if busy.Code != http.StatusTooManyRequests || busy.Header().Get("Retry-After") != "1" || !strings.Contains(busy.Body.String(), "RESOURCE_EXHAUSTED") {
		t.Errorf("busy: status=%d retry=%q body=%s", busy.Code, busy.Header().Get("Retry-After"), busy.Body.String())
	}
	if fault.Code != http.StatusServiceUnavailable || fault.Header().Get("Retry-After") != "" {
		t.Errorf("fault: status=%d retry=%q", fault.Code, fault.Header().Get("Retry-After"))
	}
}
