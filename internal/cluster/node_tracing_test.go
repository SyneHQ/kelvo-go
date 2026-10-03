// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
)

type traceCaptureExecutor struct {
	calls   int
	carrier tracing.Carrier
}

func (e *traceCaptureExecutor) Execute(ctx context.Context, request query.Request, _ query.Sink) (query.Stats, error) {
	if request.SQL == "SELECT 1" {
		return query.Stats{}, nil
	}
	e.calls++
	e.carrier = tracing.CarrierFromContext(ctx)
	return query.Stats{}, query.NewError("QUERY_FAILED", "fixture execution failed")
}

func TestClusterTraceWorkerUsesValidatedDurableCarrier(t *testing.T) {
	for _, scenario := range []string{"valid", "legacy", "invalid", "legacy writer drops carrier", "principal revoked before claim", "forged claim"} {
		t.Run(scenario, func(t *testing.T) {
			policy := principalTestPolicy()
			authority, _ := authorityForPrincipal(policy, "analyst")
			store := &nodeTestStore{p: policy, jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}
			executor := &traceCaptureExecutor{}
			recorder, err := tracing.New(tracing.Config{Endpoint: "https://127.0.0.1:1", SampleRatio: 0})
			if err != nil {
				t.Fatal(err)
			}
			defer recorder.Shutdown(context.Background())
			node, err := newNode(NodeConfig{Policy: policy, WorkerID: "a1", RuntimeTracing: recorder}, store, executor)
			if err != nil {
				t.Fatal(err)
			}
			defer node.Close()
			id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			carrier := copyJobTraceValue(fixtureTrace())
			if scenario == "legacy" {
				carrier = nil
			} else if scenario == "invalid" {
				carrier = &tracing.Carrier{Version: 99}
			}
			store.mu.Lock()
			store.jobs[id] = Snapshot{Revision: 1, Job: Job{ID: id, TenantID: "a", State: Queued, Authority: &authority, Trace: carrier,
				ExpiresAt: time.Now().Add(time.Minute), Request: query.Request{Mode: "native", ConnectionID: "sales_native", SQL: "SELECT 2"}}}
			store.mu.Unlock()
			if err := store.Enqueue(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { snapshot, _ := store.Get(context.Background(), id); return snapshot.Job.State == Assigned })
			snapshot, _ := store.Get(context.Background(), id)
			next := snapshot.Job
			next.State, next.Claim = Claimed, strings.Repeat("c", 32)
			if scenario == "principal revoked before claim" {
				next.Authority = nil
			}
			if scenario == "legacy writer drops carrier" {
				next.Trace = nil
			}
			if _, err := store.CompareAndSwap(context.Background(), snapshot, next); err != nil {
				t.Fatal(err)
			}
			uri, _ := url.Parse(GatewayIdentity)
			cert := &x509.Certificate{URIs: []*url.URL{uri}}
			forged := fixtureTrace()
			forged.TraceID = strings.Repeat("f", 32)
			request := httptest.NewRequest(http.MethodGet, "/internal/queries/"+id+"/results", nil).WithContext(tracing.WithCarrier(context.Background(), forged))
			request.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}
			request.Header.Set("X-Kelvo-Claim", next.Claim)
			request.Header.Set("Traceparent", "00-"+forged.TraceID+"-"+forged.SpanID+"-01")
			if scenario == "forged claim" {
				request.Header.Set("X-Kelvo-Claim", strings.Repeat("e", 32))
			}
			response := httptest.NewRecorder()
			node.ServeHTTP(response, request)
			if response.Code != http.StatusConflict {
				t.Fatal("fixture failure status changed", response.Code)
			}
			if scenario == "principal revoked before claim" || scenario == "forged claim" {
				if executor.calls != 0 {
					t.Fatal("trace context bypassed claim or principal validation")
				}
			} else if executor.calls != 1 || executor.carrier != jobTrace(next.Trace) {
				t.Fatal("execution did not receive only validated durable context")
			}
		})
	}
}

func TestClusterTraceWorkerRejectsUnauthorizedDispatch(t *testing.T) {
	policy := principalTestPolicy()
	store := &nodeTestStore{p: policy, jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}
	executor := &traceCaptureExecutor{}
	recorder, err := tracing.New(tracing.Config{Endpoint: "https://127.0.0.1:1", SampleRatio: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer recorder.Shutdown(context.Background())
	node, err := newNode(NodeConfig{Policy: policy, WorkerID: "a1", RuntimeTracing: recorder}, store, executor)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	store.mu.Lock()
	store.jobs[id] = Snapshot{Revision: 1, Job: Job{ID: id, TenantID: "a", State: Queued, Trace: copyJobTraceValue(fixtureTrace()),
		ExpiresAt: time.Now().Add(time.Minute), Request: query.Request{Mode: "native", ConnectionID: "sales_native", SQL: "SELECT 2"}}}
	store.mu.Unlock()
	if err := store.Enqueue(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { snapshot, _ := store.Get(context.Background(), id); return snapshot.Job.State == Failed })
	if executor.calls != 0 {
		t.Fatal("diagnostic carrier allowed unauthorized dispatch")
	}
}
