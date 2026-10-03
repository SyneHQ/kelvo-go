// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
	"go.opentelemetry.io/otel/trace"
)

type traceGatewayStore struct {
	gatewayStore
	submit func(context.Context, query.Request) (Snapshot, error)
}

func (s *traceGatewayStore) Submit(ctx context.Context, req query.Request) (Snapshot, error) {
	return s.submit(ctx, req)
}

func configuredTraceGateway(t *testing.T, cfg *tracing.Config, store Store) *Gateway {
	t.Helper()
	workerTLS, _, _ := tlsFiles(t, GatewayIdentity, nil, nil)
	t.Setenv("KELVO_TRACE_GATEWAY_TOKEN", strings.Repeat("t", 32))
	gateway, err := NewGateway(GatewayConfig{Tracing: cfg, WorkerTLS: workerTLS, MaxHTTPRequests: 4,
		Tenants: []TenantConfig{{Policy: store.Policy(), TokenEnv: "KELVO_TRACE_GATEWAY_TOKEN", Workers: []Endpoint{{ID: "a1", URL: "https://127.0.0.1:1"}}}},
	}, map[string]Store{"a": store})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gateway.Close() })
	return gateway
}

func TestClusterTraceGatewayMintsPrivateContextAfterAuthentication(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			var observed tracing.Carrier
			calls := 0
			store := &traceGatewayStore{gatewayStore: gatewayStore{policy: testPolicy()}, submit: func(ctx context.Context, req query.Request) (Snapshot, error) {
				calls++
				observed = tracing.CarrierFromContext(ctx)
				return Snapshot{Job: Job{ID: "accepted", State: Queued}}, nil
			}}
			var cfg *tracing.Config
			if enabled {
				cfg = &tracing.Config{Endpoint: "https://127.0.0.1:1", SampleRatio: 0}
			}
			gateway := configuredTraceGateway(t, cfg, store)
			incoming := fixtureTrace()
			tid, _ := trace.TraceIDFromHex(incoming.TraceID)
			sid, _ := trace.SpanIDFromHex(incoming.SpanID)
			ctx := trace.ContextWithRemoteSpanContext(tracing.WithCarrier(context.Background(), incoming), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
			for _, token := range []string{"wrong", strings.Repeat("t", 32)} {
				r := httptest.NewRequest(http.MethodPost, "/v1/queries", strings.NewReader(`{"mode":"federated","sql":"SELECT 1"}`)).WithContext(ctx)
				r.Header.Set("Authorization", "Bearer "+token)
				r.Header.Set("Traceparent", "00-"+incoming.TraceID+"-"+incoming.SpanID+"-01")
				r.Header.Set("Tracestate", "private=secret")
				r.Header.Set("Baggage", "tenant=secret")
				w := httptest.NewRecorder()
				gateway.ServeHTTP(w, r)
				if token == "wrong" {
					if w.Code != http.StatusUnauthorized || calls != 0 {
						t.Fatal("unauthenticated submission reached tracing boundary")
					}
					continue
				}
				if w.Code != http.StatusCreated || calls != 1 || observed.Valid() != enabled || observed.TraceID == incoming.TraceID {
					t.Fatalf("context was not freshly minted: status=%d calls=%d carrier=%+v", w.Code, calls, observed)
				}
				if observed.Sampled || strings.Contains(strings.ToLower(w.Body.String()), "trace") || w.Header().Get("Traceparent") != "" {
					t.Fatal("sampling ceiling or public response boundary changed")
				}
			}
		})
	}
}

func TestClusterTraceGatewayClosesRecorderAfterHandlers(t *testing.T) {
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	store := &traceGatewayStore{gatewayStore: gatewayStore{policy: testPolicy()}, submit: func(ctx context.Context, _ query.Request) (Snapshot, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		return Snapshot{}, ctx.Err()
	}}
	gateway := configuredTraceGateway(t, &tracing.Config{Endpoint: "https://127.0.0.1:1", SampleRatio: 0}, store)
	r := httptest.NewRequest(http.MethodPost, "/v1/queries", strings.NewReader(`{"mode":"federated","sql":"SELECT 1"}`))
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("t", 32))
	handlerDone := make(chan struct{})
	go func() { defer close(handlerDone); gateway.ServeHTTP(httptest.NewRecorder(), r) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("submission did not enter configured gateway")
	}
	closed := make(chan error, 1)
	go func() { closed <- gateway.Close() }()
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("gateway close did not cancel its handler")
	}
	span := gateway.tracing.Start(tracing.Carrier{}, tracing.ClusterSubmit)
	if !span.Carrier().Valid() {
		unblock()
		t.Fatal("recorder stopped before its handler joined")
	}
	span.End(telemetry.OutcomeCanceled)
	select {
	case <-closed:
		unblock()
		t.Fatal("close returned while handler was active")
	default:
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("gateway failed to join handler and recorder")
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("close returned before handler completion")
	}
	if gateway.tracing.Start(tracing.Carrier{}, tracing.ClusterSubmit).Carrier().Valid() {
		t.Fatal("gateway leaked its configured recorder")
	}
	if err := gateway.Close(); err != nil {
		t.Fatal("repeated close changed result", err)
	}
}

func TestClusterTraceGatewayBoundsStalledExporterShutdown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	gateway := configuredTraceGateway(t, &tracing.Config{Endpoint: "https://" + listener.Addr().String(), SampleRatio: 1, QueueSize: 1, ExportTimeout: 30 * time.Second}, &gatewayStore{policy: testPolicy()})
	span := gateway.tracing.Start(tracing.Carrier{}, tracing.ClusterSubmit)
	span.End(telemetry.OutcomeSuccess)
	var connection net.Conn
	select {
	case connection = <-accepted:
		defer connection.Close()
	case <-time.After(3 * time.Second):
		t.Fatal("configured exporter did not attempt TLS")
	}
	// The collector accepts TCP but never completes TLS. No trust bypass is used.
	closed := make(chan error, 1)
	go func() { closed <- gateway.Close() }()
	select {
	case err := <-closed:
		if err == nil || strings.Contains(err.Error(), listener.Addr().String()) {
			t.Fatal("stalled shutdown missing sanitized failure", err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("exporter escaped the five-second shutdown bound")
	}
}

func TestClusterTraceGatewayConfigValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.yml")
	for _, endpoint := range []string{"https://collector.invalid/v1/traces", "http://collector.invalid/v1/traces"} {
		if err := os.WriteFile(path, []byte(gatewayYAML+"tracing:\n  endpoint: "+endpoint+"\n  sample_ratio: 0\n"), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadGateway(path)
		if strings.HasPrefix(endpoint, "https:") {
			if err != nil || cfg.Tracing == nil || cfg.Tracing.Endpoint != endpoint {
				t.Fatal("valid opt-in tracing config rejected", err)
			}
		} else if err == nil {
			t.Fatal("insecure tracing config accepted")
		}
	}
}

func TestClusterTraceGatewayRelayPreservesDurableEOS(t *testing.T) {
	encoded, stats := compressedRelayFixture(t, "lz4_frame")
	for _, failCommit := range []bool{false, true} {
		name := "success"
		if failCommit {
			name = "commit conflict"
		}
		t.Run(name, func(t *testing.T) {
			store := &compressionRelayStore{gatewayStore: gatewayStore{policy: testPolicy()}, failCommit: failCommit,
				snapshot: Snapshot{Revision: 1, Job: Job{Trace: copyJobTraceValue(fixtureTrace()), ID: "result", TenantID: "a", State: Assigned, WorkerID: "a1", Owner: strings.Repeat("c", 32), ExpiresAt: time.Now().Add(time.Minute)}},
			}
			output := httptest.NewRecorder()
			store.beforeCommit = func() {
				if !bytes.Equal(output.Body.Bytes(), encoded[:len(encoded)-8]) {
					t.Error("trace instrumentation released EOS before durable success")
				}
			}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Traceparent") != "" || r.Header.Get("Tracestate") != "" || r.Header.Get("Baggage") != "" {
					t.Error("gateway forwarded client telemetry headers")
				}
				if !store.resultReady(r.Header.Get("X-Kelvo-Claim"), stats) {
					t.Error("claim changed")
				}
				_, _ = w.Write(encoded)
			}))
			defer upstream.Close()
			endpoint, _ := url.Parse(upstream.URL)
			recorder, err := tracing.New(tracing.Config{Endpoint: "https://127.0.0.1:1", SampleRatio: 0})
			if err != nil {
				t.Fatal(err)
			}
			defer recorder.Shutdown(context.Background())
			gateway := &Gateway{tracing: recorder}
			request := httptest.NewRequest(http.MethodGet, "/v1/queries/result/results", nil)
			request.Header.Set("Traceparent", "forged")
			request.Header.Set("Baggage", "private-data")
			aborted := false
			func() {
				defer func() {
					if value := recover(); value != nil {
						if value != http.ErrAbortHandler {
							t.Fatalf("unexpected panic: %v", value)
						}
						aborted = true
					}
				}()
				gateway.results(output, request, gatewayTenant{store: store, workers: map[string]workerEndpoint{"a1": {url: endpoint, client: upstream.Client()}}}, "result", nil)
			}()
			want := encoded
			if failCommit {
				want = encoded[:len(encoded)-8]
			}
			if aborted != failCommit || !bytes.Equal(output.Body.Bytes(), want) {
				t.Fatal("tracing changed relay completion")
			}
		})
	}
}

type traceQueuedStore struct {
	gatewayStore
	cancel context.CancelFunc
	gets   atomic.Int32
}

func (s *traceQueuedStore) Get(ctx context.Context, id string) (Snapshot, error) {
	if s.gets.Add(1) == 2 {
		s.cancel()
	}
	return s.gatewayStore.Get(ctx, id)
}

func TestClusterTraceGatewayQueuedCancellationAndPrincipalIsolation(t *testing.T) {
	recorder, err := tracing.New(tracing.Config{Endpoint: "https://127.0.0.1:1", SampleRatio: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer recorder.Shutdown(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &traceQueuedStore{cancel: cancel, gatewayStore: gatewayStore{policy: testPolicy(), jobs: map[string]Snapshot{"pending": {Job: Job{
		ID: "pending", TenantID: "a", State: Queued, Trace: copyJobTraceValue(fixtureTrace()), ExpiresAt: time.Now().Add(time.Minute),
	}}}}}
	gateway := &Gateway{tracing: recorder}
	output := httptest.NewRecorder()
	gateway.results(output, httptest.NewRequest(http.MethodGet, "/v1/queries/pending/results", nil).WithContext(ctx), gatewayTenant{store: store}, "pending", nil)
	if store.gets.Load() != 2 || store.jobs["pending"].Job.State != Queued || strings.Contains(output.Body.String(), "trace") {
		t.Fatal("cancelled local wait changed durable job")
	}
	policy := principalTestPolicy()
	authority, _ := authorityForPrincipal(policy, "analyst")
	foreign, _ := authorityForPrincipal(policy, "reports")
	store.policy = policy
	job := store.jobs["pending"]
	job.Job.Authority = &authority
	job.Job.Request = query.Request{Mode: "native", ConnectionID: "sales_native", SQL: "SELECT 1"}
	store.jobs["pending"] = job
	ctx = context.WithValue(context.Background(), jobAuthorityKey{}, foreign)
	output = httptest.NewRecorder()
	gateway.results(output, httptest.NewRequest(http.MethodGet, "/v1/queries/pending/results", nil).WithContext(ctx), gatewayTenant{store: store}, "pending", nil)
	if output.Code != http.StatusNotFound || store.jobs["pending"].Job.State != Queued {
		t.Fatal("trace carrier bypassed durable principal isolation")
	}
}
