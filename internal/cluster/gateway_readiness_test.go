// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type readinessTransport func(*http.Request) (*http.Response, error)

func (f readinessTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func readinessResponse(status int) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("ready\n")),
		TLS: &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}}}}}
}

func readinessEndpoint(handler readinessTransport) workerEndpoint {
	u, _ := url.Parse("https://worker.invalid")
	return workerEndpoint{url: u, client: &http.Client{Transport: handler, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func readinessGateway(t *testing.T, tenants map[string]gatewayTenant) *Gateway {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g := &Gateway{tenants: tenants, ctx: ctx, cancel: cancel, reconcileOK: map[string]bool{}}
	for tenant := range tenants {
		g.reconcileOK[tenant] = true
	}
	return g
}

func readinessStatus(g *Gateway) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	return w
}

func TestGatewayWorkerReadinessRequiresEachTenantAndNeverFansOutPublicProbes(t *testing.T) {
	var calls atomic.Int32
	var tenantDown atomic.Bool
	endpoint := func(healthy func() bool) workerEndpoint {
		return readinessEndpoint(func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			if r.Method != http.MethodGet || r.URL.Path != "/ready" || r.Header.Get("Authorization") != "" || r.Header.Get("X-Kelvo-Claim") != "" {
				t.Error("health probe used an execution route or caller credentials")
			}
			status := http.StatusOK
			if !healthy() {
				status = http.StatusServiceUnavailable
			}
			return readinessResponse(status), nil
		})
	}
	g := readinessGateway(t, map[string]gatewayTenant{
		"private-tenant-a": {workers: map[string]workerEndpoint{"down": endpoint(func() bool { return false }), "up": endpoint(func() bool { return true })}},
		"private-tenant-b": {workers: map[string]workerEndpoint{"up": endpoint(func() bool { return !tenantDown.Load() })}},
	})
	if readinessStatus(g).Code != http.StatusServiceUnavailable || calls.Load() != 0 {
		t.Fatal("uninitialized cache reported readiness or public probe did network I/O")
	}
	g.refreshWorkerReadiness()
	for range 32 {
		if readinessStatus(g).Code != http.StatusOK {
			t.Fatal("one healthy configured worker per tenant was insufficient")
		}
	}
	if calls.Load() != 3 {
		t.Fatal("public readiness probes fanned out to workers")
	}
	tenantDown.Store(true)
	g.refreshWorkerReadiness()
	response := readinessStatus(g)
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "private-tenant") || strings.Contains(response.Body.String(), "worker") {
		t.Fatal("missing tenant worker did not fail privately")
	}
	tenantDown.Store(false)
	g.refreshWorkerReadiness()
	if readinessStatus(g).Code != http.StatusOK {
		t.Fatal("worker recovery did not restore readiness")
	}
}

func TestGatewayWorkerReadinessExpiresAndStopsOnDrainShutdown(t *testing.T) {
	for _, reason := range []string{"stale", "drain", "shutdown", "closed", "static-certificate", "unconfigured-worker", "reconcile"} {
		t.Run(reason, func(t *testing.T) {
			g := readinessGateway(t, map[string]gatewayTenant{"a": {workers: map[string]workerEndpoint{"a1": {}}}})
			g.workerHealth = gatewayWorkerHealth{"a": {"a1": time.Now().Add(time.Minute)}}
			if readinessStatus(g).Code != http.StatusOK {
				t.Fatal("healthy fixture is unavailable")
			}
			switch reason {
			case "stale":
				g.workerHealth["a"]["a1"] = time.Now().Add(-time.Second)
			case "drain":
				g.BeginDrain()
				g.refreshWorkerReadiness() // A drained sweep must not deadlock.
			case "shutdown":
				g.cancel()
			case "closed":
				g.closed = true
			case "static-certificate":
				g.workerCertificateUntil = time.Now().Add(-time.Second)
			case "unconfigured-worker":
				g.workerHealth = gatewayWorkerHealth{"a": {"other": time.Now().Add(time.Minute)}}
			case "reconcile":
				g.reconcileOK["a"] = false
			}
			if readinessStatus(g).Code != http.StatusServiceUnavailable {
				t.Fatal("failed state retained readiness")
			}
		})
	}
}

func TestGatewayWorkerReadinessBoundsConcurrencyAndCancels(t *testing.T) {
	var active, peak atomic.Int32
	entered := make(chan struct{}, 24)
	workers := map[string]workerEndpoint{}
	for i := range 24 {
		workers[fmt.Sprint(i)] = readinessEndpoint(func(r *http.Request) (*http.Response, error) {
			count := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); count > old && !peak.CompareAndSwap(old, count); old = peak.Load() {
			}
			deadline, ok := r.Context().Deadline()
			if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > gatewayReadinessTimeout {
				t.Error("worker probe is not bounded by two seconds")
			}
			entered <- struct{}{}
			<-r.Context().Done()
			return nil, r.Context().Err()
		})
	}
	g := readinessGateway(t, map[string]gatewayTenant{"a": {workers: workers}})
	done := make(chan struct{})
	go func() { g.refreshWorkerReadiness(); close(done) }()
	for range gatewayReadinessParallel {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("worker probes did not start")
		}
	}
	if peak.Load() != gatewayReadinessParallel || readinessStatus(g).Code != http.StatusServiceUnavailable {
		t.Fatal("concurrency bound or fail-closed initial state changed")
	}
	g.cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel blocked probes")
	}
	if active.Load() != 0 || peak.Load() > gatewayReadinessParallel {
		t.Fatal("worker probe leaked or exceeded parallelism")
	}
}

func TestGatewayWorkerReadinessRejectsInvalidResponsesAndBoundsCertificates(t *testing.T) {
	for _, reason := range []string{"status", "redirect", "no-tls", "unverified", "empty-chain", "expired", "future", "oversized", "cancelled"} {
		t.Run(reason, func(t *testing.T) {
			g := readinessGateway(t, nil)
			ep := readinessEndpoint(func(*http.Request) (*http.Response, error) {
				r := readinessResponse(http.StatusOK)
				switch reason {
				case "status":
					r.StatusCode = http.StatusServiceUnavailable
				case "redirect":
					r.StatusCode = http.StatusTemporaryRedirect
				case "no-tls":
					r.TLS = nil
				case "unverified":
					r.TLS.VerifiedChains = nil
				case "empty-chain":
					r.TLS.VerifiedChains = [][]*x509.Certificate{nil}
				case "expired":
					r.TLS.VerifiedChains[0][0].NotAfter = time.Now().Add(-time.Second)
				case "future":
					r.TLS.VerifiedChains[0][0].NotBefore = time.Now().Add(time.Second)
				case "oversized":
					r.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", gatewayReadinessBodyMax+1)))
				case "cancelled":
					g.cancel()
				}
				return r, nil
			})
			if !g.probeWorkerReadiness(ep).IsZero() {
				t.Fatal("invalid health evidence was cached")
			}
		})
	}
	g := readinessGateway(t, nil)
	certificateExpiry := time.Now().Add(time.Second)
	ep := readinessEndpoint(func(*http.Request) (*http.Response, error) {
		r := readinessResponse(http.StatusOK)
		r.TLS.VerifiedChains[0][0].NotAfter = certificateExpiry
		return r, nil
	})
	if until := g.probeWorkerReadiness(ep); !until.Equal(certificateExpiry) {
		t.Fatal("health cache outlived peer certificate")
	}
	g.workerCertificateUntil = time.Now().Add(500 * time.Millisecond)
	if until := g.probeWorkerReadiness(ep); !until.Equal(g.workerCertificateUntil) {
		t.Fatal("health cache outlived static gateway certificate")
	}
}

type readinessBlockedBody struct {
	ctx    context.Context
	closed atomic.Bool
}

func TestGatewayWorkerReadinessCertificateBoundsRetainMonotonicExpiry(t *testing.T) {
	for _, source := range []string{"peer", "static-client"} {
		t.Run(source, func(t *testing.T) {
			g := readinessGateway(t, nil)
			// Parsed X.509 dates have no monotonic component. A near expiry
			// must shorten the cache without making it follow wall-clock steps.
			expires := time.Now().Add(time.Second).Round(0)
			if source == "static-client" {
				g.workerCertificateUntil = expires
			}
			ep := readinessEndpoint(func(*http.Request) (*http.Response, error) {
				response := readinessResponse(http.StatusOK)
				if source == "peer" {
					response.TLS.VerifiedChains[0][0].NotAfter = expires
				}
				return response, nil
			})
			until := g.probeWorkerReadiness(ep)
			if !until.Equal(expires) {
				t.Fatal("certificate expiry did not bound cached health")
			}
			// Round(0) strips the monotonic reading. Direct equality also
			// compares that reading, unlike Equal, which compares instants.
			if until == until.Round(0) {
				t.Fatal("certificate clipping discarded monotonic cache expiry")
			}
			g.tenants = map[string]gatewayTenant{"a": {workers: map[string]workerEndpoint{"a1": ep}}}
			g.workerHealth = gatewayWorkerHealth{"a": {"a1": until}}
			if !g.workersReadyLocked(until.Add(-time.Nanosecond)) || g.workersReadyLocked(until) {
				t.Fatal("cached health did not expire at its monotonic deadline")
			}
		})
	}
}

func (b *readinessBlockedBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (b *readinessBlockedBody) Close() error { b.closed.Store(true); return nil }

func TestGatewayWorkerReadinessBodyDeadline(t *testing.T) {
	g := readinessGateway(t, nil)
	var body *readinessBlockedBody
	ep := readinessEndpoint(func(r *http.Request) (*http.Response, error) {
		response := readinessResponse(http.StatusOK)
		body = &readinessBlockedBody{ctx: r.Context()}
		response.Body = body
		return response, nil
	})
	started := time.Now()
	if !g.probeWorkerReadiness(ep).IsZero() || !body.closed.Load() {
		t.Fatal("stalled response body succeeded or remained open")
	}
	if time.Since(started) > gatewayReadinessTimeout+time.Second {
		t.Fatal("response body exceeded the bounded health deadline")
	}
}

func TestGatewayWorkerReadinessUsesVerifiedMTLSIdentity(t *testing.T) {
	gatewayTLS, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	workerTLS, _, _ := tlsFiles(t, WorkerIdentity("a", "a1"), ca, key)
	serverConfig, err := BuildServerTLS(workerTLS, WorkerIdentity("a", "a1"), GatewayIdentity)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/ready" {
			t.Error("health check requested an execution endpoint")
		}
		_, _ = io.WriteString(w, "ready\n")
	}))
	server.TLS = serverConfig
	server.StartTLS()
	t.Cleanup(server.Close)
	endpointURL, _ := url.Parse(server.URL)
	g := readinessGateway(t, nil)
	for _, tenant := range []string{"a", "other"} {
		clientConfig, err := BuildClientTLS(gatewayTLS, WorkerIdentity(tenant, "a1"))
		if err != nil {
			t.Fatal(err)
		}
		clientConfig.ServerName = "gateway.test"
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy, transport.TLSClientConfig = nil, clientConfig
		t.Cleanup(transport.CloseIdleConnections)
		g.workerCertificateUntil = gatewayStaticCertificateExpiry(clientConfig)
		until := g.probeWorkerReadiness(workerEndpoint{url: endpointURL, client: &http.Client{Transport: transport}})
		if until.IsZero() != (tenant != "a") {
			t.Fatal("worker identity was not verified before caching readiness")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("wrong worker identity reached the health handler")
	}
}

// Sign the existing fixture identity for loopback before starting background I/O.
func readinessLoopbackIdentity(t *testing.T, raw []byte, ca *x509.Certificate, key *rsa.PrivateKey) []byte {
	t.Helper()
	block, rest := pem.Decode(raw)
	if block == nil {
		t.Fatal("missing fixture certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	cert.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca, cert.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), rest...)
}
