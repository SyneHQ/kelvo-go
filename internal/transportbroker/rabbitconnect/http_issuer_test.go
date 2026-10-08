// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

func httpIssuerFixture(t *testing.T, f fixture, config *tls.Config, handler http.HandlerFunc) (*HTTPIssuer, *Opener) {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.TLS = config
	server.StartTLS()
	t.Cleanup(server.Close)
	endpoint := "https://proxy.test:" + strings.Split(server.Listener.Addr().String(), ":")[1] + transportissuer.Path
	client, err := NewHTTPIssuer(HTTPIssuerConfig{Endpoint: endpoint, WorkerIdentity: f.config.WorkerIdentity, RootCAPEM: f.config.RootCAPEM, ClientCertificatePEM: f.config.ClientCertificatePEM, ClientKeyPEM: f.config.ClientKeyPEM, MaxInFlight: 1})
	if err != nil {
		t.Fatal(err)
	}
	dial := client.transport.DialTLSContext
	client.transport.DialTLSContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dial(ctx, network, server.Listener.Addr().String())
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := client.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	opener, err := New(f.config, client)
	if err != nil {
		t.Fatal(err)
	}
	return client, opener
}

func TestHTTPIssuerVerifiedRequestAndTicket(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	var expected IssueRequest
	client, opener := httpIssuerFixture(t, f, f.server, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "POST" || r.URL.Path != transportissuer.Path || r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			t.Error("issuer did not receive verified mTLS POST")
		}
		data, err := io.ReadAll(io.LimitReader(r.Body, transportissuer.MaxRequestBytes+1))
		if err != nil {
			t.Error(err)
		}
		var request transportissuer.Request
		if strictJSON(data, &request) != nil || request.Version != 1 || request.Authority != expected.Binding.Authority || request.OpenID != expected.OpenID || request.Execution.Claim != expected.Binding.Execution.Claim || request.SourceRevision != expected.Binding.SourceRevision {
			t.Error("issuer request lost exact scope")
		}
		sum := sha256.Sum256(r.TLS.PeerCertificates[0].Raw)
		if request.WorkerCertSHA256 != hex.EncodeToString(sum[:]) {
			t.Error("request does not match verified caller certificate")
		}
		sum = sha256.Sum256(data)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(transportissuer.Response{Version: 1, RequestSHA256: hex.EncodeToString(sum[:]), Token: signTicket(f.key, claimsFor(expected))})
	})
	expected = issueFor(opener, testRequest())
	token, err := client.Issue(context.Background(), expected)
	if err != nil || opener.verifyTicket(token, expected, time.Now()) != nil || calls.Load() != 1 {
		t.Fatal("one verified issuance failed", err, calls.Load())
	}
	wrong := expected
	wrong.WorkerIdentity = "spiffe://other.test/worker"
	if _, err = client.Issue(context.Background(), wrong); !errors.Is(err, transportbroker.ErrScope) || calls.Load() != 1 {
		t.Fatal("foreign worker reached issuer", err)
	}
}

func TestHTTPIssuerRefusesMalformedOrRedirectedResponses(t *testing.T) {
	for _, mode := range []string{"redirect", "oversize", "duplicate", "unknown", "wrong-digest", "wrong-version", "compressed", "empty", "unauthorized"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			var calls atomic.Int32
			client, opener := httpIssuerFixture(t, f, f.server, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, _ := io.ReadAll(r.Body)
				sum := sha256.Sum256(body)
				response := transportissuer.Response{Version: 1, RequestSHA256: hex.EncodeToString(sum[:]), Token: "fixture-token"}
				w.Header().Set("Content-Type", "application/json")
				switch mode {
				case "redirect":
					w.Header().Set("Location", "/never-follow")
					w.WriteHeader(307)
					return
				case "oversize":
					_, _ = io.WriteString(w, strings.Repeat("x", transportissuer.MaxResponseBytes+1))
					return
				case "duplicate":
					_, _ = io.WriteString(w, `{"version":1,"version":1}`)
					return
				case "unknown":
					_, _ = io.WriteString(w, `{"version":1,"secret":"private"}`)
					return
				case "wrong-digest":
					response.RequestSHA256 = strings.Repeat("a", 64)
				case "wrong-version":
					response.Version = 2
				case "compressed":
					w.Header().Set("Content-Encoding", "gzip")
				case "empty":
					response.Token = ""
				case "unauthorized":
					w.WriteHeader(403)
				}
				_ = json.NewEncoder(w).Encode(response)
			})
			if token, err := client.Issue(context.Background(), issueFor(opener, testRequest())); err == nil || token != "" || calls.Load() != 1 {
				t.Fatal("invalid issuance accepted or retried", err, calls.Load())
			}
		})
	}
}

func TestHTTPIssuerRequiresMutualTLS(t *testing.T) {
	f := newFixture(t)
	server := f.server.Clone()
	server.ClientAuth = tls.NoClientCert
	var calls atomic.Int32
	client, opener := httpIssuerFixture(t, f, server, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	if _, err := client.Issue(context.Background(), issueFor(opener, testRequest())); err == nil || calls.Load() != 0 {
		t.Fatal("issuer without client authentication received request", err)
	}
}

func TestHTTPIssuerCapacityAndShutdownJoin(t *testing.T) {
	f := newFixture(t)
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	client, opener := httpIssuerFixture(t, f, f.server, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
		close(cancelled)
	})
	done := make(chan error, 1)
	go func() { _, err := client.Issue(context.Background(), issueFor(opener, testRequest())); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("issuer did not start")
	}
	if _, err := client.Issue(context.Background(), issueFor(opener, testRequest())); !errors.Is(err, transportbroker.ErrCapacity) {
		t.Fatal("excess issuance was not refused", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("shutdown accepted an unfinished ticket")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("HTTP request did not cancel")
	}
	if len(client.slots) != 0 {
		t.Fatal("issuer retained admission after joined shutdown")
	}
	if _, err := client.Issue(context.Background(), issueFor(opener, testRequest())); !errors.Is(err, transportbroker.ErrClosed) {
		t.Fatal("shutdown allowed new admission", err)
	}
}

func TestHTTPIssuerRejectsPeerExpiryDuringBody(t *testing.T) {
	f := newFixture(t)
	// Align the short certificate with whole-second X.509 validity timestamps.
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second)))
	pair := f.server.Certificates[0]
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	leaf.NotAfter = time.Now().Truncate(time.Second).Add(time.Second)
	der, err := x509.CreateCertificate(rand.Reader, leaf, leaf, leaf.PublicKey, pair.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	pair.Certificate = [][]byte{der}
	pair.Leaf = nil
	server := f.server.Clone()
	server.Certificates = []tls.Certificate{pair}
	f.config.RootCAPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	var wroteHeaders atomic.Bool
	client, opener := httpIssuerFixture(t, f, server, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(body)
		data, _ := json.Marshal(transportissuer.Response{Version: 1, RequestSHA256: hex.EncodeToString(sum[:]), Token: "fixture-token"})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		wroteHeaders.Store(true)
		time.Sleep(time.Until(leaf.NotAfter.Add(25 * time.Millisecond)))
		_, _ = w.Write(data)
	})
	if token, err := client.Issue(context.Background(), issueFor(opener, testRequest())); err == nil || token != "" || !wroteHeaders.Load() {
		t.Fatal("peer expiry during response was not rejected after a valid handshake", err)
	}
}

func TestHTTPIssuerRejectsUnsafeEndpoints(t *testing.T) {
	f := newFixture(t)
	for _, endpoint := range []string{"http://proxy.test/v1/private-transport/issue", "https://user:secret@proxy.test/v1/private-transport/issue", "https://proxy.test/v1/private-transport/issue?", "https://proxy.test/v1/private-transport/issue?target=other", "https://proxy.test:99999/v1/private-transport/issue", "https://proxy.test/v1/private-transport/%69ssue", "https://proxy.test/other"} {
		if _, err := NewHTTPIssuer(HTTPIssuerConfig{Endpoint: endpoint, WorkerIdentity: f.config.WorkerIdentity, RootCAPEM: f.config.RootCAPEM, ClientCertificatePEM: f.config.ClientCertificatePEM, ClientKeyPEM: f.config.ClientKeyPEM, MaxInFlight: 1}); err == nil {
			t.Error("unsafe issuer endpoint accepted")
		}
	}
}
