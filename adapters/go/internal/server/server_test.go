package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
)

type executeFunc func(context.Context, string, string, operations.Request) (operations.Receipt, error)

func (f executeFunc) Execute(ctx context.Context, id, grant string, request operations.Request) (operations.Receipt, error) {
	return f(ctx, id, grant, request)
}

func testServer(t *testing.T, executor Executor) *http.Server {
	t.Helper()
	server, err := New(Config{TLS: &tls.Config{Certificates: []tls.Certificate{{}}, ClientCAs: x509.NewCertPool()}, WorkerURIs: []string{"spiffe://kelvo/worker/test"}, Executor: executor, MaxConcurrent: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return server
}
func requestFixture() operations.Request {
	return operations.Request{Version: operations.Version, Kind: operations.QueryRead, Connection: operations.ConnectionRef{ID: "saved-connection"}, Spec: operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT 1"}}}
}
func wireRequest(t *testing.T) *http.Request {
	t.Helper()
	data, err := json.Marshal(requestFixture())
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "https://adapter/v1/operations", strings.NewReader(string(data)))
	r.Header.Set("Authorization", "Bearer opaque-grant")
	r.Header.Set("Kelvo-Operation-ID", "operation-1")
	identity, _ := url.Parse("spiffe://kelvo/worker/test")
	cert := &x509.Certificate{NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), URIs: []*url.URL{identity}}
	r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}
	return r
}
func completed(request operations.Request) operations.Receipt {
	digest, _ := operations.Digest(request)
	return operations.Receipt{Version: operations.Version, OperationID: "operation-1", RequestSHA256: digest, Outcome: operations.Completed, Effect: operations.EffectNone}
}

func TestRequiresVerifiedCurrentWorkerIdentity(t *testing.T) {
	var called atomic.Int64
	server := testServer(t, executeFunc(func(_ context.Context, _, _ string, r operations.Request) (operations.Receipt, error) {
		called.Add(1)
		return completed(r), nil
	}))
	for _, mutate := range []func(*http.Request){
		func(r *http.Request) { r.TLS = nil },
		func(r *http.Request) { r.TLS.VerifiedChains = nil },
		func(r *http.Request) { r.TLS.PeerCertificates[0].NotAfter = time.Now().Add(-time.Second) },
		func(r *http.Request) { r.TLS.PeerCertificates[0].URIs = nil },
		func(r *http.Request) { r.Header.Del("Authorization") },
	} {
		r := wireRequest(t)
		mutate(r)
		w := httptest.NewRecorder()
		server.Handler.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status=%d body=%s", w.Code, w.Body)
		}
	}
	if called.Load() != 0 {
		t.Fatal("unauthorized backend execution")
	}
}

func TestRedactsExecutorFailureAndBindsReceipt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		execute executeFunc
		status  int
	}{
		{"success", func(_ context.Context, _, _ string, r operations.Request) (operations.Receipt, error) {
			return completed(r), nil
		}, http.StatusOK},
		{"driver error", func(context.Context, string, string, operations.Request) (operations.Receipt, error) {
			return operations.Receipt{}, errors.New("secret=password; query=private")
		}, http.StatusBadGateway},
		{"wrong operation", func(_ context.Context, _, _ string, r operations.Request) (operations.Receipt, error) {
			v := completed(r)
			v.OperationID = "other-operation"
			return v, nil
		}, http.StatusBadGateway},
		{"mutated payload", func(_ context.Context, _, _ string, r operations.Request) (operations.Receipt, error) {
			r.Spec.Query.SQL = "SELECT private"
			return completed(r), nil
		}, http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			testServer(t, tc.execute).Handler.ServeHTTP(w, wireRequest(t))
			if w.Code != tc.status || strings.Contains(w.Body.String(), "password") || strings.Contains(w.Body.String(), "private") {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
		})
	}
}

func TestAdmissionRemainsHeldUntilExecutorReturns(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	server := testServer(t, executeFunc(func(_ context.Context, _, _ string, r operations.Request) (operations.Receipt, error) {
		close(started)
		<-release
		return completed(r), nil
	}))
	first := wireRequest(t)
	ctx, cancel := context.WithCancel(first.Context())
	first = first.WithContext(ctx)
	go func() { defer close(done); server.Handler.ServeHTTP(httptest.NewRecorder(), first) }()
	<-started
	cancel()
	second := httptest.NewRecorder()
	server.Handler.ServeHTTP(second, wireRequest(t))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d", second.Code)
	}
	close(release)
	<-done
}
