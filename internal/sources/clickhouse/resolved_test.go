package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func resolvedFixture(t *testing.T, server *httptest.Server) ResolvedSource {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	return ResolvedSource{ID: "analytics", URL: server.URL + "?database=reports", Username: "reader", Password: "test-only-password", TLS: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
}

func TestResolvedSourceUsesVerifiedTLSAndExplicitCredentials(t *testing.T) {
	data, rowsBytes := fixture(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "reader" || password != "test-only-password" || r.TLS == nil || r.URL.Query().Get("readonly") != "1" {
			t.Error("source authority/TLS/read-only setting lost")
		}
		_, _ = w.Write(data)
	}))
	defer server.Close()
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("KELVO_TEST_CLICKHOUSE_USER", "wrong")
	source := resolvedFixture(t, server)
	engine, err := NewResolved(source, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	source.Username = "mutated"
	source.TLS.InsecureSkipVerify = true
	if engine.client.Transport.(*http.Transport).Proxy != nil {
		t.Fatal("resolved source inherited ambient proxy")
	}
	stats, err := engine.Execute(context.Background(), request(), &testSink{})
	if err != nil || stats.Rows != 2 || stats.Bytes != rowsBytes {
		t.Fatal(stats, err)
	}
}

func TestResolvedSourceRejectsUnsafeTransportAndRedirects(t *testing.T) {
	var redirected atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/other" {
			redirected.Add(1)
		}
		http.Redirect(w, r, "/other", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	valid := resolvedFixture(t, server)
	for _, mutate := range []func(*ResolvedSource){
		func(s *ResolvedSource) { s.URL = "http://db.example/?database=reports" },
		func(s *ResolvedSource) { s.URL += "&query=DROP+TABLE+data" },
		func(s *ResolvedSource) { s.TLS = &tls.Config{InsecureSkipVerify: true} },
		func(s *ResolvedSource) { s.Username = "reader:other" },
		func(s *ResolvedSource) { s.URL = server.URL + "/proxy?database=reports" },
	} {
		source := valid
		mutate(&source)
		if _, err := NewResolved(source, query.DefaultLimits()); err == nil {
			t.Fatal("unsafe resolved source accepted")
		}
	}
	engine, err := NewResolved(valid, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if _, err := engine.Execute(context.Background(), request(), &testSink{}); err == nil || redirected.Load() != 0 {
		t.Fatal("source redirect followed")
	}
	untrusted := valid
	untrusted.TLS = nil
	other, err := NewResolved(untrusted, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Execute(context.Background(), request(), &testSink{}); err == nil {
		t.Fatal("untrusted certificate accepted")
	}
}

func TestResolvedSourceCancellationStopsBlockedRead(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}))
	defer server.Close()
	engine, err := NewResolved(resolvedFixture(t, server), query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := engine.Execute(ctx, request(), &testSink{}); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not arrive")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatal("cancellation not reported")
		}
	case <-time.After(time.Second):
		t.Fatal("source read remained blocked")
	}
}
