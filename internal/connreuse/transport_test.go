// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package connreuse

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fixture struct {
	server   *httptest.Server
	dials    atomic.Int64
	requests atomic.Int64
	drop     atomic.Bool // close the connection after reading the next request
	mu       sync.Mutex
	seen     []string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{}
	f.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests.Add(1)
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.seen = append(f.seen, r.Method+" te="+strings.Join(r.TransferEncoding, ",")+" len="+string(rune('0'+len(body)))+" close="+r.Header.Get("Connection"))
		f.mu.Unlock()
		if f.drop.Swap(false) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	f.server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			f.dials.Add(1)
		}
	}
	f.server.StartTLS()
	t.Cleanup(f.server.Close)
	return f
}

func (f *fixture) client(t *testing.T, idle, maxAge time.Duration) (*http.Client, *Transport) {
	t.Helper()
	base := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: f.server.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs, MinVersion: tls.VersionTLS13, NextProtos: []string{"http/1.1"}},
		DialContext: (&net.Dialer{Timeout: time.Second}).DialContext, MaxConnsPerHost: 2, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}}
	transport, err := New(base, idle, maxAge)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport}, transport
}

func get(t *testing.T, client *http.Client, url string) error {
	t.Helper()
	response, err := client.Get(url)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if raw, _ := io.ReadAll(response.Body); string(raw) != "ok" {
		t.Fatalf("unexpected response %q", raw)
	}
	return nil
}

func TestSequentialRequestsShareOneConnection(t *testing.T) {
	f := newFixture(t)
	client, _ := f.client(t, time.Minute, time.Minute)
	for i := 0; i < 3; i++ {
		if err := get(t, client, f.server.URL); err != nil {
			t.Fatal(err)
		}
	}
	response, err := client.Post(f.server.URL, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if f.dials.Load() != 1 || f.requests.Load() != 4 {
		t.Fatalf("dials=%d requests=%d", f.dials.Load(), f.requests.Load())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// The empty body that blocks replay must not change what the server receives.
	if f.seen[0] != "GET te= len=0 close=" || f.seen[3] != "POST te= len=2 close=" {
		t.Fatalf("unexpected wire requests %q", f.seen)
	}
}

func TestConnectionIsRetiredAtMaxAge(t *testing.T) {
	f := newFixture(t)
	client, transport := f.client(t, time.Minute, time.Minute)
	clock := time.Now()
	transport.now = func() time.Time { return clock }
	if err := get(t, client, f.server.URL); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(59 * time.Second)
	if err := get(t, client, f.server.URL); err != nil || f.dials.Load() != 1 {
		t.Fatal("connection retired before its maximum age", err, f.dials.Load())
	}
	clock = clock.Add(2 * time.Second)
	// The over-age connection serves this one request, announced as its last.
	if err := get(t, client, f.server.URL); err != nil || f.dials.Load() != 1 {
		t.Fatal("retiring request failed or redialed", err, f.dials.Load())
	}
	if err := get(t, client, f.server.URL); err != nil || f.dials.Load() != 2 {
		t.Fatal("over-age connection carried another request", err, f.dials.Load())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seen[1] != "GET te= len=0 close=" || f.seen[2] != "GET te= len=0 close=close" {
		t.Fatalf("unexpected close signalling %q", f.seen)
	}
}

func TestAmbiguousFailureOnReusedConnectionIsNotReplayed(t *testing.T) {
	f := newFixture(t)
	client, _ := f.client(t, time.Minute, time.Minute)
	if err := get(t, client, f.server.URL); err != nil {
		t.Fatal(err)
	}
	f.drop.Store(true)
	if err := get(t, client, f.server.URL); err == nil {
		t.Fatal("a dropped request reported success")
	}
	// net/http alone would resend this GET on a new connection: three requests.
	if f.requests.Load() != 2 || f.dials.Load() != 1 {
		t.Fatalf("request was replayed: requests=%d dials=%d", f.requests.Load(), f.dials.Load())
	}
	if err := get(t, client, f.server.URL); err != nil || f.dials.Load() != 2 {
		t.Fatal("client did not recover on a new connection", err, f.dials.Load())
	}
}

func TestIdleConnectionIsClosed(t *testing.T) {
	f := newFixture(t)
	client, _ := f.client(t, 50*time.Millisecond, time.Minute)
	if err := get(t, client, f.server.URL); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := get(t, client, f.server.URL); err != nil || f.dials.Load() != 2 {
		t.Fatal("idle connection was reused", err, f.dials.Load())
	}
}

func TestNewRejectsUnsafeConfiguration(t *testing.T) {
	dial := (&net.Dialer{}).DialContext
	for name, base := range map[string]*http.Transport{
		"nil":            nil,
		"no dialer":      {MaxConnsPerHost: 1},
		"unbounded host": {DialContext: dial},
	} {
		if _, err := New(base, time.Second, time.Minute); err == nil {
			t.Fatal(name, "accepted")
		}
	}
	if _, err := New(&http.Transport{DialContext: dial, MaxConnsPerHost: 1}, time.Minute, time.Second); err == nil {
		t.Fatal("maximum age below idle timeout accepted")
	}
}
