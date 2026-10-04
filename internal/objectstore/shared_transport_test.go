// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

func sharedTransportFixture(t *testing.T, handler http.Handler) (*SharedTransport, catalog.ObjectLocation) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	location := catalog.ObjectLocation{Provider: "s3", Endpoint: server.URL, Bucket: "snapshots", Prefix: "fixture", Region: "us-east-1"}
	transport, err := NewSharedTransport(location)
	if err != nil {
		t.Fatal(err)
	}
	trust := x509.NewCertPool()
	trust.AddCert(server.Certificate())
	transport.base.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: trust}
	t.Cleanup(transport.Close)
	return transport, location
}

func TestSharedTransportReusesPoolAcrossImmutableCredentialClients(t *testing.T) {
	var mu sync.Mutex
	var authorizations []string
	transport, location := sharedTransportFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("ETag", `"fixture"`)
		w.Header().Set("Content-Length", "1")
		w.Header().Set("x-amz-meta-kelvo-sha256", strings.Repeat("a", 64))
		w.WriteHeader(http.StatusOK)
	}))
	refs := catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_TEST_ID", SecretAccessKeyEnv: "KELVO_SOURCE_TEST_SECRET"}
	t.Setenv(refs.AccessKeyIDEnv, "first-id")
	t.Setenv(refs.SecretAccessKeyEnv, "fixture-secret")
	first, err := NewWithTransport(location, refs, transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(refs.AccessKeyIDEnv, "second-id")
	second, err := NewWithTransport(location, refs, transport)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	defer second.Close()
	if first.(*s3Client).http.Transport != second.(*s3Client).http.Transport {
		t.Fatal("logical client created a transport")
	}
	if _, err := first.Head(context.Background(), "fixture/value", ""); err != nil {
		t.Fatal(err)
	}
	first.Close()
	if _, err := second.Head(context.Background(), "fixture/value", ""); err != nil {
		t.Fatal("client Close stopped shared pool", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(authorizations) != 2 || !strings.Contains(authorizations[0], "Credential=first-id/") || !strings.Contains(authorizations[1], "Credential=second-id/") {
		t.Fatal("client did not retain its own credential snapshot")
	}
	other := location
	other.Prefix = "other"
	if _, err := NewWithTransport(other, refs, transport); err == nil {
		t.Fatal("transport accepted another namespace")
	}
}

func TestSharedTransportRetainsRequestSlotsUntilBodyClose(t *testing.T) {
	transport, location := sharedTransportFixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "x") }))
	bodies := make([]io.ReadCloser, 0, sharedRequestLimit)
	defer func() {
		for _, body := range bodies {
			_ = body.Close()
		}
	}()
	for range sharedRequestLimit {
		request, _ := http.NewRequest(http.MethodGet, location.Endpoint+"/fixture", nil)
		response, err := transport.RoundTrip(request)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, response.Body)
		if _, err := io.ReadAll(response.Body); err != nil {
			t.Fatal(err)
		}
	}
	request, _ := http.NewRequest(http.MethodGet, location.Endpoint+"/fixture", nil)
	if _, err := transport.RoundTrip(request); !errors.Is(err, errTransportCapacity) {
		t.Fatal("EOF released a still-borrowed body slot", err)
	}
	for _, body := range bodies {
		if err := body.Close(); err != nil {
			t.Fatal(err)
		}
	}
	transport.Close()
	select {
	case <-transport.Quiesced():
	default:
		t.Fatal("joined work did not quiesce")
	}
}

func TestSharedTransportBoundsOwnedDialsAndWaitsForLateReturn(t *testing.T) {
	location := catalog.ObjectLocation{Provider: "s3", Endpoint: "https://fixture.invalid", Bucket: "snapshots", Prefix: "fixture", Region: "us-east-1"}
	transport, err := NewSharedTransport(location)
	if err != nil {
		t.Fatal(err)
	}
	started, canceled := make(chan struct{}, sharedDialLimit), make(chan struct{}, sharedDialLimit)
	release, finished := make(chan struct{}), make(chan struct{}, sharedDialLimit)
	var once sync.Once
	defer func() {
		once.Do(func() { close(release) })
		transport.Close()
	}()
	transport.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		started <- struct{}{}
		<-ctx.Done()
		canceled <- struct{}{}
		<-release
		return nil, ctx.Err()
	}
	for range sharedDialLimit {
		go func() {
			_, _ = transport.dialContext(context.Background(), "tcp", "fixture.invalid:443")
			finished <- struct{}{}
		}()
	}
	for range sharedDialLimit {
		sharedTransportWait(t, started, "dial admission")
	}
	if _, err := transport.dialContext(context.Background(), "tcp", "fixture.invalid:443"); !errors.Is(err, errTransportCapacity) {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { transport.Close(); close(closed) }()
	for range sharedDialLimit {
		sharedTransportWait(t, canceled, "dial cancellation")
	}
	select {
	case <-closed:
		t.Fatal("stalled dial was declared quiescent")
	default:
	}
	if _, err := transport.dialContext(context.Background(), "tcp", "fixture.invalid:443"); !errors.Is(err, errClientClosed) {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	for range sharedDialLimit {
		sharedTransportWait(t, finished, "dial return")
	}
	sharedTransportWait(t, closed, "late dial joins")
}

func sharedTransportWait(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for", label)
	}
}

type sharedTransportPartialConn struct {
	net.Conn
	closes atomic.Int32
}

func (c *sharedTransportPartialConn) Close() error { c.closes.Add(1); return nil }

func TestSharedTransportClosesLateOrPartialDialConnections(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "partial error"
		if canceled {
			name = "late cancellation"
		}
		t.Run(name, func(t *testing.T) {
			transport, err := NewSharedTransport(catalog.ObjectLocation{Provider: "s3", Endpoint: "https://fixture.invalid", Bucket: "snapshots", Prefix: "fixture", Region: "us-east-1"})
			if err != nil {
				t.Fatal(err)
			}
			defer transport.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			partial := &sharedTransportPartialConn{}
			failure := errors.New("fixture dial failure")
			transport.dial = func(context.Context, string, string) (net.Conn, error) {
				if canceled {
					cancel()
					return partial, nil
				}
				return partial, failure
			}
			conn, err := transport.dialContext(ctx, "tcp", "fixture.invalid:443")
			want := failure
			if canceled {
				want = context.Canceled
			}
			if conn != nil || !errors.Is(err, want) || partial.closes.Load() != 1 {
				t.Fatal("dial abandoned connection custody", err, partial.closes.Load())
			}
		})
	}
}

type sharedTransportClosedBody struct{ closed bool }

func (*sharedTransportClosedBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *sharedTransportClosedBody) Close() error           { b.closed = true; return nil }

func TestSharedTransportRefusalReturnsUploadOwnership(t *testing.T) {
	transport, err := NewSharedTransport(catalog.ObjectLocation{Provider: "s3", Endpoint: "https://fixture.invalid", Bucket: "snapshots", Prefix: "fixture", Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	body := &sharedTransportClosedBody{}
	request, _ := http.NewRequest(http.MethodPut, "https://other.invalid/value", body)
	if _, err := transport.RoundTrip(request); err == nil || !body.closed {
		t.Fatal("refused request retained upload body", err)
	}
}
