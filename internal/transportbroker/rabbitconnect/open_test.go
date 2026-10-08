// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
)

func TestOpenVerifiedMTLSPreservesPayloadHalfCloseAndSourceAuthority(t *testing.T) {
	f := newFixture(t)
	type observed struct {
		request string
		state   tls.ConnectionState
	}
	seen := make(chan observed, 1)
	f.config.ProxyAddress = serveOnce(t, f.server, func(conn net.Conn) {
		request, err := readCONNECT(conn)
		if err != nil {
			t.Error("fixture could not read CONNECT")
			return
		}
		seen <- observed{request, conn.(*tls.Conn).ConnectionState()}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\nhello")
		body, err := io.ReadAll(conn)
		if err != nil {
			t.Error("fixture did not receive half-close")
			return
		}
		_, _ = conn.Write(body)
	})
	o, err := New(f.config, f.issuer())
	if err != nil {
		t.Fatal(err)
	}
	// Configuration buffers belong to the caller. Mutating them cannot alter the
	// already-constructed opener's CA trust, signing key or client certificate.
	f.config.RootCAPEM[0] = 'x'
	f.config.ClientCertificatePEM[0] = 'x'
	f.config.ClientKeyPEM[0] = 'x'
	f.config.IssuerPublicKey[0] ^= 1
	var calls atomic.Int32
	dial := o.dial
	o.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		calls.Add(1)
		if network != "tcp" || address != o.proxy {
			t.Error("opener changed its configured endpoint")
		}
		return dial(ctx, network, address)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := testRequest()
	conn, err := o.Open(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// A driver must not be able to replace the outer TLS transport with a
	// different connection through an exported embedded interface.
	replacement, unusedPeer := net.Pipe()
	defer replacement.Close()
	defer unusedPeer.Close()
	_ = replacement.SetDeadline(time.Now())
	if field := reflect.ValueOf(conn).Elem().FieldByName("Conn"); field.IsValid() && field.CanSet() {
		field.Set(reflect.ValueOf(replacement))
	}
	cancel() // Setup cancellation no longer owns an accepted broker connection.
	if conn.RemoteAddr().String() != request.Binding.Authority || conn.RemoteAddr().Network() != "tcp" {
		t.Fatal("proxy replaced original source authority")
	}
	var result observed
	select {
	case result = <-seen:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture did not record the accepted CONNECT")
	}
	if !result.state.HandshakeComplete || result.state.Version != tls.VersionTLS13 || len(result.state.VerifiedChains) == 0 || len(result.state.PeerCertificates) != 1 {
		t.Fatal("fixture did not verify mutual TLS")
	}
	lines := strings.Split(result.request, "\r\n")
	if len(lines) != 5 || lines[0] != "CONNECT "+request.Binding.Authority+" HTTP/1.1" || lines[1] != "Host: "+request.Binding.Authority || !strings.HasPrefix(lines[2], "Proxy-Authorization: Bearer ") {
		t.Fatal("CONNECT wire request changed")
	}
	token := strings.TrimPrefix(lines[2], "Proxy-Authorization: Bearer ")
	if o.verifyTicket(token, issueFor(o, request), time.Now()) != nil {
		t.Fatal("proxy received an unbound ticket")
	}
	first := make([]byte, 5)
	if _, err := io.ReadFull(conn, first); err != nil || string(first) != "hello" {
		t.Fatal("buffered CONNECT payload was lost", err)
	}
	if _, err := io.WriteString(conn, "data"); err != nil {
		t.Fatal(err)
	}
	if err := conn.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	last := make([]byte, 4)
	if _, err := io.ReadFull(conn, last); err != nil || string(last) != "data" {
		t.Fatal("half-close interrupted reverse traffic", err)
	}
	if !errors.Is(conn.(interface{ CloseRead() error }).CloseRead(), transportbroker.ErrUnsupported) || calls.Load() != 1 {
		t.Fatal("unsupported half-close or dial retry changed")
	}
}

func TestOpenClearsSetupDeadlineAfterJoiningCancellation(t *testing.T) {
	f := newFixture(t)
	f.config.SetupTimeout = 500 * time.Millisecond
	allow := make(chan struct{})
	defer close(allow)
	f.config.ProxyAddress = serveOnce(t, f.server, func(conn net.Conn) {
		if _, err := readCONNECT(conn); err != nil {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
		select {
		case <-allow:
		case <-time.After(3 * time.Second):
			return
		}
		_, _ = io.WriteString(conn, "x")
	})
	var deadline time.Time
	o, err := New(f.config, issueFunc(func(ctx context.Context, r IssueRequest) (string, error) {
		deadline, _ = ctx.Deadline()
		return signTicket(f.key, claimsFor(r)), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := o.Open(context.Background(), testRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	timer := time.NewTimer(time.Until(deadline) + 20*time.Millisecond)
	defer timer.Stop()
	<-timer.C
	// This is intentionally after the old deadline, with no new read deadline.
	select {
	case allow <- struct{}{}:
	case <-time.After(3 * time.Second):
		t.Fatal("fixture did not accept delayed source bytes")
	}
	var value [1]byte
	if _, err := io.ReadFull(conn, value[:]); err != nil || value[0] != 'x' {
		t.Fatal("setup deadline remained on accepted socket", err)
	}
}

func TestOpenRejectsNoncanonicalConnectResponses(t *testing.T) {
	for name, response := range map[string]string{
		"redirect":      "HTTP/1.1 307 Temporary Redirect\r\nLocation: https://elsewhere.invalid\r\n\r\n",
		"denied":        "HTTP/1.1 403 Forbidden\r\n\r\n",
		"other-version": "HTTP/1.0 200 Connection Established\r\n\r\n",
		"length":        "HTTP/1.1 200 Connection Established\r\nContent-Length: 0\r\n\r\n",
		"chunked":       "HTTP/1.1 200 Connection Established\r\nTransfer-Encoding: chunked\r\n\r\n",
		"duplicate":     "HTTP/1.1 200 Connection Established\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
		"oversize":      strings.Repeat("a", 5000) + "\r\n\r\n",
		"truncated":     "HTTP/1.1 200 Connection Established\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.config.ProxyAddress = serveOnce(t, f.server, func(conn net.Conn) {
				if _, err := readCONNECT(conn); err == nil {
					_, _ = io.WriteString(conn, response)
				}
			})
			calls := 0
			o, err := New(f.config, issueFunc(func(_ context.Context, r IssueRequest) (string, error) {
				calls++
				return signTicket(f.key, claimsFor(r)), nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			conn, err := o.Open(context.Background(), testRequest())
			if conn != nil || !errors.Is(err, transportbroker.ErrOpen) || calls != 1 {
				t.Fatal("malformed response accepted or retried", err)
			}
		})
	}
}

func TestOpenRequiresVerifiedProxyAndMutualTLS(t *testing.T) {
	for _, scenario := range []string{"wrong-host", "wrong-ca", "no-client-auth", "old-tls", "wrong-alpn"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			switch scenario {
			case "wrong-host":
				f.config.ProxyServerName = "other.test"
			case "wrong-ca":
				other := newFixture(t)
				f.config.RootCAPEM = other.config.RootCAPEM
			case "no-client-auth":
				f.server.ClientAuth = tls.NoClientCert
			case "old-tls":
				f.server.MinVersion = tls.VersionTLS12
				f.server.MaxVersion = tls.VersionTLS12
			case "wrong-alpn":
				f.server.NextProtos = []string{"h2"}
			}
			f.config.ProxyAddress = serveOnce(t, f.server, func(conn net.Conn) { _, _ = io.Copy(io.Discard, conn) })
			o, err := New(f.config, f.issuer())
			if err != nil {
				t.Fatal(err)
			}
			conn, err := o.Open(context.Background(), testRequest())
			if conn != nil || err == nil {
				t.Fatal("unverified proxy established a tunnel")
			}
		})
	}
}

func TestIssuerAndConnectShareOneBoundedSetupContext(t *testing.T) {
	for _, scenario := range []string{"issuer", "late-issuer", "tls", "connect"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			f.config.SetupTimeout = 100 * time.Millisecond
			if scenario == "tls" {
				f.config.ProxyAddress = serveOnce(t, nil, func(conn net.Conn) { _, _ = io.Copy(io.Discard, conn) })
			}
			if scenario == "connect" {
				f.config.ProxyAddress = serveOnce(t, f.server, func(conn net.Conn) {
					if _, err := readCONNECT(conn); err == nil {
						_, _ = io.Copy(io.Discard, conn)
					}
				})
			}
			calls, dials := 0, 0
			var issuedDeadline time.Time
			o, err := New(f.config, issueFunc(func(ctx context.Context, r IssueRequest) (string, error) {
				calls++
				issuedDeadline, _ = ctx.Deadline()
				if scenario == "issuer" || scenario == "late-issuer" {
					select {
					case <-ctx.Done():
					case <-time.After(3 * time.Second):
						return "", errors.New("fixture issuer did not receive its deadline")
					}
					if scenario == "issuer" {
						return "", ctx.Err()
					}
				}
				return signTicket(f.key, claimsFor(r)), nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			dial := o.dial
			o.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
				dials++
				deadline, _ := ctx.Deadline()
				if !deadline.Equal(issuedDeadline) {
					t.Error("TCP received a fresh setup budget")
				}
				return dial(ctx, network, address)
			}
			started := time.Now()
			conn, err := o.Open(context.Background(), testRequest())
			if conn != nil || !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 2*time.Second || calls != 1 {
				t.Fatal("setup exceeded its budget or lost cancellation", err)
			}
			want := 1
			if scenario == "issuer" || scenario == "late-issuer" {
				want = 0
			}
			if dials != want {
				t.Fatal("late issuance dialed or setup retried")
			}
		})
	}
}

func TestFailedSocketCloseTransfersCleanupCustody(t *testing.T) {
	f := newFixture(t)
	o, err := New(f.config, f.issuer())
	if err != nil {
		t.Fatal(err)
	}
	raw := &uncertainConn{}
	o.dial = func(context.Context, string, string) (net.Conn, error) { return raw, errors.New("private dial detail") }
	conn, err := o.Open(context.Background(), testRequest())
	if conn == nil || !errors.Is(err, transportbroker.ErrCleanup) || raw.closes.Load() != 1 {
		t.Fatal("uncertain socket custody was released")
	}
	if !errors.Is(conn.Close(), transportbroker.ErrCleanup) || raw.closes.Load() != 1 {
		t.Fatal("failed close lost its sticky result")
	}
}

type uncertainConn struct{ closes atomic.Int32 }

func (*uncertainConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (*uncertainConn) Write([]byte) (int, error)        { return 0, io.ErrClosedPipe }
func (c *uncertainConn) Close() error                   { c.closes.Add(1); return errors.New("private close detail") }
func (*uncertainConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*uncertainConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*uncertainConn) SetDeadline(time.Time) error      { return nil }
func (*uncertainConn) SetReadDeadline(time.Time) error  { return nil }
func (*uncertainConn) SetWriteDeadline(time.Time) error { return nil }
