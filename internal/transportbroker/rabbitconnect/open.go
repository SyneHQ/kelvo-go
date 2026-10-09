// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

var _ transportbroker.Opener = (*Opener)(nil)

// Open spends one setup budget across issuance, TCP, mTLS and CONNECT. The
// trusted issuer must obey its context. There are no retries or direct-source
// fallbacks. Returning a socket with an error transfers uncertain cleanup custody.
func (o *Opener) Open(parent context.Context, request transportbroker.OpenRequest) (net.Conn, error) {
	if o == nil || parent == nil || request.ValidateAt(time.Now()) != nil {
		return nil, transportbroker.ErrInvalid
	}
	binding := request.Binding
	if binding.Issuer != o.issuer || binding.Audience != o.audience || binding.ClusterTenant != o.cluster || binding.ServicePrincipal != o.principal {
		return nil, transportbroker.ErrScope
	}
	deadline := time.Now().Add(o.timeout)
	for _, until := range []time.Time{binding.ExpiresAt, o.certificateUntil} {
		if until.Before(deadline) {
			deadline = until
		}
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if time.Now().Before(o.certificateFrom) {
		return nil, transportbroker.ErrInvalid
	}
	issue := IssueRequest{Binding: binding, OpenID: request.ID, WorkerIdentity: o.identity, WorkerCertSHA256: o.certDigest}
	if o.sourceProof != nil {
		var err error
		issue, err = o.refreshSourceProof(ctx, issue)
		if err != nil {
			return nil, err
		}
	}
	token, err := o.tickets.Issue(ctx, issue)
	if err != nil || ctx.Err() != nil || o.verifyTicket(token, issue, time.Now()) != nil {
		return nil, setupError(ctx)
	}
	if o.acceptedTrust != nil && request.Purpose != transportbroker.Data {
		return nil, transportbroker.ErrScope
	}
	conn, err := o.connect(ctx, binding.Authority, token, o.acceptedTrust != nil, false)
	if err != nil {
		if conn == nil {
			return nil, err
		}
		return conn, err
	}
	if o.acceptedTrust != nil {
		accepted, err := o.acceptedConnection(conn, token, binding)
		if err != nil {
			if conn.closeErr != nil {
				return conn, err
			}
			return nil, err
		}
		return accepted, nil
	}
	return conn, nil
}

// connect performs only outer mTLS and CONNECT. Issuance and the chosen token's
// complete scope verification must succeed before this function is called.
func (o *Opener) connect(ctx context.Context, authority, token string, accepted, abort bool) (*tunnelConn, error) {
	raw, err := o.dial(ctx, "tcp", o.proxy)
	if raw == nil {
		return nil, setupError(ctx)
	}
	conn := &tunnelConn{conn: raw, raw: raw, authority: authority}
	if err != nil {
		return reject(conn, setupError(ctx))
	}
	completed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(completed)
		_ = conn.Close()
	})
	var joinOnce sync.Once
	join := func() {
		joinOnce.Do(func() {
			if !stop() {
				<-completed
			}
		})
	}
	defer join()
	setupDeadline, _ := ctx.Deadline()
	if conn.SetDeadline(setupDeadline) != nil {
		return reject(conn, setupError(ctx))
	}
	config := o.tls.Clone()
	var certificateRequested atomic.Bool
	config.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
		certificate := &config.Certificates[0]
		if info.SupportsCertificate(certificate) != nil {
			return nil, transportbroker.ErrOpen
		}
		certificateRequested.Store(true)
		return certificate, nil
	}
	secure := tls.Client(raw, config)
	conn.conn = secure
	if secure.HandshakeContext(ctx) != nil {
		return reject(conn, setupError(ctx))
	}
	state := secure.ConnectionState()
	if !state.HandshakeComplete || !certificateRequested.Load() || state.Version != tls.VersionTLS13 || len(state.VerifiedChains) == 0 ||
		(state.NegotiatedProtocol != "" && state.NegotiatedProtocol != "http/1.1") {
		return reject(conn, transportbroker.ErrOpen)
	}
	requestBytes := "CONNECT " + authority + " HTTP/1.1\r\nHost: " + authority + "\r\nProxy-Authorization: Bearer " + token + "\r\n"
	if accepted {
		requestBytes += transportissuer.AcceptedOpenHeader + ": " + transportissuer.AcceptedOpenRequired + "\r\n"
	}
	if abort {
		requestBytes += transportissuer.PostgresAbortHeader + ": " + transportissuer.PostgresAbortRequired + "\r\n"
	}
	requestBytes += "\r\n"
	if n, err := io.WriteString(secure, requestBytes); err != nil || n != len(requestBytes) {
		return reject(conn, setupError(ctx))
	}
	reader := bufio.NewReaderSize(secure, transportissuer.MaxAcceptedOpenBytes+128)
	// Rabbit v1 emits exactly this headerless response. ReadSlice is bounded;
	// reject redirects, bodies, framing headers, duplicate headers and extensions.
	line, err := reader.ReadSlice('\n')
	if err != nil || !bytes.Equal(line, []byte("HTTP/1.1 200 Connection Established\r\n")) {
		return reject(conn, setupError(ctx))
	}
	if accepted {
		receipt, err := readAcceptedHeader(reader)
		if err != nil {
			return reject(conn, err)
		}
		conn.acceptedReceipt = receipt
	} else {
		line, err = reader.ReadSlice('\n')
		if err != nil || !bytes.Equal(line, []byte("\r\n")) {
			return reject(conn, setupError(ctx))
		}
	}
	conn.reader = reader
	// Join a callback that may have begun closing the socket before clearing its
	// deadline. The returned connection is then owned by the broker's session.
	join()
	if ctx.Err() != nil || conn.SetDeadline(time.Time{}) != nil {
		return reject(conn, setupError(ctx))
	}
	return conn, nil
}

func setupError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return transportbroker.ErrOpen
}

func reject(conn *tunnelConn, err error) (*tunnelConn, error) {
	if conn.Close() != nil {
		return conn, transportbroker.ErrCleanup
	}
	return nil, err
}

type tunnelConn struct {
	conn            net.Conn
	raw             net.Conn
	reader          *bufio.Reader
	authority       string
	closeOnce       sync.Once
	closeErr        error
	closed          atomic.Bool
	acceptedReceipt string
}

func (c *tunnelConn) Read(p []byte) (int, error) {
	if c.reader != nil {
		return c.reader.Read(p)
	}
	return c.conn.Read(p)
}

// Closing the raw TCP socket cannot wait for a TLS close_notify write. Native
// source TLS still owns its own protocol shutdown; CloseWrite preserves outer TLS.
func (c *tunnelConn) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if err := c.raw.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			c.closeErr = transportbroker.ErrCleanup
		}
	})
	return c.closeErr
}

func (c *tunnelConn) RemoteAddr() net.Addr { return sourceAddress(c.authority) }
func (c *tunnelConn) CloseWrite() error {
	if conn, ok := c.conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return transportbroker.ErrUnsupported
}
func (*tunnelConn) CloseRead() error { return transportbroker.ErrUnsupported }

type sourceAddress string

func (sourceAddress) Network() string  { return "tcp" }
func (a sourceAddress) String() string { return string(a) }

func (c *tunnelConn) Write(p []byte) (int, error)        { return c.conn.Write(p) }
func (c *tunnelConn) LocalAddr() net.Addr                { return c.conn.LocalAddr() }
func (c *tunnelConn) SetDeadline(t time.Time) error      { return c.conn.SetDeadline(t) }
func (c *tunnelConn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *tunnelConn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }
