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
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/diagnostic"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

var _ transportbroker.Opener = (*Opener)(nil)

// Open bounds fresh source resolution separately from issuance, TCP, mTLS and CONNECT. The
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
	deadline := binding.ExpiresAt
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
		refreshContext, stopRefresh := context.WithTimeout(ctx, o.sourceProof.RefreshTimeout)
		issue, err = o.refreshSourceProof(refreshContext, issue)
		stopRefresh()
		if err != nil {
			return nil, err
		}
	}
	// The operation and certificate deadlines remain authoritative across both phases.
	// A slow resolver does not consume the short physical-connection setup budget.
	if ctx.Err() != nil || request.ValidateAt(time.Now()) != nil || !time.Now().Before(o.certificateUntil) {
		return nil, setupError(ctx)
	}
	ctx, stopSetup := context.WithTimeout(ctx, o.timeout)
	defer stopSetup()
	diagnostic.Record(ctx, diagnostic.TicketIssue, diagnostic.Started, 0)
	token, err := o.tickets.Issue(ctx, issue)
	if err != nil || ctx.Err() != nil {
		recordDiagnosticFailure(ctx, diagnostic.TicketIssue, err, 0)
		return nil, setupError(ctx)
	}
	diagnostic.Record(ctx, diagnostic.TicketIssue, diagnostic.Succeeded, 0)
	diagnostic.Record(ctx, diagnostic.TicketVerified, diagnostic.Started, 0)
	if o.verifyTicket(token, issue, time.Now()) != nil {
		diagnostic.Record(ctx, diagnostic.TicketVerified, diagnostic.ScopeDenied, 0)
		return nil, setupError(ctx)
	}
	diagnostic.Record(ctx, diagnostic.TicketVerified, diagnostic.Succeeded, 0)
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
		diagnostic.Record(ctx, diagnostic.AcceptedReceipt, diagnostic.Started, 0)
		accepted, err := o.acceptedConnection(conn, token, binding)
		diagnostic.Record(ctx, diagnostic.AcceptedReceipt, diagnostic.ResultFor(ctx, err), 0)
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
func (o *Opener) connect(ctx context.Context, authority, token string, accepted, abort bool) (result *tunnelConn, resultErr error) {
	stage := diagnostic.ProxyTCP
	status := 0
	diagnostic.Record(ctx, stage, diagnostic.Started, status)
	defer func() {
		if resultErr != nil {
			diagnostic.Record(ctx, stage, diagnostic.ResultFor(ctx, resultErr), status)
		}
	}()
	raw, err := o.dial(ctx, "tcp", o.proxy)
	if raw == nil {
		return nil, setupError(ctx)
	}
	conn := &tunnelConn{conn: raw, raw: raw, authority: authority}
	if err != nil {
		return reject(conn, setupError(ctx))
	}
	diagnostic.Record(ctx, stage, diagnostic.Succeeded, 0)
	stage = diagnostic.ProxyTLS
	diagnostic.Record(ctx, stage, diagnostic.Started, 0)
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
	diagnostic.Record(ctx, stage, diagnostic.Succeeded, 0)
	stage = diagnostic.ConnectResponse
	diagnostic.Record(ctx, stage, diagnostic.Started, 0)
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
	status = diagnosticHTTPStatus(line)
	if err != nil || !bytes.Equal(line, []byte("HTTP/1.1 200 Connection Established\r\n")) {
		return reject(conn, setupError(ctx))
	}
	if accepted {
		diagnostic.Record(ctx, stage, diagnostic.Succeeded, status)
		stage = diagnostic.AcceptedReceipt
		diagnostic.Record(ctx, stage, diagnostic.Started, 0)
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
	diagnostic.Record(ctx, stage, diagnostic.Succeeded, status)
	return conn, nil
}

// recordDiagnosticFailure does not allocate or classify errors when disabled.
func recordDiagnosticFailure(ctx context.Context, stage diagnostic.Stage, err error, status int) {
	if diagnostic.FromContext(ctx) == nil {
		return
	}
	if err == nil {
		err = transportbroker.ErrOpen
	}
	diagnostic.Record(ctx, stage, diagnostic.ResultFor(ctx, err), status)
}

// diagnosticHTTPStatus retains only a three-digit status from a bounded HTTP/1.1 line.
func diagnosticHTTPStatus(line []byte) int {
	if len(line) < 13 || !bytes.HasPrefix(line, []byte("HTTP/1.1 ")) || line[12] != ' ' {
		return 0
	}
	status := 0
	for _, digit := range line[9:12] {
		if digit < '0' || digit > '9' {
			return 0
		}
		status = status*10 + int(digit-'0')
	}
	if status < 100 || status > 599 {
		return 0
	}
	return status
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
