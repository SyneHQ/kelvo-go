// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/secrets"
	"github.com/nats-io/nats.go"
)

const maxResponseBytes = 64 << 10

// ownedConn refuses later writes and clamps every deadline the NATS client
// applies, including its post-handshake deadline reset, to this attempt.
type ownedConn struct {
	net.Conn
	ctx      context.Context
	deadline time.Time
	once     sync.Once
	done     chan struct{}
	err      error
	sealed   atomic.Bool
}

func (c *ownedConn) Write(p []byte) (int, error) {
	if c.sealed.Load() {
		return 0, net.ErrClosed
	}
	select {
	case <-c.done:
		return 0, net.ErrClosed
	default:
	}
	if c.ctx.Err() != nil || !time.Now().Before(c.deadline) {
		return 0, context.DeadlineExceeded
	}
	return c.Conn.Write(p)
}

func (c *ownedConn) bounded(t time.Time) time.Time {
	if t.IsZero() || t.After(c.deadline) {
		return c.deadline
	}
	return t
}
func (c *ownedConn) SetDeadline(t time.Time) error      { return c.Conn.SetDeadline(c.bounded(t)) }
func (c *ownedConn) SetReadDeadline(t time.Time) error  { return c.Conn.SetReadDeadline(c.bounded(t)) }
func (c *ownedConn) SetWriteDeadline(t time.Time) error { return c.Conn.SetWriteDeadline(c.bounded(t)) }
func (c *ownedConn) Close() error {
	c.once.Do(func() { c.sealed.Store(true); c.err = c.Conn.Close(); close(c.done) })
	return c.err
}

// A generation is registered before dialing. Cancellation may precede the
// actual connection; a late connection is closed by its original dial owner.
type connectionGeneration struct {
	ctx            context.Context
	address        string
	deadline       time.Time
	mu             sync.Mutex
	sealed, dialed bool
	conn           *ownedConn
	closeErr       error
	dial           func(context.Context, string, string) (net.Conn, error)
	stop           func() bool
	callbackDone   chan struct{}
}

func newGeneration(ctx context.Context, address string) *connectionGeneration {
	deadline, _ := ctx.Deadline()
	g := &connectionGeneration{ctx: ctx, address: address, deadline: deadline,
		dial: (&net.Dialer{}).DialContext, callbackDone: make(chan struct{})}
	g.stop = context.AfterFunc(ctx, func() { defer close(g.callbackDone); _ = g.abort() })
	return g
}

func (g *connectionGeneration) Dial(network, address string) (net.Conn, error) {
	g.mu.Lock()
	if g.sealed || g.dialed || g.ctx.Err() != nil || address != g.address || network != "tcp" {
		g.mu.Unlock()
		return nil, ErrUnavailable
	}
	g.dialed = true
	g.mu.Unlock()
	raw, err := g.dial(g.ctx, network, address)
	if raw == nil {
		return nil, ErrUnavailable
	}
	conn := &ownedConn{Conn: raw, ctx: g.ctx, deadline: g.deadline, done: make(chan struct{})}
	g.mu.Lock()
	stale := g.sealed || g.ctx.Err() != nil || !time.Now().Before(g.deadline) || err != nil
	if !stale {
		g.conn = conn
	}
	g.mu.Unlock()
	if stale {
		if closeErr := conn.Close(); closeErr != nil {
			g.mu.Lock()
			g.closeErr = closeErr
			g.mu.Unlock()
		}
		return nil, ErrUnavailable
	}
	if err = conn.SetDeadline(g.deadline); err != nil {
		_ = conn.Close()
		return nil, ErrUnavailable
	}
	return conn, nil
}

func (g *connectionGeneration) abort() error {
	g.mu.Lock()
	g.sealed = true
	conn := g.conn
	g.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (g *connectionGeneration) finish() error {
	stopped := g.stop()
	err := g.abort()
	if !stopped {
		<-g.callbackDone
	}
	if err == nil {
		g.mu.Lock()
		err = g.closeErr
		g.mu.Unlock()
	}
	return err
}

type natsSession struct {
	op         *operation
	nc         *nats.Conn
	generation *connectionGeneration
	prefix     string
}

// Certificates need not be secret. The bounded descriptor must remain the
// same regular file throughout this read; private credentials use secrets'
// stronger descriptor-relative ownership and permissions checks below.
func readCertificate(ctx context.Context, path string) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ErrExpired
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > secrets.MaxDocumentBytes {
		return nil, ErrUnavailable
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, ErrUnavailable
	}
	opened, statErr := f.Stat()
	var raw []byte
	if statErr == nil && os.SameFile(before, opened) && opened.Mode().IsRegular() {
		raw, err = io.ReadAll(io.LimitReader(f, secrets.MaxDocumentBytes+1))
	} else {
		err = ErrUnavailable
	}
	after, afterErr := f.Stat()
	closeErr := f.Close()
	current, currentErr := os.Lstat(path)
	if err != nil || afterErr != nil || closeErr != nil || currentErr != nil || ctx.Err() != nil ||
		len(raw) == 0 || len(raw) > secrets.MaxDocumentBytes || !os.SameFile(before, after) ||
		!os.SameFile(before, current) || !current.Mode().IsRegular() || before.Size() != after.Size() ||
		!before.ModTime().Equal(after.ModTime()) || int64(len(raw)) != after.Size() {
		clear(raw)
		return nil, ErrUnavailable
	}
	return raw, nil
}

func openNATSSession(op *operation, config Config) (wireSession, error) {
	u, _ := url.Parse(config.URL)
	rootBytes, err := readCertificate(op.ctx, config.CAFile)
	if err != nil {
		return nil, ErrUnavailable
	}
	roots := x509.NewCertPool()
	validRoots := roots.AppendCertsFromPEM(rootBytes)
	clear(rootBytes)
	if !validRoots {
		return nil, ErrUnavailable
	}
	tlsConfig := &tls.Config{RootCAs: roots, ServerName: u.Hostname(), MinVersion: tls.VersionTLS13}
	if config.CertFile != "" {
		certBytes, err := readCertificate(op.ctx, config.CertFile)
		if err != nil {
			return nil, ErrUnavailable
		}
		keyBytes, err := secrets.ReadPrivateDocument(op.ctx, config.KeyFile, secrets.MaxDocumentBytes)
		if err != nil {
			clear(certBytes)
			return nil, ErrUnavailable
		}
		certificate, err := tls.X509KeyPair(certBytes, keyBytes)
		clear(certBytes)
		clear(keyBytes)
		if err != nil {
			return nil, ErrUnavailable
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	var credentialBytes []byte
	var credential nats.Option
	if config.CredentialsFile != "" {
		credentialBytes, err = secrets.ReadPrivateDocument(op.ctx, config.CredentialsFile, secrets.MaxDocumentBytes)
		if err != nil {
			return nil, ErrUnavailable
		}
		defer clear(credentialBytes)
		credential = nats.UserCredentialBytes(credentialBytes)
	} else {
		password := os.Getenv(config.PasswordEnv)
		if len(password) < 32 || len(password) > 4096 {
			return nil, ErrUnavailable
		}
		credential = nats.UserInfo(config.Username, password)
	}
	if !op.fresh() {
		return nil, ErrExpired
	}
	generation := newGeneration(op.ctx, u.Host)
	session := &natsSession{op: op, generation: generation, prefix: "_INBOX.kelvo-authfence." + config.ReplicaID}
	remaining := time.Until(generation.deadline)
	opts := []nats.Option{nats.Secure(tlsConfig), nats.Timeout(remaining), nats.SetCustomDialer(generation),
		nats.NoReconnect(), nats.ReconnectBufSize(-1), nats.IgnoreDiscoveredServers(),
		nats.NoCallbacksAfterClientClose(), nats.SyncQueueLen(1),
		// The default callback prints raw broker errors and subjects to stderr.
		// Protocol methods return bounded sentinels instead; this callback owns
		// no client or operation state.
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}), credential}
	nc, connectErr := nats.Connect(config.URL, opts...)
	session.nc = nc
	if connectErr != nil || nc == nil || !op.fresh() {
		return session, ErrUnavailable
	}
	if version := nc.ConnectedServerVersion(); version != "2.14.7" && version != "2.15.0" {
		return session, ErrUnavailable
	}
	return session, nil
}

func (s *natsSession) request(subject string, data []byte, header nats.Header) ([]byte, error) {
	if !s.op.fresh() || s.nc == nil {
		return nil, ErrExpired
	}
	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, ErrUnavailable
	}
	inbox := s.prefix + "." + hex.EncodeToString(nonce[:])
	sub, err := s.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer sub.Unsubscribe()
	if err = sub.SetPendingLimits(1, maxResponseBytes); err != nil {
		return nil, ErrUnavailable
	}
	if err = sub.AutoUnsubscribe(1); err != nil {
		return nil, ErrUnavailable
	}
	// SUB and PUB share the ordered connection. No extra request mux or retry
	// can route a different attempt's response into this literal subscription.
	if !s.op.fresh() {
		return nil, ErrExpired
	}
	if err = s.nc.PublishMsg(&nats.Msg{Subject: subject, Reply: inbox, Data: data, Header: header}); err != nil {
		return nil, ErrUnavailable
	}
	response, err := sub.NextMsgWithContext(s.op.ctx)
	if err != nil || response == nil || !s.op.fresh() {
		return nil, ErrUnavailable
	}
	return correlatedReply(inbox, response)
}

func correlatedReply(inbox string, response *nats.Msg) ([]byte, error) {
	if response == nil {
		return nil, ErrUnavailable
	}
	if response.Subject != inbox || response.Reply != "" || len(response.Data) > maxResponseBytes {
		return nil, ErrUnavailable
	}
	if len(response.Header) != 0 {
		// Core no-responders and header/status error replies never count as ACKs.
		return nil, ErrUnavailable
	}
	return append([]byte(nil), response.Data...), nil
}

func (s *natsSession) close() error {
	// Close the raw transport first: nats.Conn.Close is permitted to flush.
	err := s.generation.finish()
	if s.nc != nil {
		s.nc.Close()
	}
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return ErrUnknown
	}
	return nil
}
