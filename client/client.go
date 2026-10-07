// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
)

const (
	maxRequestBytes = 256 << 10
	maxHandleBytes  = 4 << 10
	maxStatusBytes  = 256 << 10
	cleanupTimeout  = 2 * time.Second
)

var handleName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// Client is safe for concurrent calls. Admission is fail-fast; requests are
// never retried automatically. Close cancels active calls and bounded readers.
type Client struct {
	cfg              Config
	origin, token    string
	http             *http.Client
	transport        *http.Transport
	permits, control chan struct{}
	ctx              context.Context
	cancel           context.CancelFunc
	mu               sync.Mutex
	closed           bool
	active           sync.WaitGroup
	closeOnce        sync.Once
	closeDone        chan struct{}
}

// New requires HTTPS with certificate validation. It does not contact Kelvo.
func New(cfg Config) (*Client, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 8
	}
	if cfg.MaxControlConcurrent == 0 {
		cfg.MaxControlConcurrent = 8
	}
	if cfg.MaxRows == 0 {
		cfg.MaxRows = 1_000_000
	}
	if cfg.MaxDecodedBytes == 0 {
		cfg.MaxDecodedBytes = 256 << 20
	}
	if cfg.MaxWireBytes == 0 {
		cfg.MaxWireBytes = 256 << 20
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.Opaque != "" ||
		(u.Path != "" && u.Path != "/") || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		cfg.Timeout <= 0 || cfg.Timeout > time.Hour || cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > 256 || cfg.MaxControlConcurrent < 1 || cfg.MaxControlConcurrent > 256 ||
		cfg.MaxRows < 1 || cfg.MaxDecodedBytes < 1 || cfg.MaxWireBytes < 1 || cfg.MaxDecodedBytes > 1<<40 || cfg.MaxWireBytes > 1<<40 ||
		(cfg.BearerToken != "" && !validHeaderToken(cfg.BearerToken, 8192)) {
		return nil, failure("INVALID_CONFIG")
	}
	tlsConfig := &tls.Config{}
	if cfg.TLSConfig != nil {
		tlsConfig = cfg.TLSConfig.Clone()
	}
	if tlsConfig.InsecureSkipVerify || (tlsConfig.MaxVersion != 0 && tlsConfig.MaxVersion < tls.VersionTLS13) || tlsConfig.MinVersion > tls.VersionTLS13 {
		return nil, failure("INVALID_CONFIG")
	}
	tlsConfig.MinVersion = tls.VersionTLS13
	tlsConfig.NextProtos = []string{"http/1.1"}
	if tlsConfig.RootCAs != nil {
		tlsConfig.RootCAs = tlsConfig.RootCAs.Clone()
	}
	tlsConfig.ClientSessionCache = tls.NewLRUClientSessionCache((cfg.MaxConcurrent + cfg.MaxControlConcurrent) * 2)
	// A fresh HTTP/1 connection prevents net/http from replaying a single-consumer
	// GET on a reused connection. TLS sessions may still resume between calls.
	transport := &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig,
		DialContext:         (&net.Dialer{Timeout: min(cfg.Timeout, 10*time.Second)}).DialContext,
		TLSHandshakeTimeout: min(cfg.Timeout, 10*time.Second), ResponseHeaderTimeout: cfg.Timeout,
		DisableKeepAlives: true, DisableCompression: true, MaxConnsPerHost: cfg.MaxConcurrent + cfg.MaxControlConcurrent,
		MaxResponseHeaderBytes: 16 << 10, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{cfg: cfg, origin: "https://" + u.Host, token: cfg.BearerToken, transport: transport,
		http:    &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		permits: make(chan struct{}, cfg.MaxConcurrent), control: make(chan struct{}, cfg.MaxControlConcurrent), ctx: ctx, cancel: cancel, closeDone: make(chan struct{})}, nil
}

func validHeaderToken(value string, limit int) bool {
	return len(value) > 0 && len(value) <= limit && strings.IndexFunc(value, func(r rune) bool { return r <= 32 || r >= 127 }) < 0
}
func (c *Client) authority(auth Authority, family string) (Authority, error) {
	if c == nil {
		return auth, failure("CLIENT_CLOSED")
	}
	if auth.BearerToken == "" {
		auth.BearerToken = c.token
	}
	if auth.BearerToken != "" && !validHeaderToken(auth.BearerToken, 8192) {
		return auth, failure("INVALID_ARGUMENT")
	}
	switch family {
	case "query":
		if auth.OperationGrant != "" || auth.InputGrant != "" || (auth.Delegation != "" && !validHeaderToken(auth.Delegation, 32768)) {
			return auth, failure("INVALID_ARGUMENT")
		}
	case "operation":
		if auth.Delegation != "" || auth.InputGrant != "" || !validOperationGrant(auth.OperationGrant) {
			return auth, failure("INVALID_ARGUMENT")
		}
	case "input":
		if auth.Delegation != "" || auth.OperationGrant != "" || !validOperationGrant(auth.InputGrant) {
			return auth, failure("INVALID_ARGUMENT")
		}
	default:
		return auth, failure("INVALID_ARGUMENT")
	}
	return auth, nil
}
func (c *Client) begin(ctx context.Context, control bool) (context.Context, func(), error) {
	if c == nil {
		return nil, nil, failure("CLIENT_CLOSED")
	}
	if ctx == nil {
		return nil, nil, failure("INVALID_ARGUMENT")
	}
	if ctx.Err() != nil {
		return nil, nil, contextFailure(ctx)
	}
	permits := c.permits
	timeout := c.cfg.Timeout
	if control {
		permits = c.control
		timeout = min(timeout, 5*time.Second)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, failure("CLIENT_CLOSED")
	}
	select {
	case permits <- struct{}{}:
		c.active.Add(1)
	default:
		c.mu.Unlock()
		return nil, nil, failure("RESOURCE_EXHAUSTED")
	}
	c.mu.Unlock()
	bounded, cancel := context.WithTimeout(ctx, timeout)
	stop := context.AfterFunc(c.ctx, cancel)
	var once sync.Once
	return bounded, func() { once.Do(func() { stop(); cancel(); <-permits; c.active.Done() }) }, nil
}
func (c *Client) do(ctx context.Context, method, path string, auth Authority, body []byte) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.origin+path, reader)
	if err != nil {
		return nil, failure("INVALID_ARGUMENT")
	}
	req.GetBody = nil
	if auth.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+auth.BearerToken)
	}
	if auth.Delegation != "" {
		req.Header.Set("X-Kelvo-Delegation", auth.Delegation)
	}
	if auth.OperationGrant != "" {
		req.Header.Set("X-Kelvo-Operation-Grant", auth.OperationGrant)
	}
	if auth.InputGrant != "" {
		req.Header.Set("X-Kelvo-Operation-Input-Grant", auth.InputGrant)
	}
	req.Header.Set("Accept", "application/vnd.apache.arrow.stream, application/json")
	req.Header.Set("Accept-Encoding", "identity")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.send(req)
}

func (c *Client) send(req *http.Request) (*http.Response, error) {
	response, err := c.http.Do(req)
	if err != nil {
		if req.Context().Err() != nil {
			return nil, contextFailure(req.Context())
		}
		return nil, failure("UNAVAILABLE")
	}
	until, ok := responsePeerExpiry(response.TLS, time.Now())
	if !ok {
		response.Body.Close()
		return nil, failure("UNAVAILABLE")
	}
	// A certificate valid at handshake must not authorize a response beyond the
	// verified chain's expiry. Reading and closing remain bounded by the caller.
	ctx, cancel := context.WithDeadline(req.Context(), until)
	original := response.Body
	stop := context.AfterFunc(ctx, func() { _ = original.Close() })
	response.Body = &deadlineBody{ReadCloser: original, ctx: ctx, cancel: cancel, stop: stop}
	return response, nil
}
func responsePeerExpiry(state *tls.ConnectionState, now time.Time) (time.Time, bool) {
	if state == nil || !state.HandshakeComplete || state.Version < tls.VersionTLS13 || len(state.PeerCertificates) == 0 || state.PeerCertificates[0] == nil {
		return time.Time{}, false
	}
	leaf := state.PeerCertificates[0]
	var best time.Time
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 || chain[0] == nil || !chain[0].Equal(leaf) {
			continue
		}
		until := leaf.NotAfter
		valid := true
		for _, cert := range chain {
			if cert == nil || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
				valid = false
				break
			}
			if cert.NotAfter.Before(until) {
				until = cert.NotAfter
			}
		}
		if valid && until.After(best) {
			best = until
		}
	}
	return best, !best.IsZero()
}

type deadlineBody struct {
	io.ReadCloser
	ctx      context.Context
	cancel   context.CancelFunc
	stop     func() bool
	once     sync.Once
	closeErr error
}

func (b *deadlineBody) Read(p []byte) (int, error) {
	if b.ctx.Err() != nil {
		return 0, contextFailure(b.ctx)
	}
	n, err := b.ReadCloser.Read(p)
	if b.ctx.Err() != nil {
		return n, contextFailure(b.ctx)
	}
	return n, err
}
func (b *deadlineBody) Close() error {
	b.once.Do(func() { b.stop(); b.cancel(); b.closeErr = b.ReadCloser.Close() })
	return b.closeErr
}

func decodeJSON(response *http.Response, value any, limit int64) error {
	if !operationContentType(response, "application/json") || response.ContentLength > limit {
		return failure("PROTOCOL_ERROR")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil || int64(len(raw)) > limit || operations.DecodeStrict(raw, value, int(limit)) != nil {
		return failure("PROTOCOL_ERROR")
	}
	return nil
}
func contextFailure(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return failure("DEADLINE_EXCEEDED")
	}
	return failure("CANCELLED")
}
func statusFailure(status int) error {
	switch status {
	case http.StatusUnauthorized:
		return failure("UNAUTHENTICATED")
	case http.StatusForbidden:
		return failure("PERMISSION_DENIED")
	case http.StatusNotFound:
		return failure("NOT_FOUND")
	case http.StatusTooManyRequests, http.StatusRequestEntityTooLarge:
		return failure("RESOURCE_EXHAUSTED")
	case http.StatusBadRequest:
		return failure("INVALID_ARGUMENT")
	default:
		return failure("UNAVAILABLE")
	}
}
func (c *Client) defaultLimits() Limits {
	return Limits{c.cfg.MaxRows, c.cfg.MaxDecodedBytes, c.cfg.MaxWireBytes}
}
func (c *Client) validLimits(l Limits) bool {
	return l.MaxRows > 0 && l.MaxDecodedBytes > 0 && l.MaxWireBytes > 0 && l.MaxRows <= c.cfg.MaxRows && l.MaxDecodedBytes <= c.cfg.MaxDecodedBytes && l.MaxWireBytes <= c.cfg.MaxWireBytes
}

// Close waits at most three seconds. A non-cooperating sink still owns its
// admission until it returns, even if Close reports a timeout.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		c.mu.Unlock()
		go func() { c.active.Wait(); c.transport.CloseIdleConnections(); close(c.closeDone) }()
	})
	timer := time.NewTimer(cleanupTimeout + time.Second)
	defer timer.Stop()
	select {
	case <-c.closeDone:
		return nil
	case <-timer.C:
		return failure("UNAVAILABLE")
	}
}
