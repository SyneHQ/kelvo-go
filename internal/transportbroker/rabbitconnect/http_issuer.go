// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

// HTTPIssuerConfig belongs to the trusted parent. Requests cannot select an
// endpoint, certificate, root CA or worker identity. No credential enters a URL.
type HTTPIssuerConfig struct {
	Endpoint, WorkerIdentity                      string
	RootCAPEM, ClientCertificatePEM, ClientKeyPEM []byte
	MaxInFlight                                   int
}

type HTTPIssuer struct {
	endpoint, identity, fingerprint string
	notBefore, notAfter             time.Time
	client                          *http.Client
	transport                       *http.Transport
	slots                           chan struct{}
	cleanupSlots                    chan struct{}
	ctx                             context.Context
	cancel                          context.CancelFunc
	mu                              sync.Mutex
	closed                          bool
	active                          sync.WaitGroup
	shutdownOnce                    sync.Once
	shutdownDone                    chan struct{}
}

var _ Issuer = (*HTTPIssuer)(nil)

func NewHTTPIssuer(c HTTPIssuerConfig) (*HTTPIssuer, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil || len(c.Endpoint) > 2048 || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || u.Path != transportissuer.Path || !textValue(c.WorkerIdentity, 512) || c.MaxInFlight < 1 || c.MaxInFlight > 64 {
		return nil, transportbroker.ErrInvalid
	}
	if len(c.RootCAPEM) == 0 || len(c.RootCAPEM) > 1<<20 || len(c.ClientCertificatePEM) == 0 || len(c.ClientCertificatePEM) > 64<<10 || len(c.ClientKeyPEM) == 0 || len(c.ClientKeyPEM) > 64<<10 {
		return nil, transportbroker.ErrInvalid
	}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	if transportbroker.ValidateAuthority(net.JoinHostPort(u.Hostname(), port)) != nil {
		return nil, transportbroker.ErrInvalid
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(c.RootCAPEM) {
		return nil, transportbroker.ErrInvalid
	}
	pair, err := tls.X509KeyPair(c.ClientCertificatePEM, c.ClientKeyPEM)
	if err != nil || len(pair.Certificate) == 0 {
		return nil, transportbroker.ErrInvalid
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil || len(leaf.URIs) != 1 || leaf.URIs[0].String() != c.WorkerIdentity || leaf.URIs[0].Scheme != "spiffe" {
		return nil, transportbroker.ErrInvalid
	}
	clientUsage := false
	for _, usage := range leaf.ExtKeyUsage {
		clientUsage = clientUsage || usage == x509.ExtKeyUsageClientAuth
	}
	if !clientUsage {
		return nil, transportbroker.ErrInvalid
	}
	from, until := leaf.NotBefore, leaf.NotAfter
	for _, der := range pair.Certificate[1:] {
		certificate, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, transportbroker.ErrInvalid
		}
		if certificate.NotBefore.After(from) {
			from = certificate.NotBefore
		}
		if certificate.NotAfter.Before(until) {
			until = certificate.NotAfter
		}
	}
	now := time.Now()
	if now.Before(from) || !now.Before(until) {
		return nil, transportbroker.ErrInvalid
	}
	sum := sha256.Sum256(leaf.Raw)
	ctx, cancel := context.WithCancel(context.Background())
	result := &HTTPIssuer{endpoint: c.Endpoint, identity: c.WorkerIdentity, fingerprint: hex.EncodeToString(sum[:]), notBefore: from, notAfter: until, slots: make(chan struct{}, c.MaxInFlight), cleanupSlots: make(chan struct{}, c.MaxInFlight), ctx: ctx, cancel: cancel, shutdownDone: make(chan struct{})}
	config := &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, RootCAs: roots, ServerName: u.Hostname(), Certificates: []tls.Certificate{pair}, NextProtos: []string{"http/1.1"}}
	tr := &http.Transport{Proxy: nil, DisableCompression: true, DisableKeepAlives: true, ForceAttemptHTTP2: false, MaxConnsPerHost: c.MaxInFlight, MaxResponseHeaderBytes: 8192, ResponseHeaderTimeout: MaxSetupTime}
	tr.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		raw, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		requested := false
		tlsConfig := config.Clone()
		tlsConfig.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			if info.SupportsCertificate(&pair) != nil {
				return nil, transportbroker.ErrOpen
			}
			requested = true
			return &pair, nil
		}
		secure := tls.Client(raw, tlsConfig)
		if err = secure.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, transportbroker.ErrOpen
		}
		state := secure.ConnectionState()
		if !requested || state.Version != tls.VersionTLS13 || len(state.VerifiedChains) == 0 || (state.NegotiatedProtocol != "" && state.NegotiatedProtocol != "http/1.1") {
			_ = raw.Close()
			return nil, transportbroker.ErrOpen
		}
		return secure, nil
	}
	result.transport = tr
	result.client = &http.Client{Transport: tr, Timeout: MaxSetupTime, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return result, nil
}

// Issue makes one bounded POST. It never retries, follows a redirect, accepts a
// compressed response or queues above capacity. Each request uses a fresh mTLS
// connection so a reused HTTP connection cannot retain expired peer authority.
func (i *HTTPIssuer) Issue(parent context.Context, r IssueRequest) (string, error) {
	if i == nil || parent == nil {
		return "", transportbroker.ErrInvalid
	}
	now := time.Now()
	request := transportbroker.OpenRequest{Binding: r.Binding, ID: r.OpenID, Purpose: transportbroker.Data}
	if request.ValidateAt(now) != nil || r.WorkerIdentity != i.identity || r.WorkerCertSHA256 != i.fingerprint || now.Before(i.notBefore) || !now.Before(i.notAfter) {
		return "", transportbroker.ErrScope
	}
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return "", transportbroker.ErrClosed
	}
	select {
	case i.slots <- struct{}{}:
	default:
		i.mu.Unlock()
		return "", transportbroker.ErrCapacity
	}
	i.active.Add(1)
	i.mu.Unlock()
	defer func() { <-i.slots; i.active.Done() }()
	deadline := now.Add(MaxSetupTime)
	for _, limit := range []time.Time{r.Binding.ExpiresAt, i.notAfter} {
		if limit.Before(deadline) {
			deadline = limit
		}
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	stop := context.AfterFunc(i.ctx, cancel)
	defer stop()
	if i.ctx.Err() != nil {
		cancel()
	}
	b, e := r.Binding, r.Binding.Execution
	wire := transportissuer.Request{PrivateSource: r.PrivateSource, RouteID: r.RouteID, TokenID: r.TokenID, BindingVersion: r.BindingVersion, Version: transportissuer.Version, Issuer: b.Issuer, Audience: b.Audience, ClusterTenant: b.ClusterTenant, ServicePrincipal: b.ServicePrincipal, Tenant: b.Tenant, Source: b.Source, SourceRevision: b.SourceRevision, Authority: b.Authority, ExpiresAt: b.ExpiresAt.Unix(), OpenID: r.OpenID, WorkerIdentity: r.WorkerIdentity, WorkerCertSHA256: r.WorkerCertSHA256, Execution: transportissuer.Execution{Kind: e.Kind, ID: e.ID, GrantSHA256: e.GrantSHA256, Worker: e.Worker, Owner: e.Owner, Claim: e.Claim}}
	return i.post(ctx, i.endpoint, wire)
}

func (i *HTTPIssuer) post(ctx context.Context, endpoint string, wire any) (string, error) {
	body, err := json.Marshal(wire)
	if err != nil || len(body) > transportissuer.MaxRequestBytes {
		return "", transportbroker.ErrInvalid
	}
	sum := sha256.Sum256(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", transportbroker.ErrInvalid
	}
	req.GetBody = nil // Prevent replay even if a transport retry becomes possible.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := i.client.Do(req)
	if err != nil {
		return "", setupError(ctx)
	}
	defer resp.Body.Close()
	if resp.TLS == nil || len(resp.TLS.VerifiedChains) == 0 {
		return "", transportbroker.ErrOpen
	}
	now := time.Now()
	for _, certificate := range resp.TLS.VerifiedChains[0] {
		if now.Before(certificate.NotBefore) || !now.Before(certificate.NotAfter) {
			return "", transportbroker.ErrOpen
		}
	}
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Encoding") != "" {
		return "", transportbroker.ErrOpen
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, transportissuer.MaxResponseBytes+1))
	var answer transportissuer.Response
	if err != nil || len(data) > transportissuer.MaxResponseBytes || strictJSON(data, &answer) != nil || answer.Version != transportissuer.Version || answer.RequestSHA256 != hex.EncodeToString(sum[:]) || len(answer.Token) == 0 || len(answer.Token) > maxTicketBytes {
		return "", transportbroker.ErrOpen
	}
	if ctx.Err() != nil {
		return "", setupError(ctx)
	}
	now = time.Now()
	for _, certificate := range resp.TLS.VerifiedChains[0] {
		if now.Before(certificate.NotBefore) || !now.Before(certificate.NotAfter) {
			return "", transportbroker.ErrOpen
		}
	}
	return answer.Token, nil
}

// Shutdown stops admission, cancels in-flight requests and joins their owners.
// A deadline returns an error; the same owners continue releasing their slots.
func (i *HTTPIssuer) Shutdown(ctx context.Context) error {
	if i == nil || ctx == nil {
		return transportbroker.ErrInvalid
	}
	i.mu.Lock()
	i.closed = true
	i.cancel()
	i.mu.Unlock()
	i.transport.CloseIdleConnections()
	i.shutdownOnce.Do(func() { go func() { i.active.Wait(); close(i.shutdownDone) }() })
	select {
	case <-i.shutdownDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
