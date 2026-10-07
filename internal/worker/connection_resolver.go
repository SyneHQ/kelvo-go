// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

const maxConnectionResponseBytes = resolver.MaxResponseBytes

// ConnectionResolverConfig belongs to the operator, never a query. Resolver
// URLs are also pinned into the authenticated principal's immutable policy.
type ConnectionResolverConfig struct {
	URL           string        `yaml:"url"`
	CAFile        string        `yaml:"ca_file"`
	CertFile      string        `yaml:"cert_file"`
	KeyFile       string        `yaml:"key_file"`
	Timeout       time.Duration `yaml:"timeout"`
	MaxConcurrent int           `yaml:"max_concurrent"`
}

func (c ConnectionResolverConfig) Validate() error {
	u, err := url.Parse(c.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || u.Path != resolver.QueryPath ||
		c.CAFile == "" || c.CertFile == "" || c.KeyFile == "" || c.Timeout < time.Second || c.Timeout > 30*time.Second || c.MaxConcurrent < 1 || c.MaxConcurrent > 64 {
		return errors.New("invalid on-demand connection resolver configuration")
	}
	return nil
}

// ConnectionResolver holds parent-only TLS identity and bounded HTTP admission.
// Its immutable configuration is safe to share across concurrent executions.
type ConnectionResolver struct {
	url    string
	client *http.Client
	slots  chan struct{}
}

// WithConnectionResolvers snapshots the issuer registry. Mutating a caller's
// map cannot retarget an already configured worker or a copied executor.
func (e *Executor) WithConnectionResolvers(resolvers map[string]*ConnectionResolver) (*Executor, error) {
	if e == nil || len(resolvers) > 64 {
		return nil, errors.New("invalid on-demand connection resolver registry")
	}
	copy := *e
	copy.connectionResolvers = make(map[string]*ConnectionResolver, len(resolvers))
	for issuer, resolver := range resolvers {
		if issuer == "" || len(issuer) > 128 || resolver == nil || resolver.url == "" || resolver.client == nil || resolver.slots == nil {
			return nil, errors.New("invalid on-demand connection resolver registry")
		}
		copy.connectionResolvers[issuer] = resolver
	}
	return &copy, nil
}

func (e *Executor) HasConnectionResolver(issuer, url string) bool {
	if e == nil {
		return false
	}
	resolver := e.connectionResolvers[issuer]
	return resolver != nil && resolver.url == url
}

func NewConnectionResolver(c ConnectionResolverConfig, workerIdentity string) (*ConnectionResolver, error) {
	bad := errors.New("on-demand connection resolver TLS configuration is unavailable")
	if err := c.Validate(); err != nil {
		return nil, err
	}
	identity, err := url.Parse(workerIdentity)
	if err != nil || identity.Scheme != "spiffe" || identity.Hostname() == "" {
		return nil, bad
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil || len(cert.Certificate) == 0 {
		return nil, bad
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil || leaf.IsCA || len(leaf.URIs) != 1 || leaf.URIs[0].String() != workerIdentity || time.Now().Before(leaf.NotBefore) || !time.Now().Before(leaf.NotAfter) {
		return nil, bad
	}
	clientAuth := false
	for _, usage := range leaf.ExtKeyUsage {
		clientAuth = clientAuth || usage == x509.ExtKeyUsageClientAuth
	}
	if !clientAuth {
		return nil, bad
	}
	pem, err := os.ReadFile(c.CAFile)
	if err != nil || len(pem) > maxConnectionResponseBytes {
		return nil, bad
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, bad
	}
	transport := &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{cert}},
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: c.Timeout,
		MaxResponseHeaderBytes: 16 << 10, MaxConnsPerHost: c.MaxConcurrent,
		// Credential delivery always performs a fresh peer-certificate check.
		// TLS sessions and pooled connections must not outlive trust validity.
		DisableKeepAlives: true, DisableCompression: true,
	}
	return &ConnectionResolver{
		url: c.URL, slots: make(chan struct{}, c.MaxConcurrent),
		client: &http.Client{Transport: transport, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}, nil
}

func (r *ConnectionResolver) Close() {
	if r != nil && r.client != nil {
		r.client.CloseIdleConnections()
	}
}

type connectionResolutionRequest = resolver.QueryRequest

func connectionUnavailable() error {
	return query.NewError("UNAVAILABLE", "On-demand connection resolution is unavailable")
}

func (r *ConnectionResolver) resolve(ctx context.Context, execution delegation.Execution, request query.Request) (connectionResolution, error) {
	var out connectionResolution
	if r == nil || r.client == nil || r.slots == nil {
		return out, connectionUnavailable()
	}
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	case <-ctx.Done():
		return out, ctx.Err()
	}
	request.Delegation = ""
	body, err := json.Marshal(connectionResolutionRequest{
		Delegation: execution.Token, Query: request, JobID: execution.Binding.JobID,
		WorkerID: execution.Binding.WorkerID, Owner: execution.Binding.Owner, Claim: execution.Binding.Claim,
	})
	if err != nil || len(body) > resolver.MaxQueryRequestBytes {
		return out, connectionUnavailable()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return out, connectionUnavailable()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		return out, connectionUnavailable()
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
		return out, query.NewError("PERMISSION_DENIED", "On-demand connection access denied")
	}
	mediaType, _, contentErr := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if resp.StatusCode != http.StatusOK || contentErr != nil || mediaType != "application/json" || resp.Header.Get("Content-Encoding") != "" || resp.ContentLength > maxConnectionResponseBytes {
		return out, connectionUnavailable()
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxConnectionResponseBytes+1))
	if err != nil || len(data) > maxConnectionResponseBytes {
		return out, connectionUnavailable()
	}
	var wire resolver.QueryResponse
	if delegation.StrictJSONLimit(data, &wire, maxConnectionResponseBytes) != nil {
		return connectionResolution{}, connectionUnavailable()
	}
	out = resolvedQuery(wire)
	now := time.Now()
	certificateExpiry, verified := resolverCertificateExpiry(resp.TLS, now)
	if !verified || out.Version != resolver.Version || out.DelegationSHA256 != delegation.Digest(execution.Token) || out.ValidUntil > execution.Claims.ExpiresAt || out.ValidUntil <= now.Unix() {
		return connectionResolution{}, connectionUnavailable()
	}
	// The handshake may precede slow response delivery. Keep the receipt bound
	// to a still-valid verified chain through the immediate pre-launch check.
	out.ValidUntil = min(out.ValidUntil, certificateExpiry.Unix())
	if out.ValidUntil <= now.Unix() {
		return connectionResolution{}, connectionUnavailable()
	}
	for _, value := range out.Secrets {
		if len(value) == 0 || len(value) > 32<<10 || strings.IndexByte(value, 0) >= 0 {
			return connectionResolution{}, connectionUnavailable()
		}
	}
	return out, nil
}

func resolverCertificateExpiry(state *tls.ConnectionState, now time.Time) (time.Time, bool) {
	var latest time.Time
	if state == nil || !state.HandshakeComplete || state.Version < tls.VersionTLS13 {
		return latest, false
	}
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 {
			continue
		}
		var expires time.Time
		valid := true
		for _, cert := range chain {
			if cert == nil || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
				valid = false
				break
			}
			if expires.IsZero() || cert.NotAfter.Before(expires) {
				expires = cert.NotAfter
			}
		}
		if valid && expires.After(latest) {
			latest = expires
		}
	}
	return latest, !latest.IsZero()
}
