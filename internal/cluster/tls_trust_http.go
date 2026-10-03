// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// configureTLSTrustServer selects an immutable CA pool for each handshake.
// Standard RequireAndVerifyClientCert verification runs before VerifyConnection.
func configureTLSTrustServer(base *tls.Config, trust *tlsTrust, expectedURI string) (*tls.Config, error) {
	if base == nil || trust == nil || !validTrustURI(expectedURI) || base.InsecureSkipVerify || base.GetConfigForClient != nil {
		return nil, errTLSTrustUnavailable
	}
	snapshot, err := trust.current()
	if err != nil {
		return nil, err
	}
	config := base.Clone()
	config.MinVersion = tls.VersionTLS13
	config.ClientAuth = tls.RequireAndVerifyClientCert
	config.ClientCAs = snapshot.roots
	config.SessionTicketsDisabled = true
	verify := base.VerifyConnection
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if verify != nil {
			if err := verify(state); err != nil {
				return err
			}
		}
		current, err := trust.current()
		if err != nil {
			return err
		}
		_, err = current.verifyPeer(state, x509.ExtKeyUsageClientAuth, "", expectedURI)
		return err
	}
	config.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		current, err := trust.current()
		if err != nil {
			return nil, err
		}
		selected := config.Clone()
		selected.GetConfigForClient = nil
		selected.ClientCAs = current.roots
		return selected, nil
	}
	return config, nil
}

type tlsTrustHandler struct {
	http.Handler
	trust       *tlsTrust
	expectedURI string
}

func (h *tlsTrustHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	snapshot, err := h.trust.current()
	if err == nil {
		if r.TLS == nil {
			err = errTLSTrustUnavailable
		} else {
			_, err = snapshot.verifyPeer(*r.TLS, x509.ExtKeyUsageClientAuth, "", h.expectedURI)
		}
	}
	if err != nil {
		w.Header().Set("Connection", "close")
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	h.Handler.ServeHTTP(w, r)
}
func (h *tlsTrustHandler) Drain(ctx context.Context) error {
	if drainer, ok := h.Handler.(interface{ Drain(context.Context) error }); ok {
		return drainer.Drain(ctx)
	}
	return nil
}

const maxTLSTrustPools = 4
const maxTLSTrustPeerChains = 16

type tlsTrustPool struct {
	transport *http.Transport
	digest    [32]byte
	identity  [32]byte
	peers     map[[32]byte]tls.ConnectionState
	requests  int
	hostname  string
	authority string
	retired   bool
}

// tlsTrustTransport retains normal TLS verification and separates idle pools by
// immutable trust document and local certificate. Reused peers are reverified
// before each new admission. Requests admitted earlier retain their deadlines.
type tlsTrustTransport struct {
	mu          sync.Mutex
	template    *http.Transport
	trust       *tlsTrust
	identity    *tlsIdentity
	expectedURI string
	active      *tlsTrustPool
	pools       map[*tlsTrustPool]bool
}

func newTLSTrustTransport(template *http.Transport, trust *tlsTrust, identity *tlsIdentity, expectedURI string) (*tlsTrustTransport, error) {
	if template == nil || template.TLSClientConfig == nil || template.TLSClientConfig.InsecureSkipVerify || trust == nil || !validTrustURI(expectedURI) || template.DialTLS != nil || template.DialTLSContext != nil {
		return nil, errTLSTrustUnavailable
	}
	if _, err := trust.current(); err != nil {
		return nil, err
	}
	return &tlsTrustTransport{template: template.Clone(), trust: trust, identity: identity, expectedURI: expectedURI, pools: map[*tlsTrustPool]bool{}}, nil
}
func (t *tlsTrustTransport) localIdentity() ([32]byte, error) {
	var digest [32]byte
	var cert *tls.Certificate
	if t.identity != nil {
		var err error
		cert, err = t.identity.current()
		if err != nil {
			return digest, err
		}
	} else {
		if len(t.template.TLSClientConfig.Certificates) != 1 {
			return digest, errTLSIdentityUnavailable
		}
		cert = &t.template.TLSClientConfig.Certificates[0]
	}
	if len(cert.Certificate) == 0 || len(cert.Certificate) > 16 {
		return digest, errTLSIdentityUnavailable
	}
	now := time.Now()
	for n, der := range cert.Certificate {
		parsed, err := x509.ParseCertificate(der)
		if err != nil || now.Before(parsed.NotBefore) || !now.Before(parsed.NotAfter) || len(parsed.UnhandledCriticalExtensions) != 0 || (n == 0 && (!hasURI(parsed, GatewayIdentity) || parsed.IsCA)) {
			return digest, errTLSIdentityUnavailable
		}
	}
	return tlsTrustChainDigest(cert.Certificate), nil
}
func (t *tlsTrustTransport) poolUsable(pool *tlsTrustPool, snapshot *tlsTrustSnapshot, identity [32]byte, authority string) bool {
	if pool == nil || pool.retired || pool.digest != snapshot.digest || pool.identity != identity || pool.authority != authority {
		return false
	}
	for _, peer := range pool.peers {
		if _, err := snapshot.verifyPeer(peer, x509.ExtKeyUsageServerAuth, pool.hostname, t.expectedURI); err != nil {
			return false
		}
	}
	return true
}
func (t *tlsTrustTransport) retire(pool *tlsTrustPool) {
	if pool == nil {
		return
	}
	pool.retired = true
	pool.transport.CloseIdleConnections()
	if pool.requests == 0 {
		delete(t.pools, pool)
	}
}
func (t *tlsTrustTransport) newPool(snapshot *tlsTrustSnapshot, identity [32]byte, request *http.Request) *tlsTrustPool {
	pool := &tlsTrustPool{digest: snapshot.digest, identity: identity, authority: tlsTrustAuthority(request), peers: map[[32]byte]tls.ConnectionState{}}
	transport := t.template.Clone()
	config := transport.TLSClientConfig.Clone()
	config.MinVersion = tls.VersionTLS13
	config.RootCAs = snapshot.roots
	config.ClientSessionCache = nil
	pool.hostname = normalizeTLSTrustHost(config.ServerName)
	if pool.hostname == "" {
		pool.hostname = normalizeTLSTrustHost(request.URL.Hostname())
	}
	config.ServerName = pool.hostname
	verify := config.VerifyConnection
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if verify != nil {
			if err := verify(state); err != nil {
				return err
			}
		}
		if _, err := t.localIdentity(); err != nil {
			return err
		}
		current, err := t.trust.current()
		if err != nil {
			return err
		}
		if _, err = current.verifyPeer(state, x509.ExtKeyUsageServerAuth, pool.hostname, t.expectedURI); err != nil {
			return err
		}
		chain := make([][]byte, len(state.PeerCertificates))
		for n, cert := range state.PeerCertificates {
			chain[n] = cert.Raw
		}
		key := tlsTrustChainDigest(chain)
		t.mu.Lock()
		defer t.mu.Unlock()
		if _, exists := pool.peers[key]; !exists && len(pool.peers) >= maxTLSTrustPeerChains {
			t.retire(pool)
			return errTLSTrustUnavailable
		}
		pool.peers[key] = state
		return nil
	}
	transport.TLSClientConfig = config
	pool.transport = transport
	t.pools[pool] = true
	return pool
}
func (t *tlsTrustTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil || request.URL.Scheme != "https" || request.URL.Hostname() == "" || request.URL.User != nil {
		closeTLSTrustRequest(request)
		return nil, errTLSTrustUnavailable
	}
	// Lock admission with pool selection. A later revocation never selects this
	// pool for another request; requests admitted here are already in flight.
	t.mu.Lock()
	snapshot, err := t.trust.current()
	var identity [32]byte
	if err == nil {
		identity, err = t.localIdentity()
	}
	if err != nil {
		t.retire(t.active)
		t.active = nil
		t.mu.Unlock()
		closeTLSTrustRequest(request)
		return nil, err
	}
	if !t.poolUsable(t.active, snapshot, identity, tlsTrustAuthority(request)) {
		t.retire(t.active)
		t.active = nil
		if len(t.pools) >= maxTLSTrustPools {
			t.mu.Unlock()
			closeTLSTrustRequest(request)
			return nil, errTLSTrustUnavailable
		}
		t.active = t.newPool(snapshot, identity, request)
	}
	pool := t.active
	pool.requests++
	t.mu.Unlock()
	response, err := pool.transport.RoundTrip(request)
	if err != nil {
		t.release(pool)
		return nil, err
	}
	response.Body = &tlsTrustBody{ReadCloser: response.Body, release: func() { t.release(pool) }}
	return response, nil
}
func (t *tlsTrustTransport) release(pool *tlsTrustPool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pool.requests--
	if pool.retired {
		pool.transport.CloseIdleConnections()
		if pool.requests == 0 {
			delete(t.pools, pool)
		}
	}
}
func (t *tlsTrustTransport) CloseIdleConnections() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for pool := range t.pools {
		t.retire(pool)
	}
	t.active = nil
}

type tlsTrustBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (b *tlsTrustBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.once.Do(b.release)
	}
	return n, err
}
func (b *tlsTrustBody) Close() error { err := b.ReadCloser.Close(); b.once.Do(b.release); return err }

func tlsTrustChainDigest(chain [][]byte) [32]byte {
	hash := sha256.New()
	var size [8]byte
	for _, der := range chain {
		binary.BigEndian.PutUint64(size[:], uint64(len(der)))
		hash.Write(size[:])
		hash.Write(der)
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func closeTLSTrustRequest(request *http.Request) {
	if request != nil && request.Body != nil {
		request.Body.Close()
	}
}

func normalizeTLSTrustHost(host string) string {
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}
func tlsTrustAuthority(request *http.Request) string {
	port := request.URL.Port()
	if port == "" {
		port = "443"
	}
	return "https://" + net.JoinHostPort(normalizeTLSTrustHost(request.URL.Hostname()), port)
}
