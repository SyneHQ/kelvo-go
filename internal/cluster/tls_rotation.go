// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/secrets"
)

const tlsIdentityReadTimeout = 2 * time.Second

var errTLSIdentityUnavailable = errors.New("TLS identity unavailable")

func validateTLSRotation(c TLSConfig) error {
	if c.Trust != nil {
		if c.CAFile != "" {
			return errors.New("TLS trust replaces ca_file")
		}
		if err := c.Trust.validate(); err != nil {
			return err
		}
	}

	if c.IdentityFile == "" {
		if c.ReloadInterval != 0 {
			return errors.New("TLS reload_interval requires identity_file")
		}
		return nil
	}
	if c.CertFile != "" || c.KeyFile != "" || !filepath.IsAbs(c.IdentityFile) || filepath.Clean(c.IdentityFile) != c.IdentityFile || len(c.IdentityFile) > 4096 || c.IdentityFile == string(filepath.Separator) {
		return errors.New("TLS identity_file requires an absolute private file and replaces cert_file/key_file")
	}
	if c.ReloadInterval != 0 && (c.ReloadInterval < time.Second || c.ReloadInterval > time.Hour) {
		return errors.New("TLS reload_interval must be between one second and one hour")
	}
	return nil
}

type tlsIdentityRead struct {
	certificate *tls.Certificate
	expires     time.Time
	started     time.Time
	err         error
}

type tlsIdentityReader func(context.Context, string, int) ([]byte, error)

// A reloader owns one snapshot and at most one filesystem reader. Filesystem
// syscalls cannot always be interrupted; a late reader cannot renew authority.
type tlsIdentity struct {
	mu          sync.Mutex
	config      TLSConfig
	ownURI      string
	usage       x509.ExtKeyUsage
	certificate *tls.Certificate
	validUntil  time.Time
	closed      bool
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	read        tlsIdentityReader
}

func newTLSIdentity(config TLSConfig, ownURI string, usage x509.ExtKeyUsage, reader tlsIdentityReader) (*tlsIdentity, error) {
	if err := validateTLSRotation(config); err != nil {
		return nil, err
	}
	if config.IdentityFile == "" {
		return nil, errors.New("TLS identity_file is required for rotation")
	}
	if config.ReloadInterval == 0 {
		config.ReloadInterval = time.Second
	}
	if reader == nil {
		reader = secrets.ReadPrivateDocument
	}
	ctx, cancel := context.WithCancel(context.Background())
	i := &tlsIdentity{config: config, ownURI: ownURI, usage: usage, ctx: ctx, cancel: cancel, done: make(chan struct{}), read: reader}
	initial := i.beginRead()
	timer := time.NewTimer(tlsIdentityReadTimeout)
	defer timer.Stop()
	select {
	case result := <-initial:
		if !i.apply(result) {
			cancel()
			return nil, errTLSIdentityUnavailable
		}
	case <-timer.C:
		cancel()
		return nil, errTLSIdentityUnavailable
	}
	go i.run()
	return i, nil
}

func parseTLSIdentity(raw []byte, ownURI string, usage x509.ExtKeyUsage, now time.Time) (*tls.Certificate, time.Time, error) {
	var certificatePEM, keyPEM []byte
	defer func() { clear(keyPEM) }()
	certificates, keys := 0, 0
	for len(bytes.TrimSpace(raw)) != 0 {
		raw = bytes.TrimSpace(raw)
		if !bytes.HasPrefix(raw, []byte("-----BEGIN ")) {
			return nil, time.Time{}, errTLSIdentityUnavailable
		}
		block, rest := pem.Decode(raw)
		header, _, _ := bytes.Cut(raw, []byte("\n"))
		if block == nil || len(block.Headers) != 0 || !bytes.Equal(bytes.TrimSuffix(header, []byte("\r")), []byte("-----BEGIN "+block.Type+"-----")) {
			return nil, time.Time{}, errTLSIdentityUnavailable
		}
		// pem.Decode can skip malformed leading blocks. Reject that behavior:
		// one call must consume precisely the first block, with no nested BEGIN.
		if nested := bytes.Index(raw[len(header):], []byte("-----BEGIN ")); nested >= 0 && nested+len(header) < len(raw)-len(rest) {
			clear(block.Bytes)
			return nil, time.Time{}, errTLSIdentityUnavailable
		}
		switch block.Type {
		case "CERTIFICATE":
			certificates++
			if certificates > 16 {
				return nil, time.Time{}, errTLSIdentityUnavailable
			}
			certificatePEM = append(certificatePEM, pem.EncodeToMemory(block)...)
		case "PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY":
			keys++
			if keys != 1 {
				clear(block.Bytes)
				return nil, time.Time{}, errTLSIdentityUnavailable
			}
			keyPEM = pem.EncodeToMemory(block)
			clear(block.Bytes)
		default:
			return nil, time.Time{}, errTLSIdentityUnavailable
		}
		raw = rest
	}
	if certificates == 0 || keys != 1 {
		return nil, time.Time{}, errTLSIdentityUnavailable
	}
	cert, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		return nil, time.Time{}, errTLSIdentityUnavailable
	}
	chain := make([]*x509.Certificate, len(cert.Certificate))
	var expires time.Time
	for n, der := range cert.Certificate {
		chain[n], err = x509.ParseCertificate(der)
		if err != nil || len(chain[n].UnhandledCriticalExtensions) != 0 || now.Before(chain[n].NotBefore) || !now.Before(chain[n].NotAfter) {
			return nil, time.Time{}, errTLSIdentityUnavailable
		}
		if expires.IsZero() || chain[n].NotAfter.Before(expires) {
			expires = chain[n].NotAfter
		}
		if n > 0 && chain[n-1].CheckSignatureFrom(chain[n]) != nil {
			return nil, time.Time{}, errTLSIdentityUnavailable
		}
	}
	cert.Leaf = chain[0]
	allowed := len(cert.Leaf.ExtKeyUsage) == 0 && len(cert.Leaf.UnknownExtKeyUsage) == 0
	for _, u := range cert.Leaf.ExtKeyUsage {
		allowed = allowed || u == usage || u == x509.ExtKeyUsageAny
	}
	if !allowed || cert.Leaf.IsCA || (ownURI != "" && !hasURI(cert.Leaf, ownURI)) || (cert.Leaf.KeyUsage != 0 && cert.Leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0) {
		return nil, time.Time{}, errTLSIdentityUnavailable
	}
	return &cert, expires, nil
}

func (i *tlsIdentity) beginRead() <-chan tlsIdentityRead {
	output := make(chan tlsIdentityRead, 1)
	started := time.Now()
	ctx, cancel := context.WithTimeout(i.ctx, tlsIdentityReadTimeout)
	go func() {
		defer cancel()
		raw, err := i.read(ctx, i.config.IdentityFile, secrets.MaxDocumentBytes)
		var cert *tls.Certificate
		var expires time.Time
		if err == nil {
			cert, expires, err = parseTLSIdentity(raw, i.ownURI, i.usage, time.Now())
		}
		clear(raw)
		output <- tlsIdentityRead{certificate: cert, expires: expires, started: started, err: err}
	}()
	return output
}

func (i *tlsIdentity) apply(result tlsIdentityRead) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	until := result.started.Add(i.config.ReloadInterval + tlsIdentityReadTimeout)
	if result.expires.Before(until) {
		until = result.started.Add(result.expires.Sub(result.started))
	}
	if i.closed || result.err != nil || result.certificate == nil || time.Since(result.started) >= tlsIdentityReadTimeout || !time.Now().Before(until) {
		i.certificate, i.validUntil = nil, time.Time{}
		return false
	}
	i.certificate, i.validUntil = result.certificate, until
	return true
}

func (i *tlsIdentity) invalidate() {
	i.mu.Lock()
	i.certificate, i.validUntil = nil, time.Time{}
	i.mu.Unlock()
}

func (i *tlsIdentity) current() (*tls.Certificate, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed || i.certificate == nil || !time.Now().Before(i.validUntil) {
		i.certificate, i.validUntil = nil, time.Time{}
		return nil, errTLSIdentityUnavailable
	}
	return i.certificate, nil
}

func (i *tlsIdentity) ready() bool {
	_, err := i.current()
	return err == nil
}

func (i *tlsIdentity) run() {
	defer close(i.done)
	tick := time.NewTicker(i.config.ReloadInterval)
	defer tick.Stop()
	var pending <-chan tlsIdentityRead
	var timeout <-chan time.Time
	var deadline *time.Timer
	var timedOut bool
	defer func() {
		if deadline != nil {
			deadline.Stop()
		}
	}()
	for {
		select {
		case <-i.ctx.Done():
			i.invalidate()
			return
		case <-tick.C:
			i.ready()
			if pending == nil {
				pending = i.beginRead()
				deadline = time.NewTimer(tlsIdentityReadTimeout)
				timeout, timedOut = deadline.C, false
			}
		case result := <-pending:
			deadline.Stop()
			pending, timeout = nil, nil
			if timedOut {
				i.invalidate()
			} else {
				i.apply(result)
			}
		case <-timeout:
			timeout, timedOut = nil, true
			i.invalidate()
		}
	}
}

func (i *tlsIdentity) close() {
	i.mu.Lock()
	i.closed = true
	i.mu.Unlock()
	i.cancel()
	i.invalidate()
	<-i.done
}

// ServerTLS owns opt-in certificate/key rotation. Close it after the server
// finishes draining. Optional trust rotation preserves normal peer verification.
type ServerTLS struct {
	Config   *tls.Config
	identity *tlsIdentity
	trust    *tlsTrust
	peerURI  string
}

func OpenServerTLS(c TLSConfig, ownURI, clientURI string) (*ServerTLS, error) {
	if err := validateTLSRotation(c); err != nil {
		return nil, err
	}
	runtime := &ServerTLS{peerURI: clientURI}
	started := false
	defer func() {
		if !started {
			runtime.Close()
		}
	}()
	var err error
	if c.Trust != nil {
		if clientURI == "" {
			return nil, errors.New("TLS trust rotation requires an mTLS listener")
		}
		runtime.trust, err = newTLSTrust(*c.Trust, nil)
		if err != nil {
			return nil, err
		}
	}
	if c.IdentityFile == "" {
		runtime.Config, err = buildServerTLS(c, ownURI, clientURI, runtime.trust)
		if err != nil {
			return nil, err
		}
	} else {
		runtime.identity, err = newTLSIdentity(c, ownURI, x509.ExtKeyUsageServerAuth, nil)
		if err != nil {
			return nil, err
		}
		identity := runtime.identity
		config := &tls.Config{MinVersion: tls.VersionTLS13, SessionTicketsDisabled: true}
		if clientURI != "" {
			config.ClientCAs, err = loadTLSRoots(c, runtime.trust)
			if err != nil {
				return nil, err
			}
			config.ClientAuth = tls.RequireAndVerifyClientCert
		}
		config.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return identity.current() }
		config.VerifyConnection = func(state tls.ConnectionState) error {
			if !identity.ready() {
				return errTLSIdentityUnavailable
			}
			if clientURI != "" && (len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 || !hasURI(state.PeerCertificates[0], clientURI)) {
				return errors.New("unexpected gateway identity")
			}
			return nil
		}
		runtime.Config = config
	}
	if runtime.trust != nil {
		runtime.Config, err = configureTLSTrustServer(runtime.Config, runtime.trust, clientURI)
		if err != nil {
			return nil, err
		}
	}
	started = true
	return runtime, nil
}

func (s *ServerTLS) Close() {
	if s.trust != nil {
		s.trust.close()
	}
	if s.identity != nil {
		s.identity.close()
	}
}

// Handler gates requests on existing keepalive connections too, and preserves
// the wrapped cluster handler's drain lifecycle. In-flight requests may finish.
func (s *ServerTLS) Handler(handler http.Handler) http.Handler {
	if s.identity != nil {
		handler = &tlsIdentityHandler{Handler: handler, identity: s.identity}
	}
	if s.trust != nil {
		handler = &tlsTrustHandler{Handler: handler, trust: s.trust, expectedURI: s.peerURI}
	}
	return handler
}

type tlsIdentityHandler struct {
	http.Handler
	identity *tlsIdentity
}

func (h *tlsIdentityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.identity.ready() {
		w.Header().Set("Connection", "close")
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	h.Handler.ServeHTTP(w, r)
}

func (h *tlsIdentityHandler) Drain(ctx context.Context) error {
	if drainer, ok := h.Handler.(interface{ Drain(context.Context) error }); ok {
		return drainer.Drain(ctx)
	}
	return nil
}

// The per-request gate applies even when a transport reuses a connection whose
// TLS handshake preceded invalidation. Already-started requests retain deadlines.
type tlsIdentityTransport struct {
	*http.Transport
	identity *tlsIdentity
}

func (t *tlsIdentityTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if !t.identity.ready() {
		return nil, errTLSIdentityUnavailable
	}
	return t.Transport.RoundTrip(r)
}
