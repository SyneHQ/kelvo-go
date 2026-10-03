// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/secrets"
	"go.yaml.in/yaml/v3"
)

const tlsTrustReadTimeout = 2 * time.Second
const tlsTrustMaxLifetime = time.Hour

var errTLSTrustUnavailable = errors.New("TLS trust unavailable")

// TLSTrustConfig opts an mTLS endpoint into bounded trust and revocation reloads.
// MinimumEpoch is an operator-managed rollback floor that survives restarts.
type TLSTrustConfig struct {
	File           string        `yaml:"file"`
	MinimumEpoch   uint64        `yaml:"minimum_epoch"`
	ReloadInterval time.Duration `yaml:"reload_interval,omitempty"`
}

func (c TLSTrustConfig) validate() error {
	if !filepath.IsAbs(c.File) || filepath.Clean(c.File) != c.File || c.File == string(filepath.Separator) || len(c.File) > 4096 || c.MinimumEpoch == 0 {
		return errors.New("TLS trust requires a private absolute file and positive minimum_epoch")
	}
	if c.ReloadInterval != 0 && (c.ReloadInterval < time.Second || c.ReloadInterval > time.Minute) {
		return errors.New("TLS trust reload_interval must be between one second and one minute")
	}
	return nil
}

type tlsTrustDocument struct {
	Version             int       `yaml:"version"`
	Epoch               uint64    `yaml:"epoch"`
	IssuedAt            time.Time `yaml:"issued_at"`
	ExpiresAt           time.Time `yaml:"expires_at"`
	RootsPEM            string    `yaml:"roots_pem"`
	RevokedIdentities   []string  `yaml:"revoked_identities,omitempty"`
	RevokedCertificates []string  `yaml:"revoked_certificates,omitempty"`
}

type tlsTrustSnapshot struct {
	epoch               uint64
	digest              [32]byte
	roots               *x509.CertPool
	revokedIdentities   map[string]bool
	revokedCertificates map[[32]byte]bool
	expires             time.Time
}

func validTrustURI(uri string) bool {
	if uri == GatewayIdentity {
		return true
	}
	parts := strings.Split(uri, "/")
	return len(parts) == 7 && parts[0] == "spiffe:" && parts[1] == "" && parts[2] == "kelvo" && parts[3] == "tenant" && clusterID.MatchString(parts[4]) && parts[5] == "worker" && clusterID.MatchString(parts[6])
}

func parseTLSTrust(raw []byte, now time.Time) (*tlsTrustSnapshot, error) {
	if len(raw) == 0 || len(raw) > secrets.MaxDocumentBytes {
		return nil, errTLSTrustUnavailable
	}
	// Reject YAML aliases and merge keys before decoding the bounded document.
	var node yaml.Node
	if yaml.Unmarshal(raw, &node) != nil || len(node.Content) != 1 || node.Content[0].Kind != yaml.MappingNode {
		return nil, errTLSTrustUnavailable
	}
	nodes := 0
	var check func(*yaml.Node, int) bool
	check = func(n *yaml.Node, depth int) bool {
		nodes++
		if depth > 8 || nodes > 5000 || n.Kind == yaml.AliasNode || n.Anchor != "" || n.Tag == "!!merge" {
			return false
		}
		for _, child := range n.Content {
			if !check(child, depth+1) {
				return false
			}
		}
		return true
	}
	if !check(&node, 0) {
		return nil, errTLSTrustUnavailable
	}
	var doc tlsTrustDocument
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if decoder.Decode(&doc) != nil || decoder.Decode(new(any)) != io.EOF || doc.Version != 1 || doc.Epoch == 0 || doc.IssuedAt.IsZero() || doc.ExpiresAt.IsZero() || doc.IssuedAt.After(now) || !now.Before(doc.ExpiresAt) || !doc.IssuedAt.Before(doc.ExpiresAt) || doc.ExpiresAt.Sub(doc.IssuedAt) > tlsTrustMaxLifetime || len(doc.RevokedIdentities) > 1024 || len(doc.RevokedCertificates) > 1024 {
		return nil, errTLSTrustUnavailable
	}
	s := &tlsTrustSnapshot{epoch: doc.Epoch, digest: sha256.Sum256(raw), roots: x509.NewCertPool(), revokedIdentities: map[string]bool{}, revokedCertificates: map[[32]byte]bool{}, expires: now.Add(doc.ExpiresAt.Sub(now))}
	for _, uri := range doc.RevokedIdentities {
		if !validTrustURI(uri) || s.revokedIdentities[uri] {
			return nil, errTLSTrustUnavailable
		}
		s.revokedIdentities[uri] = true
	}
	for _, text := range doc.RevokedCertificates {
		value, err := hex.DecodeString(text)
		if err != nil || len(value) != sha256.Size || strings.ToLower(text) != text {
			return nil, errTLSTrustUnavailable
		}
		var digest [32]byte
		copy(digest[:], value)
		if s.revokedCertificates[digest] {
			return nil, errTLSTrustUnavailable
		}
		s.revokedCertificates[digest] = true
	}
	remaining := []byte(doc.RootsPEM)
	seen := map[[32]byte]bool{}
	for len(bytes.TrimSpace(remaining)) > 0 {
		remaining = bytes.TrimSpace(remaining)
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, errTLSTrustUnavailable
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, errTLSTrustUnavailable
		}
		header, _, _ := bytes.Cut(remaining, []byte("\n"))
		if !bytes.Equal(bytes.TrimSuffix(header, []byte("\r")), []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, errTLSTrustUnavailable
		}
		if nested := bytes.Index(remaining[len(header):], []byte("-----BEGIN ")); nested >= 0 && nested+len(header) < len(remaining)-len(rest) {
			return nil, errTLSTrustUnavailable
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA || !cert.BasicConstraintsValid || cert.KeyUsage&x509.KeyUsageCertSign == 0 || len(cert.UnhandledCriticalExtensions) != 0 || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
			return nil, errTLSTrustUnavailable
		}
		hash := sha256.Sum256(cert.Raw)
		if seen[hash] || len(seen) >= 16 {
			return nil, errTLSTrustUnavailable
		}
		seen[hash] = true
		s.roots.AddCert(cert)
		expiry := now.Add(cert.NotAfter.Sub(now))
		if expiry.Before(s.expires) {
			s.expires = expiry
		}
		remaining = rest
	}
	if len(seen) == 0 {
		return nil, errTLSTrustUnavailable
	}
	return s, nil
}

// verifyPeer rebuilds certificate paths against current roots; cached handshake
// chains alone do not authorize a new HTTP request after trust changes.
func (s *tlsTrustSnapshot) verifyPeer(state tls.ConnectionState, usage x509.ExtKeyUsage, hostname, expectedURI string) (time.Time, error) {
	if !time.Now().Before(s.expires) || len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 || len(state.PeerCertificates) > 16 || !hasURI(state.PeerCertificates[0], expectedURI) {
		return time.Time{}, errTLSTrustUnavailable
	}
	for _, uri := range state.PeerCertificates[0].URIs {
		if s.revokedIdentities[uri.String()] {
			return time.Time{}, errTLSTrustUnavailable
		}
	}
	intermediates := x509.NewCertPool()
	for _, cert := range state.PeerCertificates[1:] {
		intermediates.AddCert(cert)
	}
	now := time.Now()
	chains, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: s.roots, Intermediates: intermediates, CurrentTime: now, DNSName: hostname, KeyUsages: []x509.ExtKeyUsage{usage}})
	if err != nil {
		return time.Time{}, errTLSTrustUnavailable
	}
	for _, chain := range chains {
		allowed := true
		expires := s.expires
		for _, cert := range chain {
			if s.revokedCertificates[sha256.Sum256(cert.Raw)] {
				allowed = false
				break
			}
			expiry := now.Add(cert.NotAfter.Sub(now))
			if expiry.Before(expires) {
				expires = expiry
			}
		}
		if allowed {
			return expires, nil
		}
	}
	return time.Time{}, errTLSTrustUnavailable
}

type tlsTrustRead struct {
	snapshot *tlsTrustSnapshot
	started  time.Time
	err      error
}
type tlsTrust struct {
	mu            sync.Mutex
	config        TLSTrustConfig
	snapshot      *tlsTrustSnapshot
	validUntil    time.Time
	highestEpoch  uint64
	highestDigest [32]byte
	closed        bool
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	read          tlsIdentityReader
}

func newTLSTrust(config TLSTrustConfig, reader tlsIdentityReader) (*tlsTrust, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if config.ReloadInterval == 0 {
		config.ReloadInterval = time.Second
	}
	if reader == nil {
		reader = secrets.ReadPrivateDocument
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &tlsTrust{config: config, ctx: ctx, cancel: cancel, done: make(chan struct{}), read: reader}
	initial := t.beginRead()
	timer := time.NewTimer(tlsTrustReadTimeout)
	defer timer.Stop()
	select {
	case result := <-initial:
		if !t.apply(result) {
			cancel()
			return nil, errTLSTrustUnavailable
		}
	case <-timer.C:
		cancel()
		return nil, errTLSTrustUnavailable
	}
	go t.run()
	return t, nil
}
func (t *tlsTrust) beginRead() <-chan tlsTrustRead {
	output := make(chan tlsTrustRead, 1)
	started := time.Now()
	ctx, cancel := context.WithTimeout(t.ctx, tlsTrustReadTimeout)
	go func() {
		defer cancel()
		raw, err := t.read(ctx, t.config.File, secrets.MaxDocumentBytes)
		var snapshot *tlsTrustSnapshot
		if err == nil {
			snapshot, err = parseTLSTrust(raw, time.Now())
		}
		clear(raw)
		output <- tlsTrustRead{snapshot: snapshot, started: started, err: err}
	}()
	return output
}
func (t *tlsTrust) apply(result tlsTrustRead) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := result.snapshot
	until := result.started.Add(t.config.ReloadInterval + tlsTrustReadTimeout)
	if s != nil && s.expires.Before(until) {
		until = s.expires
	}
	if t.closed || result.err != nil || s == nil || time.Since(result.started) >= tlsTrustReadTimeout || !time.Now().Before(until) || s.epoch < t.config.MinimumEpoch || s.epoch < t.highestEpoch || (s.epoch == t.highestEpoch && s.digest != t.highestDigest) {
		t.snapshot = nil
		t.validUntil = time.Time{}
		return false
	}
	t.snapshot, t.validUntil, t.highestEpoch, t.highestDigest = s, until, s.epoch, s.digest
	return true
}
func (t *tlsTrust) invalidate() {
	t.mu.Lock()
	t.snapshot = nil
	t.validUntil = time.Time{}
	t.mu.Unlock()
}
func (t *tlsTrust) current() (*tlsTrustSnapshot, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.snapshot == nil || !time.Now().Before(t.validUntil) {
		t.snapshot = nil
		t.validUntil = time.Time{}
		return nil, errTLSTrustUnavailable
	}
	return t.snapshot, nil
}
func (t *tlsTrust) ready() bool { _, err := t.current(); return err == nil }
func (t *tlsTrust) run() {
	defer close(t.done)
	tick := time.NewTicker(t.config.ReloadInterval)
	defer tick.Stop()
	var pending <-chan tlsTrustRead
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
		case <-t.ctx.Done():
			t.invalidate()
			return
		case <-tick.C:
			t.ready()
			if pending == nil {
				pending = t.beginRead()
				deadline = time.NewTimer(tlsTrustReadTimeout)
				timeout, timedOut = deadline.C, false
			}
		case result := <-pending:
			deadline.Stop()
			pending, timeout = nil, nil
			if timedOut {
				t.invalidate()
			} else {
				t.apply(result)
			}
		case <-timeout:
			timeout, timedOut = nil, true
			t.invalidate()
		}
	}
}
func (t *tlsTrust) close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.cancel()
	t.invalidate()
	<-t.done
}
