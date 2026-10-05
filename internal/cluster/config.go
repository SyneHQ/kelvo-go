// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/secrets"
	"go.yaml.in/yaml/v3"
)

var clusterID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

func LoadGateway(path string) (GatewayConfig, error) {
	var c GatewayConfig
	base, err := decodeConfig(path, &c)
	if err != nil {
		return c, err
	}
	if err := c.Audit.Validate(); err != nil {
		return c, err
	}
	if c.Tracing != nil {
		if err := c.Tracing.Validate(); err != nil {
			return c, err
		}
	}
	resolveTLS(base, &c.TLS)
	resolveTLS(base, &c.WorkerTLS)
	if c.TLS.Trust != nil {
		return c, errors.New("TLS trust rotation requires an mTLS listener")
	}
	if err := validateTLSRotation(c.TLS); err != nil {
		return c, err
	}
	if err := validateTLSRotation(c.WorkerTLS); err != nil {
		return c, err
	}
	if c.Authentication != nil {
		c.Authentication.KeysFile = relativePath(base, c.Authentication.KeysFile)
		if c.Authentication.Authority != nil {
			resolveNATS(base, &c.Authentication.Authority.NATS)
		}
	}
	if err := validateGatewayAuthentication(&c); err != nil {
		return c, err
	}
	if c.Listen == "" || len(c.Tenants) == 0 || len(c.Tenants) > 256 || c.MaxQueries < 1 || c.MaxQueries > 65536 || c.MaxConcurrent < 1 || c.MaxConcurrent > 4096 || c.MaxHTTPRequests < 1 || c.MaxHTTPRequests > 4096 {
		return c, errors.New("invalid gateway capacity or listener")
	}
	totalQueries, totalConcurrent := 0, 0
	seen := map[string]bool{}
	for i := range c.Tenants {
		t := &c.Tenants[i]
		if err := ValidatePolicy(t.Policy); err != nil {
			return c, err
		}
		if seen[t.Policy.TenantID] {
			return c, errors.New("tenant identities must be unique and have an API token reference")
		}
		seen[t.Policy.TenantID] = true
		resolveNATS(base, &t.NATS)
		if len(t.Workers) != len(t.Policy.Workers) {
			return c, errors.New("worker endpoints must exactly match the provisioned worker policy")
		}
		ids := map[string]bool{}
		for _, w := range t.Workers {
			if t.Policy.Workers[w.ID] == 0 || ids[w.ID] || !validEndpoint(w.URL) {
				return c, errors.New("invalid or duplicate worker endpoint")
			}
			ids[w.ID] = true
		}
		totalQueries += t.Policy.MaxQueries
		for _, count := range t.Policy.Workers {
			totalConcurrent += count
		}
	}
	if totalQueries > c.MaxQueries || totalConcurrent > c.MaxConcurrent {
		return c, errors.New("tenant budgets exceed the configured cluster capacity")
	}
	if err := validateGatewayExports(c); err != nil {
		return c, err
	}
	return c, nil
}

func LoadNode(path string) (NodeConfig, error) {
	var c NodeConfig
	base, err := decodeConfig(path, &c)
	if err != nil {
		return c, err
	}
	if err := validateNodeAudit(c); err != nil {
		return c, err
	}
	if c.ScratchDirectory != "" && (!filepath.IsAbs(c.ScratchDirectory) || filepath.Clean(c.ScratchDirectory) != c.ScratchDirectory || c.ScratchDirectory == string(filepath.Separator) || len(c.ScratchDirectory) > 4096) {
		return c, errors.New("scratch_directory must be a clean absolute private directory")
	}
	if c.Resources != nil {
		if _, err := c.Resources.NewPool(); err != nil {
			return c, err
		}
		if !c.Resources.Fits(c.Policy.Limits, false) {
			return c, errors.New("query reservation exceeds node resources")
		}
	}
	if c.Containment != nil {
		if err := c.Containment.Validate(c.Resources, c.Policy.Limits); err != nil {
			return c, err
		}
		if c.ScratchDirectory == "" || c.SandboxPath == "" {
			return c, errors.New("containment requires managed scratch and the sandbox launcher")
		}
	}
	resolveTLS(base, &c.TLS)
	if err := validateTLSRotation(c.TLS); err != nil {
		return c, err
	}
	resolveNATS(base, &c.NATS)
	if len(c.RequiredDatasets) > 64 {
		return c, errors.New("too many required datasets")
	}
	seenRequired := map[string]bool{}
	for _, id := range c.RequiredDatasets {
		if !catalog.ValidID(id) || seenRequired[id] {
			return c, errors.New("invalid required dataset")
		}
		seenRequired[id] = true
	}
	if c.Tracing != nil {
		if err := c.Tracing.Validate(); err != nil {
			return c, err
		}
	}
	if c.SourceHealth != nil {
		if err := c.SourceHealth.Validate(); err != nil {
			return c, err
		}
	}
	if c.History != nil {
		if err := c.History.Validate(); err != nil {
			return c, err
		}
	}
	if c.Secrets != nil {
		for _, key := range c.Secrets.Keys() {
			if err := catalog.ValidateEnvironment(key); err != nil {
				return c, errors.New("invalid secret environment reference")
			}
		}
		for key, path := range c.Secrets.Files {
			c.Secrets.Files[key] = relativePath(base, path)
		}
		for name, provider := range c.Secrets.Providers {
			provider.CredentialsFile = relativePath(base, provider.CredentialsFile)
			c.Secrets.Providers[name] = provider
		}
		provider, err := secrets.New(*c.Secrets)
		if err != nil {
			return c, errors.New("invalid source secret provider configuration")
		}
		_ = provider.Close()
	}
	c.CatalogFile = relativePath(base, c.CatalogFile)
	c.SandboxPath = relativePath(base, c.SandboxPath)
	if err = ValidatePolicy(c.Policy); err != nil {
		return c, err
	}
	if err = validateNodeExports(c); err != nil {
		return c, err
	}
	if c.Listen == "" || c.Policy.Workers[c.WorkerID] < 1 || c.CatalogFile == "" || c.SandboxPath == "" {
		return c, errors.New("node requires a listener, provisioned identity, catalog and sandbox launcher")
	}
	info, err := os.Stat(c.SandboxPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0111 == 0 || info.Mode()&0022 != 0 {
		return c, errors.New("sandbox launcher must be executable and not writable by group or others")
	}
	return c, nil
}

func ValidatePolicy(p Policy) error {
	if err := validatePrincipalPolicy(p.Access); err != nil {
		return err
	}
	if err := ValidateSourceQuotas(p.SourceQuotas); err != nil {
		return err
	}

	if !clusterID.MatchString(p.TenantID) || p.MaxQueries < 1 || p.MaxQueries > 512 || p.LeaseDuration < 5*time.Second || p.LeaseDuration > time.Minute || p.JobTTL < p.Limits.Timeout+2*p.LeaseDuration || p.JobTTL > 4*time.Hour || (p.Replicas != 1 && p.Replicas != 3) || len(p.Workers) == 0 || len(p.Workers) > 64 {
		return errors.New("invalid tenant scheduling policy")
	}
	if err := p.Limits.Validate(); err != nil {
		return err
	}
	for id, count := range p.Workers {
		if !clusterID.MatchString(id) || count < 1 || count > 64 {
			return errors.New("invalid worker capacity")
		}
	}
	return validateExportPolicy(p)
}

func validEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && (u.Path == "" || u.Path == "/") && u.RawQuery == "" && u.Fragment == "" && u.Opaque == ""
}

func decodeConfig(path string, into any) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("cluster configuration is unavailable")
	}
	f, err := os.Open(abs)
	if err != nil {
		return "", errors.New("cluster configuration is unavailable")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(b) > 1<<20 {
		return "", errors.New("cluster configuration exceeds its size limit or cannot be read")
	}
	var root yaml.Node
	if yaml.Unmarshal(b, &root) != nil || len(root.Content) != 1 || root.Content[0].Kind != yaml.MappingNode {
		return "", errors.New("cluster configuration must be a YAML mapping")
	}
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	if d.Decode(into) != nil || d.Decode(new(any)) != io.EOF {
		return "", errors.New("invalid cluster YAML configuration")
	}
	return filepath.Dir(abs), nil
}

func relativePath(base, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}
func resolveTLS(base string, c *TLSConfig) {
	if c.Trust != nil {
		c.Trust.File = relativePath(base, c.Trust.File)
	}
	c.IdentityFile = relativePath(base, c.IdentityFile)
	c.CertFile = relativePath(base, c.CertFile)
	c.KeyFile = relativePath(base, c.KeyFile)
	c.CAFile = relativePath(base, c.CAFile)
}
func resolveNATS(base string, c *NATSConfig) {
	c.CertFile = relativePath(base, c.CertFile)
	c.KeyFile = relativePath(base, c.KeyFile)
	c.CAFile = relativePath(base, c.CAFile)
	c.CredentialsFile = relativePath(base, c.CredentialsFile)
}

func loadIdentity(c TLSConfig) (tls.Certificate, error) {
	var empty tls.Certificate
	if err := validateTLSRotation(c); err != nil {
		return empty, err
	}
	if c.IdentityFile != "" {
		return empty, errors.New("rotating TLS identity requires a managed TLS lifecycle")
	}
	st, err := os.Stat(c.KeyFile)
	if err != nil || !st.Mode().IsRegular() || st.Mode()&0027 != 0 {
		return empty, errors.New("TLS private key must not be writable by group or accessible by others")
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return empty, errors.New("TLS identity cannot be loaded")
	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return empty, errors.New("invalid TLS identity")
	}
	return cert, nil
}

func loadRoots(path string) (*x509.CertPool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("TLS trust bundle cannot be loaded")
	}
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(b) {
		return nil, errors.New("TLS trust bundle has no certificates")
	}
	return p, nil
}

func hasURI(cert *x509.Certificate, expected string) bool {
	if cert == nil {
		return false
	}
	for _, uri := range cert.URIs {
		if uri.String() == expected {
			return true
		}
	}
	return false
}

const GatewayIdentity = "spiffe://kelvo/gateway"

func WorkerIdentity(tenant, worker string) string {
	return fmt.Sprintf("spiffe://kelvo/tenant/%s/worker/%s", tenant, worker)
}

// VerifyConnection also runs for resumed sessions; TLS verification is never disabled.
func BuildClientTLS(c TLSConfig, peerURI string) (*tls.Config, error) {
	return buildClientTLS(c, peerURI, nil)
}

func buildClientTLS(c TLSConfig, peerURI string, identity *tlsIdentity) (*tls.Config, error) {
	return buildClientTLSWithTrust(c, peerURI, identity, nil)
}

func buildClientTLSWithTrust(c TLSConfig, peerURI string, identity *tlsIdentity, trust *tlsTrust) (*tls.Config, error) {
	var cert tls.Certificate
	var err error
	if identity == nil {
		cert, err = loadIdentity(c)
	} else {
		var current *tls.Certificate
		current, err = identity.current()
		if err == nil {
			cert = *current
		}
	}
	if err != nil {
		return nil, err
	}
	if !hasURI(cert.Leaf, GatewayIdentity) {
		return nil, errors.New("worker client certificate must identify the gateway")
	}
	roots, err := loadTLSRoots(c, trust)
	if err != nil {
		return nil, err
	}
	result := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{cert}, VerifyConnection: func(s tls.ConnectionState) error {
		if len(s.VerifiedChains) == 0 || !hasURI(s.PeerCertificates[0], peerURI) {
			return errors.New("unexpected worker identity")
		}
		return nil
	}}
	if identity != nil {
		result.Certificates = nil
		result.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			current, err := identity.current()
			if err != nil {
				return nil, err
			}
			if err := info.SupportsCertificate(current); err != nil {
				return nil, errTLSIdentityUnavailable
			}
			return current, nil
		}
		verify := result.VerifyConnection
		result.VerifyConnection = func(state tls.ConnectionState) error {
			if !identity.ready() {
				return errTLSIdentityUnavailable
			}
			return verify(state)
		}
	}
	return result, nil
}

func BuildServerTLS(c TLSConfig, ownURI, clientURI string) (*tls.Config, error) {
	if c.Trust != nil {
		return nil, errors.New("rotating TLS trust requires a managed TLS lifecycle")
	}
	return buildServerTLS(c, ownURI, clientURI, nil)
}

func buildServerTLS(c TLSConfig, ownURI, clientURI string, trust *tlsTrust) (*tls.Config, error) {
	cert, err := loadIdentity(c)
	if err != nil {
		return nil, err
	}
	if ownURI != "" && !hasURI(cert.Leaf, ownURI) {
		return nil, errors.New("server certificate does not match the configured identity")
	}
	result := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}}
	if clientURI != "" {
		result.ClientCAs, err = loadTLSRoots(c, trust)
		if err != nil {
			return nil, err
		}
		result.ClientAuth = tls.RequireAndVerifyClientCert
		result.VerifyConnection = func(s tls.ConnectionState) error {
			if len(s.VerifiedChains) == 0 || !hasURI(s.PeerCertificates[0], clientURI) {
				return errors.New("unexpected gateway identity")
			}
			return nil
		}
	}
	return result, nil
}

func loadTLSRoots(c TLSConfig, trust *tlsTrust) (*x509.CertPool, error) {
	if trust != nil {
		snapshot, err := trust.current()
		if err != nil {
			return nil, err
		}
		return snapshot.roots, nil
	}
	if c.Trust != nil {
		return nil, errors.New("rotating TLS trust requires a managed TLS lifecycle")
	}
	return loadRoots(c.CAFile)
}
