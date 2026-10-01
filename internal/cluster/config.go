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

	"go.yaml.in/yaml/v3"
)

var clusterID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

func LoadGateway(path string) (GatewayConfig, error) {
	var c GatewayConfig
	base, err := decodeConfig(path, &c)
	if err != nil {
		return c, err
	}
	resolveTLS(base, &c.TLS)
	resolveTLS(base, &c.WorkerTLS)
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
		if seen[t.Policy.TenantID] || t.TokenEnv == "" {
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
	return c, nil
}

func LoadNode(path string) (NodeConfig, error) {
	var c NodeConfig
	base, err := decodeConfig(path, &c)
	if err != nil {
		return c, err
	}
	resolveTLS(base, &c.TLS)
	resolveNATS(base, &c.NATS)
	c.CatalogFile = relativePath(base, c.CatalogFile)
	c.SandboxPath = relativePath(base, c.SandboxPath)
	if err = ValidatePolicy(c.Policy); err != nil {
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
	return nil
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
	cert, err := loadIdentity(c)
	if err != nil {
		return nil, err
	}
	if !hasURI(cert.Leaf, GatewayIdentity) {
		return nil, errors.New("worker client certificate must identify the gateway")
	}
	roots, err := loadRoots(c.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{cert}, VerifyConnection: func(s tls.ConnectionState) error {
		if len(s.VerifiedChains) == 0 || !hasURI(s.PeerCertificates[0], peerURI) {
			return errors.New("unexpected worker identity")
		}
		return nil
	}}, nil
}

func BuildServerTLS(c TLSConfig, ownURI, clientURI string) (*tls.Config, error) {
	cert, err := loadIdentity(c)
	if err != nil {
		return nil, err
	}
	if ownURI != "" && !hasURI(cert.Leaf, ownURI) {
		return nil, errors.New("server certificate does not match the configured identity")
	}
	result := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}}
	if clientURI != "" {
		result.ClientCAs, err = loadRoots(c.CAFile)
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
