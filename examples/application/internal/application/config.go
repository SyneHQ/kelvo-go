// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package application

import (
	"bytes"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/SYNEHQ/kelvo-go/client"
	"go.yaml.in/yaml/v3"
)

var ErrConfiguration = errors.New("application configuration is unavailable or invalid")

type Identity struct {
	Issuer           string `yaml:"issuer"`
	Audience         string `yaml:"audience"`
	ClusterTenant    string `yaml:"cluster_tenant"`
	ServicePrincipal string `yaml:"service_principal"`
	PublicKeyEnv     string `yaml:"public_key_env"`
	SigningSeedFile  string `yaml:"signing_seed_file"`
}

type TLSFiles struct {
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type Config struct {
	Version    int      `yaml:"version"`
	Identity   Identity `yaml:"identity"`
	PolicyFile string   `yaml:"policy_file"`
	Gateway    struct {
		URL            string `yaml:"url"`
		BearerTokenEnv string `yaml:"bearer_token_env"`
		TLSFiles       `yaml:",inline"`
	} `yaml:"gateway"`
	Authority struct {
		Listen   string `yaml:"listen"`
		TLSFiles `yaml:",inline"`
	} `yaml:"authority"`
}

func readFile(path string, limit int64, private bool) ([]byte, error) {
	if limit < 1 || limit > 1<<20 {
		return nil, ErrConfiguration
	}
	// Check before opening so a FIFO cannot block. O_NONBLOCK also closes the
	// replacement race between stat and open; validate the opened inode again.
	before, err := os.Stat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() < 1 || before.Size() > limit || (private && before.Mode().Perm()&0077 != 0) {
		return nil, ErrConfiguration
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, ErrConfiguration
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !os.SameFile(before, info) || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > limit || (private && info.Mode().Perm()&0077 != 0) {
		return nil, ErrConfiguration
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || len(raw) == 0 || int64(len(raw)) > limit {
		clear(raw)
		return nil, ErrConfiguration
	}
	return raw, nil
}

func readYAML(path string, dst any) error {
	raw, err := readFile(path, 256<<10, false)
	if err != nil {
		return err
	}
	defer clear(raw)
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if decoder.Decode(dst) != nil || decoder.Decode(new(any)) != io.EOF {
		return ErrConfiguration
	}
	return nil
}

func relative(base, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}

func resolveTLS(base string, files *TLSFiles) {
	files.CAFile = relative(base, files.CAFile)
	files.CertFile = relative(base, files.CertFile)
	files.KeyFile = relative(base, files.KeyFile)
}

func LoadConfig(path string) (Config, error) {
	var c Config
	if err := readYAML(path, &c); err != nil {
		return c, err
	}
	if c.Version != 1 || c.PolicyFile == "" || c.Identity.Issuer == "" || c.Identity.Audience == "" || c.Identity.ClusterTenant == "" || c.Identity.ServicePrincipal == "" || c.Identity.PublicKeyEnv == "" {
		return c, ErrConfiguration
	}
	base := filepath.Dir(path)
	c.PolicyFile = relative(base, c.PolicyFile)
	c.Identity.SigningSeedFile = relative(base, c.Identity.SigningSeedFile)
	resolveTLS(base, &c.Gateway.TLSFiles)
	resolveTLS(base, &c.Authority.TLSFiles)
	return c, nil
}

func PublicKey(c Config) (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(os.Getenv(c.Identity.PublicKeyEnv))
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, ErrConfiguration
	}
	return ed25519.PublicKey(raw), nil
}

// SigningKey is used only by the trusted application exercise process. It is
// never loaded by the resolver service and is never accepted in a query body.
func SigningKey(c Config) (ed25519.PrivateKey, error) {
	raw, err := readFile(c.Identity.SigningSeedFile, 128, true)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	seed, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	defer clear(seed)
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, ErrConfiguration
	}
	key := ed25519.NewKeyFromSeed(seed)
	pub, err := PublicKey(c)
	if err != nil || !bytes.Equal(key.Public().(ed25519.PublicKey), pub) {
		clear(key)
		return nil, ErrConfiguration
	}
	return key, nil
}

func roots(path string) (*x509.CertPool, error) {
	raw, err := readFile(path, 256<<10, false)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, ErrConfiguration
	}
	return pool, nil
}

func keyPair(files TLSFiles) (tls.Certificate, error) {
	cert, err := readFile(files.CertFile, 1<<20, false)
	if err != nil {
		return tls.Certificate{}, ErrConfiguration
	}
	key, err := readFile(files.KeyFile, 1<<20, true)
	if err != nil {
		return tls.Certificate{}, ErrConfiguration
	}
	defer clear(key)
	pair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return tls.Certificate{}, ErrConfiguration
	}
	return pair, nil
}

func Gateway(c Config) (*client.Client, error) {
	pool, err := roots(c.Gateway.CAFile)
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool}
	if c.Gateway.CertFile != "" || c.Gateway.KeyFile != "" {
		cert, err := keyPair(c.Gateway.TLSFiles)
		if err != nil {
			return nil, ErrConfiguration
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	token := os.Getenv(c.Gateway.BearerTokenEnv)
	if token == "" {
		return nil, ErrConfiguration
	}
	return client.New(client.Config{URL: c.Gateway.URL, BearerToken: token, TLSConfig: tlsConfig,
		Timeout: 45 * time.Second, MaxConcurrent: 4, MaxControlConcurrent: 4,
		MaxRows: 1000, MaxDecodedBytes: 16 << 20, MaxWireBytes: 16 << 20})
}

func ServerTLS(c Config) (*tls.Config, error) {
	pool, err := roots(c.Authority.CAFile)
	if err != nil {
		return nil, err
	}
	cert, err := keyPair(c.Authority.TLSFiles)
	if err != nil {
		return nil, ErrConfiguration
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs: pool, Certificates: []tls.Certificate{cert}}, nil
}
