// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

func privateCAFixture(t *testing.T) (string, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	raw, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(raw)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw})), parsed, key
}
func TestPrivateSourceCAOptionsAreStrict(t *testing.T) {
	valid, ca, key := privateCAFixture(t)
	if ValidateSourceOptions(nil) != nil || ValidateSourceOptions(map[string]string{"tls_ca_pem": valid}) != nil {
		t.Fatal("valid CA rejected")
	}
	for _, raw := range []string{"", " ", "junk" + valid, valid + "junk", strings.Repeat(valid, 9), strings.Repeat("x", (64<<10)+1), valid + "\n-----BEGIN PRIVATE KEY-----\nYWJj\n-----END PRIVATE KEY-----"} {
		if ValidateSourceOptions(map[string]string{"tls_ca_pem": raw}) == nil {
			t.Fatal("untrusted PEM input accepted")
		}
	}
	for _, options := range []map[string]string{{"sslrootcert": "/path"}, {"tls_server_name": "other"}, {"tls_ca_pem": valid, "tls_skip_verify": "true"}} {
		if ValidateSourceOptions(options) == nil {
			t.Fatal("unsafe option accepted")
		}
	}
	for _, mode := range []string{"expired", "future", "leaf", "no_cert_sign"} {
		t.Run(mode, func(t *testing.T) {
			cert := *ca
			cert.SerialNumber = big.NewInt(2)
			switch mode {
			case "expired":
				cert.NotAfter = time.Now().Add(-time.Minute)
			case "future":
				cert.NotBefore = time.Now().Add(time.Minute)
			case "leaf":
				cert.IsCA = false
			case "no_cert_sign":
				cert.KeyUsage = x509.KeyUsageDigitalSignature
			}
			der, err := x509.CreateCertificate(rand.Reader, &cert, &cert, &key.PublicKey, key)
			if err != nil {
				t.Fatal(err)
			}
			encoded := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
			if ValidateSourceOptions(map[string]string{"tls_ca_pem": encoded}) == nil {
				t.Fatal("invalid CA accepted")
			}
		})
	}
	if validateSource(catalog.Source{Options: map[string]string{"tls_ca_pem": valid}, Federation: &catalog.FederationConfig{}}) == nil {
		t.Fatal("unimplemented federation trust accepted")
	}
}
func TestPrivateSourceCAPreservesTLSVerification(t *testing.T) {
	clearPG(t)
	root, ca, key := privateCAFixture(t)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "test server"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	serverCert := tls.Certificate{Certificate: [][]byte{der, ca.Raw}, PrivateKey: key}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			_ = conn.(*tls.Conn).Handshake()
			_ = conn.Close()
		}
	}()
	defer func() { listener.Close(); <-done }()
	other, _, _ := privateCAFixture(t)
	for _, mode := range []string{"trusted", "wrong_ca", "wrong_host", "system_roots"} {
		t.Run(mode, func(t *testing.T) {
			source := catalog.Source{Options: map[string]string{"tls_ca_pem": root}}
			host := "127.0.0.1"
			switch mode {
			case "wrong_ca":
				source.Options["tls_ca_pem"] = other
			case "wrong_host":
				host = "wrong.invalid"
			case "system_roots":
				source.Options = nil
			}
			config, err := parseSourceConfig(source, "postgres://explicit:fixture@"+host+"/analytics?sslmode=verify-full")
			if err != nil {
				t.Fatal(err)
			}
			if config.TLSConfig.InsecureSkipVerify || config.TLSConfig.ServerName != host || len(config.Fallbacks) != 0 || len(config.TLSConfig.Certificates) != 0 {
				t.Fatal("private CA weakened identity checks")
			}
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", listener.Addr().String(), config.TLSConfig)
			if conn != nil {
				conn.Close()
			}
			if mode == "trusted" && err != nil {
				t.Fatalf("private CA rejected: %v", err)
			}
			if mode != "trusted" && err == nil {
				t.Fatal("untrusted server accepted")
			}
		})
	}
	first, err := parseSourceConfig(catalog.Source{Options: map[string]string{"tls_ca_pem": root}}, "postgres://explicit:fixture@db.example/analytics?sslmode=verify-full")
	if err != nil {
		t.Fatal(err)
	}
	second, err := parseSourceConfig(catalog.Source{Options: map[string]string{"tls_ca_pem": root}}, "postgres://explicit:fixture@db.example/analytics?sslmode=verify-full")
	if err != nil {
		t.Fatal(err)
	}
	if first.TLSConfig.RootCAs == second.TLSConfig.RootCAs {
		t.Fatal("source trust pool shared across executions")
	}
	t.Setenv("PGSSLROOTCERT", "forbidden")
	if _, err := parseSourceConfig(catalog.Source{Options: map[string]string{"tls_ca_pem": root}}, "postgres://explicit:fixture@db.example/analytics?sslmode=verify-full"); err == nil {
		t.Fatal("private CA re-enabled ambient PG configuration")
	}
}
