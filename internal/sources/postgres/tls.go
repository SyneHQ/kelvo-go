// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"time"
)

// ValidateSourceOptions accepts only explicit in-memory CA certificates. The
// native PostgreSQL opener still verifies the DSN hostname and forbids PG*
// environment settings, file paths, client keys and TLS bypass flags.
func ValidateSourceOptions(options map[string]string) error {
	if len(options) == 0 {
		return nil
	}
	raw, ok := options["tls_ca_pem"]
	if !ok || len(options) != 1 {
		return configError()
	}
	_, err := privateRoots(raw, time.Now())
	return err
}
func privateRoots(raw string, now time.Time) (*x509.CertPool, error) {
	if len(raw) == 0 || len(raw) > 64<<10 {
		return nil, configError()
	}
	remaining := []byte(raw)
	roots := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(remaining)) > 0 {
		remaining = bytes.TrimSpace(remaining)
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, configError()
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, configError()
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA || !cert.BasicConstraintsValid || cert.KeyUsage&x509.KeyUsageCertSign == 0 || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
			return nil, configError()
		}
		count++
		if count > 8 {
			return nil, configError()
		}
		roots.AddCert(cert)
		remaining = rest
	}
	if count == 0 {
		return nil, configError()
	}
	return roots, nil
}
