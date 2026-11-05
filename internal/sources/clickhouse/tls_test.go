// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package clickhouse

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"math/big"
	"time"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestPrivateCAVerifiesOnlyItsServer(t *testing.T) {
	data, _ := fixture(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(data) }))
	defer server.Close()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	otherDER, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	otherCA := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: otherDER}))
	caOf := func(s *httptest.Server) string {
		return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}))
	}
	// Default engine (system roots) rejects the private server.
	if _, err := newTestEngine(t, server.URL, query.DefaultLimits()).Execute(context.Background(), request(), &testSink{}); err == nil {
		t.Fatal("system roots accepted a private CA server")
	}
	if _, err := newTestEngine(t, server.URL, query.DefaultLimits(), map[string]string{"tls_ca_pem": otherCA}).Execute(context.Background(), request(), &testSink{}); err == nil {
		t.Fatal("a different CA verified the server")
	}
	if _, err := newTestEngine(t, server.URL, query.DefaultLimits(), map[string]string{"tls_ca_pem": caOf(server)}).Execute(context.Background(), request(), &testSink{}); err != nil {
		t.Fatalf("private CA rejected its own server: %v", err)
	}
	t.Setenv("KELVO_TEST_CLICKHOUSE_URL", server.URL)
	for _, bad := range []catalog.Source{
		{ID: "analytics", Type: "clickhouse", URLEnv: "KELVO_TEST_CLICKHOUSE_URL", Options: map[string]string{"tls_ca_pem": "garbage"}},
		{ID: "analytics", Type: "clickhouse", URLEnv: "KELVO_TEST_CLICKHOUSE_URL", Options: map[string]string{"tls_ca_pem": caOf(server)}, Federation: &catalog.FederationConfig{}},
	} {
		if _, err := New(catalog.Config{Sources: []catalog.Source{bad}}, query.DefaultLimits()); err == nil {
			t.Fatal("invalid ClickHouse CA source accepted")
		}
	}
}
