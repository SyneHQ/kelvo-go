// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
)

func TestConnectionCatalogAllowsBoundedPrivateCAOnlyForNativePostgres(t *testing.T) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	root := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	for _, kind := range []string{"postgres", "cockroachdb", "alloydb", "redshift"} {
		request, execution, response := connectionFixture(t)
		response.Sources[0].Type = kind
		response.Sources[0].Options = map[string]string{"tls_ca_pem": root}
		if err := validateConnectionCatalog(response, execution.Claims, request); err != nil {
			t.Fatalf("native private CA rejected for %s", kind)
		}
	}
	for _, scenario := range []string{"junk", "private_key", "hostname_override", "unverified_tls", "other_engine", "federation"} {
		t.Run(scenario, func(t *testing.T) {
			request, execution, response := connectionFixture(t)
			response.Sources[0].Options = map[string]string{"tls_ca_pem": root}
			switch scenario {
			case "junk":
				response.Sources[0].Options["tls_ca_pem"] = root + "junk"
			case "private_key":
				response.Sources[0].Options["tls_ca_pem"] = "-----BEGIN PRIVATE KEY-----\nYWJj\n-----END PRIVATE KEY-----"
			case "hostname_override":
				response.Sources[0].Options["tls_server_name"] = "other.example"
			case "unverified_tls":
				response.Secrets[response.Sources[0].DSNEnv] = "postgres://reader:fixture@database.example:5432/analytics?connect_timeout=5&sslmode=disable"
			case "other_engine":
				response.Sources[0].Type = "mysql"
			case "federation":
				request.Mode, request.ConnectionID, request.Sources = "federated", "", []string{"selected"}
				execution.Claims.Sources[0].Schema = "public"
				execution.Claims.Sources[0].Tables = []delegation.Table{{Name: "orders", Table: "orders", Schema: "public"}}
				response.Sources[0].Federation = &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "orders", Table: "orders", Schema: "public"}}}
			}
			if validateConnectionCatalog(response, execution.Claims, request) == nil {
				t.Fatal("unsupported or unsafe source trust accepted")
			}
		})
	}
}

func TestConnectionCatalogAllowsPrivateCAForNativeClickHouse(t *testing.T) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	root := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	clickhouse := func(options map[string]string) catalog.Source {
		return catalog.Source{ID: "selected", Type: "clickhouse", URLEnv: "U", UsernameEnv: "N", PasswordEnv: "P", Options: options}
	}
	secrets := map[string]string{"U": "https://ch.example:8443/?database=analytics", "N": "reader", "P": "secret"}
	selected := delegation.Source{Alias: "selected", Database: "analytics"}
	for _, options := range []map[string]string{{"tls_ca_pem": root}, {"tls_ca_pem": root, "arrow_compression": "lz4_frame"}} {
		if !validConnectionSource(clickhouse(options), selected, secrets, false) {
			t.Fatalf("native ClickHouse private CA rejected: %v", len(options))
		}
	}
	big := root
	for len(big) <= 64<<10 {
		big += root
	}
	federated := clickhouse(map[string]string{"tls_ca_pem": root})
	federated.Federation = &catalog.FederationConfig{}
	for name, source := range map[string]catalog.Source{
		"junk":       clickhouse(map[string]string{"tls_ca_pem": root + "junk"}),
		"not_pem":    clickhouse(map[string]string{"tls_ca_pem": "garbage"}),
		"oversized":  clickhouse(map[string]string{"tls_ca_pem": big}),
		"federation": federated,
	} {
		if validConnectionSource(source, selected, secrets, false) {
			t.Fatalf("%s: unsafe ClickHouse trust accepted", name)
		}
	}
}
