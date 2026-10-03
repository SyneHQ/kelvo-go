// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

func TestSourceHealthCatalogExcludesFilesAndSnapshots(t *testing.T) {
	c := catalog.Config{Sources: []catalog.Source{
		{ID: "tenant_source", Type: "postgresql", DSNEnv: "PRIVATE_DSN"},
		{ID: "snapshot", Type: "accelerated"}, {ID: "file_csv", Type: "csv"},
		{ID: "file_parquet", Type: "parquet"}, {ID: "file_duckdb", Type: "duckdb"}, {ID: "file_sqlite", Type: "sqlite"},
	}}
	h, err := NewSourceHealth(c, telemetry.SourceHealthConfig{ObservationTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	entries := h.Entries()
	if len(entries) != 1 || entries[0].ID != "tenant_source" {
		t.Fatalf("incorrect catalog scope: %+v", entries)
	}
	if h.Observe("foreign_source", telemetry.SourceSuccess) {
		t.Fatal("foreign source accepted")
	}
}

func TestSourceHealthNodeEndpointRequiresVerifiedGateway(t *testing.T) {
	h, _ := telemetry.NewSourceHealth(telemetry.SourceHealthConfig{ObservationTTL: time.Minute}, []string{"tenant_source"})
	n := &Node{ctx: context.Background(), cfg: NodeConfig{RuntimeSourceHealth: h}}
	valid := nodeRequest(http.MethodGet, "/sources").TLS.PeerCertificates[0]
	uri, _ := url.Parse(WorkerIdentity("foreign-tenant", "worker"))
	wrong := &x509.Certificate{URIs: []*url.URL{uri}}
	for name, state := range map[string]*tls.ConnectionState{
		"plaintext":       nil,
		"unverified":      {PeerCertificates: []*x509.Certificate{valid}},
		"nil peer":        {VerifiedChains: [][]*x509.Certificate{{valid}}, PeerCertificates: []*x509.Certificate{nil}},
		"worker identity": {VerifiedChains: [][]*x509.Certificate{{wrong}}, PeerCertificates: []*x509.Certificate{wrong}},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/sources", nil)
			req.TLS = state
			w := httptest.NewRecorder()
			n.ServeHTTP(w, req)
			if w.Code != http.StatusUnauthorized || strings.Contains(w.Body.String(), "tenant_source") {
				t.Fatal("source identities exposed")
			}
		})
	}
	h.Observe("tenant_source", telemetry.SourceAccessFailure)
	w := httptest.NewRecorder()
	n.ServeHTTP(w, nodeRequest(http.MethodGet, "/sources?tenant=foreign&sql=secret&source=unconfigured"))
	if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("invalid source diagnostic response")
	}
	var entries []telemetry.SourceStatus
	if err := json.Unmarshal(w.Body.Bytes(), &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].ID != "tenant_source" || entries[0].Category != "access" {
		t.Fatalf("incorrect scope: %+v", entries)
	}
	for _, secret := range []string{"foreign", "secret", "unconfigured", "PRIVATE_DSN"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("request or credential leaked")
		}
	}
	for _, path := range []string{"/health", "/ready"} {
		w = httptest.NewRecorder()
		n.ServeHTTP(w, nodeRequest(http.MethodGet, path))
		if w.Code != http.StatusOK {
			t.Fatalf("source outage affected %s", path)
		}
	}
}

func TestSourceHealthMethodsDisabledAndDrain(t *testing.T) {
	h, _ := telemetry.NewSourceHealth(telemetry.SourceHealthConfig{ObservationTTL: time.Minute}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n := &Node{ctx: ctx, cfg: NodeConfig{RuntimeSourceHealth: h}}
	for method, want := range map[string]int{http.MethodGet: 200, http.MethodHead: 200, http.MethodPost: 405} {
		w := httptest.NewRecorder()
		n.ServeHTTP(w, nodeRequest(method, "/sources"))
		if w.Code != want {
			t.Fatalf("%s got %d", method, w.Code)
		}
		if method == http.MethodHead && w.Body.Len() != 0 {
			t.Fatal("HEAD included a body")
		}
		if method == http.MethodPost && w.Header().Get("Allow") != "GET, HEAD" {
			t.Fatal("missing allowed methods")
		}
	}
	n.cfg.RuntimeSourceHealth = nil
	n.ctx = context.Background()
	w := httptest.NewRecorder()
	n.ServeHTTP(w, nodeRequest(http.MethodGet, "/sources"))
	if w.Code != http.StatusNotFound {
		t.Fatal("disabled source diagnostics are exposed")
	}
}

func TestLoadNodeSourceHealthOptionalAndValidated(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sandbox"), []byte("fixture"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "node.yml")
	for name, tc := range map[string]struct {
		extra string
		valid bool
	}{
		"omitted":       {"", true},
		"enabled":       {"source_health: {observation_ttl: 5m}\n", true},
		"zero":          {"source_health: {observation_ttl: 0s}\n", false},
		"too long":      {"source_health: {observation_ttl: 25h}\n", false},
		"unknown field": {"source_health: {observation_ttl: 5m, source: injected}\n", false},
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(resourceNodeYAML+tc.extra), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadNode(path)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
			if name == "enabled" && (cfg.SourceHealth == nil || cfg.SourceHealth.ObservationTTL != 5*time.Minute) {
				t.Fatal("source health config lost")
			}
		})
	}
}
