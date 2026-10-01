// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const gatewayYAML = `listen: 127.0.0.1:8443
tls: {cert_file: gateway.pem, key_file: gateway.key}
worker_tls: {cert_file: gateway.pem, key_file: gateway.key, ca_file: ca.pem}
max_queries: 8
max_concurrent: 1
max_http_requests: 16
tenants:
  - token_env: KELVO_TOKEN_A
    nats: {url: "tls://localhost:4222", ca_file: ca.pem, username: gateway, password_env: KELVO_NATS_PASSWORD}
    workers: [{id: a1, url: "https://localhost:8444"}]
    policy:
      tenant_id: a
      max_queries: 8
      job_ttl: 60s
      lease_duration: 5s
      replicas: 3
      workers: {a1: 1}
      limits: {max_rows: 1000, max_bytes: 1048576, timeout: 30s, memory_mb: 256, threads: 1, max_temp_mb: 256}
`

func TestGatewayYAMLPolicies(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gateway.yml")
	if err := os.WriteFile(path, []byte(gatewayYAML), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadGateway(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Tenants[0].Policy.Limits.Timeout != 30*time.Second || c.TLS.CertFile != filepath.Join(dir, "gateway.pem") {
		t.Fatal("duration or relative path decoded incorrectly")
	}
	for name, text := range map[string]string{
		"unknown":         gatewayYAML + "unknown: true\n",
		"duplicate":       gatewayYAML + "listen: localhost:9999\n",
		"documents":       gatewayYAML + "---\nlisten: localhost:9999\n",
		"capacity":        strings.Replace(gatewayYAML, "max_queries: 8", "max_queries: 1", 1),
		"unsafe endpoint": strings.Replace(gatewayYAML, "https://localhost:8444", "https://user:password@localhost:8444", 1),
		"short retention": strings.Replace(gatewayYAML, "job_ttl: 60s", "job_ttl: 31s", 1),
		"unknown worker":  strings.Replace(gatewayYAML, "workers: [{id: a1", "workers: [{id: a2", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadGateway(path); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestEndpointAuthorityOnly(t *testing.T) {
	for _, s := range []string{"http://localhost", "https://localhost/api", "https://localhost?x=1", "https://localhost#fragment", "https://u:p@localhost", "https:opaque", "https:///"} {
		if validEndpoint(s) {
			t.Errorf("accepted %q", s)
		}
	}
}
