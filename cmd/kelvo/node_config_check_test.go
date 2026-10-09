// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func nodeCheckFixture(t *testing.T, endpoint string) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	marker := filepath.Join(root, "executed")
	binary := []byte(fmt.Sprintf("#!/bin/sh\ntouch %q\nexit 1\n", marker))
	if err := os.WriteFile(filepath.Join(root, "adapter"), binary, 0700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`listen: %q
worker_id: a1
catalog_file: missing-catalog.yml
sandbox_path: adapter
tls: {cert_file: missing.pem, key_file: missing.key, ca_file: missing-ca.pem}
nats: {url: "tls://%s", ca_file: missing-ca.pem, username: worker, password_env: KELVO_CHECK_MISSING_PASSWORD}
secrets:
  files: {KELVO_SOURCE_CHECK_SECRET: %q}
policy:
  tenant_id: a
  max_queries: 8
  job_ttl: 60s
  lease_duration: 5s
  replicas: 1
  workers: {a1: 1}
  limits: {max_rows: 1000, max_bytes: 1048576, timeout: 30s, memory_mb: 64, threads: 1, max_temp_mb: 16}
  operations: {shards: 8, slots_per_shard: 8, retention: 1h, execution_timeout: 30s}
  access: {revision: 1, principals: {api: {kind: service}}}
resources: {max_concurrent: 1, memory_mb: 512, baseline_mb: 64, overhead_mb: 128, scratch_mb: 256}
scratch_directory: %q
containment:
  root: %q
  state_directory: %q
  native_overhead_mb: 16
  parent_overhead_mb: 16
  max_processes: 32
audit:
  service_id: test-worker
  directory: %q
  max_entries: 128
  max_pending: 8
  retention: 1h
  write_timeout: 1s
operations:
  input_url: "https://%s"
  tls: {cert_file: missing.pem, key_file: missing.key, ca_file: missing-ca.pem}
  adapter:
    binary: adapter
    sha256: %x
    private_open_diagnostics: true
  results: {directory: %q, max_entries: 16, max_stored_bytes: 4194304}
  max_result_bytes: 1048576
  max_concurrent: 1
  max_downloads: 1
  poll_interval: 100ms
`, endpoint, endpoint, filepath.Join(root, "missing-secret"), filepath.Join(root, "scratch"), filepath.Join(root, "cgroup"), filepath.Join(root, "state"), filepath.Join(root, "audit"), endpoint, sha256.Sum256(binary), filepath.Join(root, "results"))
	return filepath.Join(root, "node.yml"), config, marker
}

func nodeCheckRunCaptured(t *testing.T, args []string) (error, string) {
	t.Helper()
	output, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	original := os.Stderr
	os.Stderr = output
	defer func() { os.Stderr = original }()
	result := run(args)
	if _, err := output.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	return result, string(body)
}

func TestNodeCheckConfigValidDoesNotStartRuntime(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Operation artifact validation requires Linux")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	path, config, marker := nodeCheckFixture(t, listener.Addr().String())
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	err, output := nodeCheckRunCaptured(t, []string{"node", "--check-config", "--config", path})
	if err != nil || output != "Node configuration is valid. Runtime dependencies were not checked.\n" {
		t.Fatalf("Configuration check failed: %v; output=%q", err, output)
	}
	for _, name := range []string{"scratch", "state", "audit", "results", filepath.Base(marker)} {
		if _, err := os.Lstat(filepath.Join(filepath.Dir(path), name)); !os.IsNotExist(err) {
			t.Fatalf("Configuration check created runtime path %s", name)
		}
	}
	tcp := listener.(*net.TCPListener)
	if err := tcp.SetDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	conn, err := tcp.Accept()
	if conn != nil {
		conn.Close()
		t.Fatal("Configuration check opened a network connection")
	}
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("Connection probe failed: %v", err)
	}
}

func TestNodeCheckConfigRejectsInvalidConfigurationWithoutValues(t *testing.T) {
	path, config, _ := nodeCheckFixture(t, "127.0.0.1:1")
	for name, body := range map[string]string{
		"unknown":            config + "unknown_secret_canary: value\n",
		"type":               strings.Replace(config, "private_open_diagnostics: true", "private_open_diagnostics: secret_value_canary", 1),
		"policy":             strings.Replace(config, "tenant_id: a", "tenant_id: invalid_secret_canary", 1),
		"multiple documents": config + "---\nsecret_canary: value\n",
		"artifact path":      strings.Replace(config, "binary: adapter", "binary: missing_secret_canary", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			err, output := nodeCheckRunCaptured(t, []string{"node", "--check-config", "--config", path})
			if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" || err.Error() != "Node configuration validation failed" || output != "" {
				t.Fatalf("Configuration error was not redacted: %v; output=%q", err, output)
			}
		})
	}
}

func TestNodeCheckConfigRejectsInvalidArgumentsWithoutValues(t *testing.T) {
	for _, args := range [][]string{
		{"node", "--check-config=secret_canary"},
		{"node", "--check-config", "--unknown=secret_canary"},
		{"node", "--check-config", "--drain-timeout=secret_canary"},
		{"node", "--config", "--", "--check-config=secret_canary"},
	} {
		err, output := nodeCheckRunCaptured(t, args)
		if err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" || strings.Contains(err.Error()+output, "secret_canary") || output != "" {
			t.Fatalf("Argument error was not redacted: %v; output=%q", err, output)
		}
	}
}

func TestNodeCheckConfigRejectsOtherCommands(t *testing.T) {
	for _, command := range []string{"gateway", "cluster-init", "serve", "query", "worker", "version", "audit", "accelerate"} {
		err, output := nodeCheckRunCaptured(t, []string{command, "--check-config=secret_canary"})
		if err == nil || strings.Contains(err.Error()+output, "secret_canary") {
			t.Fatalf("%s accepted the node-only flag or exposed its value", command)
		}
	}
}
