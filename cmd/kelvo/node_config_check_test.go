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

// Read capacity is a node setting. It is validated by the check, and it is not
// bounded by policy.workers, which is part of the cluster metadata record.
func TestNodeCheckConfigReadWorkersAreNodeSettings(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Operation artifact validation requires Linux")
	}
	path, config, _ := nodeCheckFixture(t, "127.0.0.1:1")
	// policy.workers stays {a1: 1}; capacity is raised only through node settings.
	raised := strings.Replace(config, "resources: {max_concurrent: 1, memory_mb: 512,", "resources: {max_concurrent: 4, memory_mb: 4096,", 1)
	raised = strings.Replace(raised, "max_concurrent: 1\n  max_downloads", "max_concurrent: 4\n  max_downloads", 1)
	pool := func(fields string) string {
		return strings.Replace(raised, "  poll_interval: 100ms\n", "  poll_interval: 100ms\n  warm_pool: {enabled: true, queue_depth: 64, worker_memory_mb: 64"+fields+"}\n", 1)
	}
	for name, tc := range map[string]struct {
		body  string
		valid bool
	}{
		"operations above policy.workers":                  {raised, true},
		"read workers with defaults":                       {pool("") + "read_workers: {max_workers: 2}\n", true},
		"read workers all settings":                        {pool("") + "read_workers: {max_workers: 2, max_in_flight: 8, pool_size: 8, idle_ttl: 10m, queue_wait: 500ms, stream_buffer_mb: 4}\n", true},
		"read workers without capacity":                    {pool("") + "read_workers: {max_in_flight: 16}\n", false},
		"read workers above admission":                     {pool("") + "read_workers: {max_workers: 5}\n", false},
		"read workers bad pool":                            {pool("") + "read_workers: {max_workers: 1, pool_size: 33}\n", false},
		"read workers bad in flight":                       {pool("") + "read_workers: {max_workers: 1, max_in_flight: 65}\n", false},
		"read workers unknown key":                         {pool("") + "read_workers: {max_workers: 1, secret_canary: x}\n", false},
		"read workers without a warm pool":                 {raised + "read_workers: {max_workers: 2}\n", false},
		"read workers with a disabled pool":                {strings.Replace(pool(""), "enabled: true", "enabled: false", 1) + "read_workers: {max_workers: 2}\n", false},
		"capacity set in both places":                      {pool(", max_workers: 2") + "read_workers: {max_workers: 2}\n", false},
		"max_per_key set in both places":                   {pool(", max_per_key: 1") + "read_workers: {max_workers: 2}\n", false},
		"read workers max_per_key":                         {pool("") + "read_workers: {max_workers: 2, max_per_key: 1}\n", true},
		"read workers source budget":                       {pool("") + "read_workers: {max_workers: 2, pool_size: 32, max_per_key: 2}\n", true},
		"read workers source budget exceeded":              {pool("") + "read_workers: {max_workers: 2, pool_size: 32, max_per_key: 2, max_source_connections: 63}\n", false},
		"read workers source budget too large":             {pool("") + "read_workers: {max_workers: 2, max_source_connections: 1025}\n", false},
		"read workers evict_min_idle":                      {pool("") + "read_workers: {max_workers: 2, evict_min_idle: 500ms}\n", true},
		"read workers evict_min_idle not below queue_wait": {pool("") + "read_workers: {max_workers: 2, evict_min_idle: 2s, queue_wait: 2s}\n", false},
		"read workers evict_min_idle too large":            {pool("") + "read_workers: {max_workers: 2, evict_min_idle: 11m}\n", false},
		"evict_min_idle set in both places":                {pool(", evict_min_idle: 1s") + "read_workers: {max_workers: 2}\n", false},
		"read workers max_per_key too large":               {pool("") + "read_workers: {max_workers: 2, max_per_key: 3}\n", false},
		"stream buffer set in both places":                 {pool(", stream_buffer_mb: 8") + "read_workers: {max_workers: 2}\n", false},
		"legacy warm pool capacity still works":            {pool(", max_per_key: 1, idle_ttl: 30m, max_workers: 2, max_in_flight: 16, queue_timeout: 5s"), true},
		"operations above audit max_pending":               {strings.Replace(raised, "max_pending: 8", "max_pending: 2", 1), false},
	} {
		if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
			t.Fatal(err)
		}
		err, output := nodeCheckRunCaptured(t, []string{"node", "--check-config", "--config", path})
		if tc.valid != (err == nil) {
			t.Fatalf("%s: valid=%v err=%v output=%q", name, tc.valid, err, output)
		}
		if !tc.valid && strings.Contains(output+fmt.Sprint(err), "canary") {
			t.Fatalf("%s: configuration value leaked: %q", name, output)
		}
	}
}
