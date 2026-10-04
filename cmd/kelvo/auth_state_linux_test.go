//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/authstate"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func authStateCLIConfig(t *testing.T, broker string) (string, string) {
	t.Helper()
	root := t.TempDir()
	directory := filepath.Join(root, "state")
	keys := filepath.Join(root, "keys.yml")
	// This deliberately has no usable broker credentials or TLS files.
	text := fmt.Sprintf(`listen: 127.0.0.1:8443
tls: {cert_file: absent.pem, key_file: absent.key}
worker_tls: {cert_file: absent.pem, key_file: absent.key, ca_file: absent-ca.pem}
max_queries: 8
max_concurrent: 1
max_http_requests: 16
authentication:
  keys_file: %q
  state: {directory: %q, scope: gateway-a}
tenants:
  - nats: {url: %q, ca_file: absent-ca.pem, username: gateway, password_env: KELVO_TEST_UNSET_PASSWORD}
    workers: [{id: a1, url: "https://127.0.0.1:8444"}]
    policy:
      tenant_id: a
      max_queries: 8
      job_ttl: 60s
      lease_duration: 5s
      replicas: 1
      workers: {a1: 1}
      limits: {max_rows: 1000, max_bytes: 1048576, timeout: 30s, memory_mb: 256, threads: 1, max_temp_mb: 256}
`, keys, directory, broker)
	if err := os.WriteFile(keys, []byte("version: 1\nrevision: 7\ntenants: {a: [abcdefghijklmnopqrstuvwxyz012345]}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "gateway.yml")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path, directory
}

func TestAuthStateCLIInitializesOfflineAndRefusesOverwrite(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	path, directory := authStateCLIConfig(t, "tls://"+listener.Addr().String())
	var output bytes.Buffer
	if err := runAuthStateInit([]string{"--config", path}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(output.String(), "Authentication state initialized.") || strings.Contains(output.String(), directory) {
		t.Fatal("unexpected initialization confirmation")
	}
	if err := listener.SetDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	connection, err := listener.Accept()
	if connection != nil {
		connection.Close()
		t.Fatal("offline initialization contacted the broker")
	}
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatal("broker canary did not time out", err)
	}
	before, err := os.ReadFile(filepath.Join(directory, "state.yml"))
	if err != nil || bytes.Contains(before, []byte("abcdefghijklmnopqrstuvwxyz012345")) {
		t.Fatal("missing state or raw token persisted", err)
	}
	output.Reset()
	if err := runAuthStateInit([]string{"--config", path}, &output); err == nil || query.PublicError(err).Code != "UNAVAILABLE" || output.Len() != 0 || strings.Contains(err.Error(), directory) {
		t.Fatal("existing state overwritten or disclosed")
	}
	after, err := os.ReadFile(filepath.Join(directory, "state.yml"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed second initialization changed history")
	}
	state, err := authstate.Open(context.Background(), directory, authstate.Scope{ID: "gateway-a", Tenants: []string{"a"}})
	if err != nil {
		t.Fatal("initialized state cannot reopen", err)
	}
	if err := state.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type authStateFailWriter struct{ short bool }

func (w authStateFailWriter) Write(p []byte) (int, error) {
	if w.short {
		return len(p) - 1, nil
	}
	return 0, errors.New("private-output-marker")
}

func TestAuthStateCLIOutputFailurePreservesInitializedState(t *testing.T) {
	for _, short := range []bool{false, true} {
		path, directory := authStateCLIConfig(t, "tls://127.0.0.1:1")
		err := runAuthStateInit([]string{"--config", path}, authStateFailWriter{short: short})
		if err == nil || query.PublicError(err).Code != "UNAVAILABLE" || !strings.Contains(err.Error(), "initialized") || strings.Contains(err.Error(), "private-output-marker") {
			t.Fatal("confirmation failure lost its durable outcome or leaked details")
		}
		state, err := authstate.Open(context.Background(), directory, authstate.Scope{ID: "gateway-a", Tenants: []string{"a"}})
		if err != nil {
			t.Fatal("output failure lost durable state", err)
		}
		if err := state.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
