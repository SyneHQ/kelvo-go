//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

// The API process has no route to source databases. Only the real Kelvo worker
// connects to them; the API retains metadata, authentication and fixture KMS.
func operationAPICrossService(t *testing.T, gateway *httptest.Server, resolverURL string, resolverTLS TLSConfig, signingKey ed25519.PrivateKey, token string) {
	t.Helper()
	get := func(name string) string { return os.Getenv("KELVO_TEST_OPERATION_API_" + name) }
	for _, name := range []string{"BINARY", "WORKDIR", "SOURCE_CONFIG", "LOG"} {
		if value := get(name); value == "" || !filepath.IsAbs(value) {
			t.Fatal("explicit API cross-service fixture is incomplete")
		}
	}
	directory := t.TempDir()
	raw, err := os.ReadFile(get("SOURCE_CONFIG"))
	if err != nil {
		t.Fatal("API fixture configuration unavailable")
	}
	var config map[string]any
	if json.Unmarshal(raw, &config) != nil {
		t.Fatal("API fixture configuration invalid")
	}
	clear(raw)
	rootCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: gateway.Certificate().Raw})
	caFile := filepath.Join(directory, "gateway-ca.pem")
	if os.WriteFile(caFile, rootCert, 0600) != nil {
		t.Fatal("API gateway CA unavailable")
	}
	resolver, err := url.Parse(resolverURL)
	if err != nil {
		t.Fatal(err)
	}
	analyticsConfig := map[string]any{
		"version": 1, "default_deployment": "shared",
		"resolver": map[string]any{"listen": resolver.Host, "ca_file": resolverTLS.CAFile, "cert_file": resolverTLS.CertFile, "key_file": resolverTLS.KeyFile, "timeout": "10s", "max_concurrent": 4},
		"deployments": []any{map[string]any{"name": "shared", "url": gateway.URL, "token_env": "KELVO_CROSS_SERVICE_TOKEN", "ca_file": caFile, "timeout": "30s", "max_concurrent": 2, "max_rows": 1000, "max_decoded_bytes": 1 << 20, "max_wire_bytes": 2 << 20,
			"on_demand": map[string]any{"issuer": "fixture-gateway", "audience": "operations", "cluster_tenant": "a", "service_principal": "api", "signing_key_env": "KELVO_CROSS_SERVICE_SIGNING"}}},
	}
	yamlBytes, err := yaml.Marshal(analyticsConfig)
	if err != nil {
		t.Fatal(err)
	}
	config["analytics_config"] = filepath.Join(directory, "analytics.yml")
	config["ready_file"] = filepath.Join(directory, "api-ready")
	if os.WriteFile(config["analytics_config"].(string), yamlBytes, 0600) != nil {
		t.Fatal("API analytics configuration write failed")
	}
	jsonBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(directory, "fixture.json")
	if os.WriteFile(fixturePath, jsonBytes, 0600) != nil {
		t.Fatal("API source configuration write failed")
	}
	clear(jsonBytes)
	envPath := filepath.Join(directory, "api.env")
	env := "KELVO_CROSS_SERVICE_CONFIG_FILE=" + fixturePath + "\nKELVO_CROSS_SERVICE_TOKEN=" + token + "\nKELVO_CROSS_SERVICE_SIGNING=" + base64.StdEncoding.EncodeToString(signingKey.Seed()) + "\nGOMAXPROCS=1\nGOMEMLIMIT=384MiB\n"
	if os.WriteFile(envPath, []byte(env), 0600) != nil {
		t.Fatal("API fixture environment write failed")
	}
	nonce := make([]byte, 8)
	if _, err = rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	unit := "kelvo-operation-api-" + hex.EncodeToString(nonce) + ".service"
	// This is an owned, finite test process. cgroup BPF filters deny all external
	// addresses; the distinct metadata fixture is reached by a loopback relay.
	args := []string{"-n", "systemd-run", "--quiet", "--wait", "--pipe", "--unit=" + unit, "--property=Type=exec", "--property=User=" + strconv.Itoa(os.Getuid()), "--property=WorkingDirectory=" + get("WORKDIR"), "--property=CPUQuota=100%", "--property=MemoryMax=512M", "--property=MemorySwapMax=0", "--property=TasksMax=128", "--property=RuntimeMaxSec=180s", "--property=TimeoutStopSec=10s", "--property=KillMode=control-group", "--property=UMask=0077", "--property=NoNewPrivileges=yes", "--property=IPAddressDeny=any", "--property=IPAddressAllow=localhost", "--property=EnvironmentFile=" + envPath, get("BINARY"), "-test.v", "-test.timeout=170s", "-test.run=^TestKelvoOperationCrossServiceProcess$"}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = exec.CommandContext(ctx, "sudo", "-n", "systemctl", "stop", unit).Run()
		_ = exec.CommandContext(ctx, "sudo", "-n", "systemctl", "reset-failed", unit).Run()
		state, err := exec.CommandContext(ctx, "systemctl", "show", unit, "-p", "MainPID", "-p", "ActiveState").Output()
		if err != nil || !bytes.Contains(state, []byte("MainPID=0")) || !bytes.Contains(state, []byte("ActiveState=inactive")) {
			t.Error("API fixture unit did not cleanly stop")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 190*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "sudo", args...).CombinedOutput()
	if writeErr := os.WriteFile(get("LOG"), output, 0600); writeErr != nil {
		t.Fatal("API receipt log could not be preserved")
	}
	if err != nil {
		t.Fatal("API cross-service test failed; private receipt log retained")
	}
	if !bytes.Contains(output, []byte("--- PASS: TestKelvoOperationCrossServiceProcess")) || bytes.Contains(output, []byte("--- SKIP:")) || bytes.Contains(output, []byte("--- FAIL:")) {
		t.Fatal("API fixture did not complete required assertions")
	}
	if _, err = os.Stat(config["ready_file"].(string)); err != nil {
		t.Fatal("real API resolver never started")
	}
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--- PASS:") {
			t.Log(strings.TrimSpace(line))
		}
	}
}
