// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTLSTrustManagedConfigAndPrivateFiles(t *testing.T) {
	_, ca, _ := tlsFiles(t, GatewayIdentity, nil, nil)
	path := filepath.Join(t.TempDir(), "trust.yml")
	raw := trustDoc(t, 1, time.Now().Add(20*time.Minute), ca)
	publishTLSIdentity(t, path, raw)
	valid := TLSTrustConfig{File: path, MinimumEpoch: 1}
	for _, bad := range []TLSTrustConfig{{File: path}, {File: "relative", MinimumEpoch: 1}, {File: "/", MinimumEpoch: 1}, {File: path, MinimumEpoch: 1, ReloadInterval: time.Millisecond}, {File: path, MinimumEpoch: 1, ReloadInterval: 2 * time.Minute}} {
		if bad.validate() == nil {
			t.Fatal("invalid trust config accepted")
		}
	}
	if validateTLSRotation(TLSConfig{CAFile: "static.pem", Trust: &valid}) == nil {
		t.Fatal("static and rotating trust accepted together")
	}
	if _, err := BuildClientTLS(TLSConfig{Trust: &valid}, WorkerIdentity("a", "a1")); err == nil {
		t.Fatal("static client builder accepted unmanaged trust")
	}
	if _, err := BuildServerTLS(TLSConfig{Trust: &valid}, WorkerIdentity("a", "a1"), GatewayIdentity); err == nil {
		t.Fatal("static server builder accepted unmanaged trust")
	}
	if _, err := OpenServerTLS(TLSConfig{Trust: &valid}, "", ""); err == nil {
		t.Fatal("public listener accepted mTLS trust config")
	}
	configPath := filepath.Join(t.TempDir(), "gateway.yml")
	source := strings.Replace(gatewayYAML, "ca_file: ca.pem}", "trust: {file: trust.yml, minimum_epoch: 1, reload_interval: 2s}}", 1)
	if err := os.WriteFile(configPath, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadGateway(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.WorkerTLS.Trust == nil || loaded.WorkerTLS.Trust.File != filepath.Join(filepath.Dir(configPath), "trust.yml") || loaded.WorkerTLS.Trust.ReloadInterval != 2*time.Second {
		t.Fatal("trust config was not resolved")
	}
	public := strings.Replace(gatewayYAML, "tls: {cert_file: gateway.pem, key_file: gateway.key}", "tls: {cert_file: gateway.pem, key_file: gateway.key, trust: {file: trust.yml, minimum_epoch: 1}}", 1)
	if err := os.WriteFile(configPath, []byte(public), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadGateway(configPath); err == nil {
		t.Fatal("public gateway trust YAML accepted")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := newTLSTrust(valid, nil); err == nil {
		t.Fatal("world-readable trust document accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	linked := filepath.Join(filepath.Dir(path), "linked.yml")
	if err := os.Symlink(path, linked); err != nil {
		t.Fatal(err)
	}
	linkConfig := valid
	linkConfig.File = linked
	if _, err := newTLSTrust(linkConfig, nil); err == nil {
		t.Fatal("symlink trust accepted")
	}
	os.Remove(linked)
	if err := os.Link(path, linked); err != nil {
		t.Fatal(err)
	}
	if _, err := newTLSTrust(valid, nil); err == nil {
		t.Fatal("hard-linked trust accepted")
	}
	os.Remove(linked)
	publishTLSIdentity(t, path, make([]byte, 256<<10+1))
	if _, err := newTLSTrust(valid, nil); err == nil {
		t.Fatal("oversized trust document accepted")
	}
}

func TestTLSTrustGatewayAndServerManagedLifecycle(t *testing.T) {
	gateway, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	uri := WorkerIdentity("a", "a1")
	worker, workerRaw := rotatingTLSFixture(t, uri, ca, key)
	publishTLSIdentity(t, worker.IdentityFile, readinessLoopbackIdentity(t, workerRaw, ca, key))
	raw := trustDoc(t, 1, time.Now().Add(20*time.Minute), ca)
	workerTrust := TLSTrustConfig{File: filepath.Join(t.TempDir(), "worker-trust.yml"), MinimumEpoch: 1, ReloadInterval: time.Second}
	publishTLSIdentity(t, workerTrust.File, raw)
	worker.CAFile = ""
	worker.Trust = &workerTrust
	runtime, err := OpenServerTLS(worker, uri, GatewayIdentity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	fixture := &tlsDrainFixture{}
	if err := runtime.Handler(fixture).(interface{ Drain(context.Context) error }).Drain(context.Background()); err == nil || !fixture.drained {
		t.Fatal("managed trust swallowed server drain")
	}
	server := httptest.NewUnstartedServer(runtime.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})))
	server.TLS = runtime.Config
	server.StartTLS()
	t.Cleanup(server.Close)
	gatewayTrust := TLSTrustConfig{File: filepath.Join(t.TempDir(), "gateway-trust.yml"), MinimumEpoch: 1, ReloadInterval: time.Second}
	publishTLSIdentity(t, gatewayTrust.File, raw)
	gateway.CAFile = ""
	gateway.Trust = &gatewayTrust
	policy := testPolicy()
	t.Setenv("KELVO_TRUST_TEST_TOKEN", strings.Repeat("t", 32))
	config := GatewayConfig{WorkerTLS: gateway, MaxHTTPRequests: 8, Tenants: []TenantConfig{{Policy: policy, TokenEnv: "KELVO_TRUST_TEST_TOKEN", Workers: []Endpoint{{ID: "a1", URL: server.URL}}}}}
	managed, err := NewGateway(config, map[string]Store{"a": &gatewayStore{policy: policy}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { managed.Close() })
	endpoint := managed.tenants["a"].workers["a1"]
	transport, ok := endpoint.client.Transport.(*tlsTrustTransport)
	if !ok || transport.trust != managed.workerTrust {
		t.Fatal("gateway did not share its managed trust lifecycle")
	}
	requireTrustOK(t, endpoint.client, server.URL)
	status := func(path string) int {
		response := httptest.NewRecorder()
		managed.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		return response.Code
	}
	waitTLS(t, func() bool { return status("/ready") == http.StatusOK })
	publishTLSIdentity(t, gatewayTrust.File, []byte("invalid trust"))
	waitTLS(t, func() bool { return !managed.workerTrust.ready() })
	if status("/ready") != http.StatusServiceUnavailable || status("/health") != http.StatusOK {
		t.Fatal("gateway trust failure did not affect readiness alone")
	}
	if status, err := trustGET(endpoint.client, server.URL); err == nil && status == http.StatusNoContent {
		t.Fatal("gateway reused connection after local trust failure")
	}
	recovery := editTrustDoc(t, raw, func(d *tlsTrustDocument) { d.Epoch = 2 })
	publishTLSIdentity(t, gatewayTrust.File, recovery)
	waitTrustEpoch(t, managed.workerTrust, 2)
	requireTrustOK(t, endpoint.client, server.URL)
	publishTLSIdentity(t, workerTrust.File, []byte("invalid trust"))
	waitTLS(t, func() bool { return !runtime.trust.ready() })
	if status, err := trustGET(endpoint.client, server.URL); err == nil && status == http.StatusNoContent {
		t.Fatal("server reused connection after trust failure")
	}
	publishTLSIdentity(t, workerTrust.File, recovery)
	waitTrustEpoch(t, runtime.trust, 2)
	requireTrustOK(t, endpoint.client, server.URL)
	if err := managed.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.Close()
	if managed.workerTrust.ready() || runtime.trust.ready() {
		t.Fatal("closed lifecycle retained trust")
	}
}
