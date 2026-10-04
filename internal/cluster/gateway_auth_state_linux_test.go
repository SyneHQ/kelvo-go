// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
//go:build linux

package cluster

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/authstate"
	"go.yaml.in/yaml/v3"
)

func durableAuthFiles(t *testing.T, raw []byte, tenants map[string]bool) GatewayAuthenticationConfig {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	config := GatewayAuthenticationConfig{KeysFile: filepath.Join(root, "keys.yml"), ReloadInterval: time.Second, MinRevision: 1,
		State: &GatewayAuthenticationStateConfig{Directory: filepath.Join(root, "auth-state"), Scope: "gateway-a"}}
	if err := os.WriteFile(config.KeysFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := GatewayConfig{Authentication: &config}
	for id := range tenants {
		policy := testPolicy()
		policy.TenantID = id
		cfg.Tenants = append(cfg.Tenants, TenantConfig{Policy: policy})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := InitializeGatewayAuthState(ctx, cfg); err != nil {
		t.Fatal("private state initialization failed", err)
	}
	return config
}

func openDurableAuth(t *testing.T, cfg GatewayAuthenticationConfig, tenants map[string]bool, raw []byte, wantSuccess bool) {
	t.Helper()
	a, err := newGatewayAuthenticator(cfg, tenants, func(context.Context, string, int) ([]byte, error) { return append([]byte(nil), raw...), nil })
	if !wantSuccess {
		if a != nil {
			_ = a.close()
			t.Fatal("rejected durable history activated authority")
		}
		if err == nil || strings.Contains(err.Error(), rotationOld) || strings.Contains(err.Error(), cfg.State.Directory) {
			t.Fatal("rejection missing or private details exposed")
		}
		return
	}
	if err != nil {
		t.Fatal("same-owner durable authority failed", err)
	}
	if !a.ready() {
		t.Error("accepted durable authority not ready")
	}
	if err := a.close(); err != nil {
		t.Fatal("durable authority failed to release cleanly", err)
	}
}

func principalStateDocument(t *testing.T, revision uint64, a, b map[string][]string) []byte {
	t.Helper()
	raw, err := yaml.Marshal(gatewayKeyDocument{Version: 2, Revision: revision, Principals: map[string]map[string][]string{"a": a, "b": b}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestGatewayAuthStateRestartRetainsRevisionAndExactDocumentDigest(t *testing.T) {
	tenants := map[string]bool{"a": true, "b": true}
	raw := rotationDocument(t, 10, map[string][]string{"a": {rotationOld}, "b": {rotationOther}})
	cfg := durableAuthFiles(t, raw, tenants)
	before, err := os.Stat(filepath.Join(cfg.State.Directory, "state.yml"))
	if err != nil {
		t.Fatal(err)
	}
	openDurableAuth(t, cfg, tenants, raw, true)
	unchanged, err := os.Stat(filepath.Join(cfg.State.Directory, "state.yml"))
	if err != nil || !os.SameFile(before, unchanged) || !before.ModTime().Equal(unchanged.ModTime()) {
		t.Fatal("identical accepted document rewrote durable state")
	}
	openDurableAuth(t, cfg, tenants, rotationDocument(t, 9, map[string][]string{"a": {rotationOld}, "b": {rotationOther}}), false)
	// Even a formatting-only change at the same revision is equivocation.
	openDurableAuth(t, cfg, tenants, append(append([]byte(nil), raw...), []byte("\n# changed document\n")...), false)
	openDurableAuth(t, cfg, tenants, raw, true)
	state, err := os.ReadFile(filepath.Join(cfg.State.Directory, "state.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(state, []byte(rotationOld)) || bytes.Contains(state, []byte(rotationOther)) {
		t.Fatal("raw key persisted in authentication state")
	}
	info, err := os.Stat(filepath.Join(cfg.State.Directory, "state.yml"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("authentication state is not private")
	}
}

func TestGatewayAuthStateRestartFencesRemovedTenantAndPrincipalOwners(t *testing.T) {
	tenants := map[string]bool{"a": true, "b": true}
	v1 := func(rev uint64, a, b []string) []byte {
		return rotationDocument(t, rev, map[string][]string{"a": a, "b": b})
	}
	v2 := func(rev uint64, a, b map[string][]string) []byte { return principalStateDocument(t, rev, a, b) }
	cases := []struct {
		name                              string
		initial, removed, moved, restored []byte
	}{
		{"tenant", v1(1, []string{rotationOld}, []string{rotationOther}), v1(2, []string{}, []string{rotationOther}), v1(3, []string{}, []string{rotationOther, rotationOld}), v1(3, []string{rotationOld}, []string{rotationOther})},
		{"principal", v2(1, map[string][]string{"analyst": {rotationOld}}, map[string][]string{"reports": {rotationOther}}), v2(2, map[string][]string{}, map[string][]string{"reports": {rotationOther}}), v2(3, map[string][]string{"reports": {rotationOld}}, map[string][]string{"reports": {rotationOther}}), v2(3, map[string][]string{"analyst": {rotationOld}}, map[string][]string{"reports": {rotationOther}})},
		{"tenant-to-principal", v1(1, []string{rotationOld}, []string{}), v1(2, []string{}, []string{}), v2(3, map[string][]string{"analyst": {rotationOld}}, map[string][]string{}), v1(3, []string{rotationOld}, []string{})},
		{"principal-to-tenant", v2(1, map[string][]string{"analyst": {rotationOld}}, map[string][]string{}), v2(2, map[string][]string{}, map[string][]string{}), v1(3, []string{rotationOld}, []string{}), v2(3, map[string][]string{"analyst": {rotationOld}}, map[string][]string{})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := durableAuthFiles(t, tc.initial, tenants)
			openDurableAuth(t, cfg, tenants, tc.removed, true)
			openDurableAuth(t, cfg, tenants, tc.moved, false)
			openDurableAuth(t, cfg, tenants, tc.restored, true)
		})
	}
}

func TestGatewayAuthStateConfiguredFailuresNeverFallBackOrReset(t *testing.T) {
	tenants := map[string]bool{"a": true, "b": true}
	raw := rotationDocument(t, 1, map[string][]string{"a": {rotationOld}, "b": {rotationOther}})
	for _, failure := range []string{"missing-directory", "missing-state", "corrupt", "scope", "tenant-set"} {
		t.Run(failure, func(t *testing.T) {
			cfg := durableAuthFiles(t, raw, tenants)
			currentTenants := tenants
			switch failure {
			case "missing-directory":
				cfg.State.Directory += "-missing"
			case "missing-state":
				if err := os.Remove(filepath.Join(cfg.State.Directory, "state.yml")); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				if err := os.WriteFile(filepath.Join(cfg.State.Directory, "state.yml"), []byte("invalid-private-state\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "scope":
				cfg.State.Scope = "other-gateway"
			case "tenant-set":
				currentTenants = map[string]bool{"a": true}
			}
			openDurableAuth(t, cfg, currentTenants, raw, false)
			if failure == "missing-directory" {
				if _, err := os.Stat(cfg.State.Directory); !os.IsNotExist(err) {
					t.Fatal("startup recreated missing durable state")
				}
			}
		})
	}
}

func TestGatewayAuthStateInitializerIsCreateOnlyAndExactTenantBound(t *testing.T) {
	tenants := map[string]bool{"a": true}
	raw := rotationDocument(t, 1, map[string][]string{"a": {rotationOld}})
	cfg := durableAuthFiles(t, raw, tenants)
	path := filepath.Join(cfg.State.Directory, "state.yml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	config := GatewayConfig{Authentication: &cfg, Tenants: []TenantConfig{{Policy: testPolicy()}}}
	if err := InitializeGatewayAuthState(context.Background(), config); err == nil {
		t.Fatal("initializer overwrote existing history")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("refused reinitialization changed state")
	}
	config.Authentication.State = &GatewayAuthenticationStateConfig{Directory: filepath.Join(filepath.Dir(cfg.State.Directory), "new-state"), Scope: "gateway-b"}
	duplicate := config.Tenants[0]
	config.Tenants = append(config.Tenants, duplicate)
	if err := InitializeGatewayAuthState(context.Background(), config); err != errGatewayAuthConfig {
		t.Fatal("initializer accepted duplicate tenant scope")
	}
	if _, err := os.Stat(config.Authentication.State.Directory); !os.IsNotExist(err) {
		t.Fatal("invalid initialization created state")
	}
}

func TestGatewayAuthStateLaterConstructorFailureReleasesWriter(t *testing.T) {
	workerTLS, _, _ := tlsFiles(t, GatewayIdentity, nil, nil)
	for _, failure := range []string{"audit", "exports"} {
		t.Run(failure, func(t *testing.T) {
			identities := map[string]bool{"a": true}
			raw := rotationDocument(t, 1, map[string][]string{"a": {rotationOld}})
			auth := durableAuthFiles(t, raw, identities)
			policy := testPolicy()
			cfg := GatewayConfig{WorkerTLS: workerTLS, MaxHTTPRequests: 4, Authentication: &auth,
				Tenants: []TenantConfig{{Policy: policy, Workers: []Endpoint{{ID: "a1", URL: "https://127.0.0.1:1"}}}}}
			if failure == "audit" {
				cfg.Audit = &ServiceAuditConfig{}
			} else {
				cfg.Exports = &GatewayExportConfig{}
			}
			gateway, err := NewGateway(cfg, map[string]Store{"a": &gatewayStore{policy: policy}})
			if gateway != nil {
				_ = gateway.Close()
				t.Fatal("constructor accepted invalid later component")
			}
			if err == nil || !strings.Contains(err.Error(), strings.TrimSuffix(failure, "s")) {
				t.Fatal("fixture did not reach intended constructor failure", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			store, err := authstate.Open(ctx, auth.State.Directory, gatewayAuthScope(auth.State, identities))
			if err != nil {
				t.Fatal("later constructor failure retained authentication writer", err)
			}
			if err := store.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
