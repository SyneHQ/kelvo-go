// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/authstate"
)

func TestGatewayAuthStateConfigurationIsOptInCanonicalAndDetached(t *testing.T) {
	base := authStateTestConfig()
	base.State = nil
	if normalized, err := base.normalized(); err != nil || normalized.State != nil {
		t.Fatal("memory-only authentication changed")
	}
	for _, scope := range []string{"a", "gateway-replica-1", "a" + strings.Repeat("b", 62)} {
		base.State = &GatewayAuthenticationStateConfig{Directory: "/var/lib/kelvo/auth", Scope: scope}
		normalized, err := base.normalized()
		if err != nil || normalized.State == base.State || *normalized.State != *base.State {
			t.Fatal("state configuration was rejected or shared")
		}
		base.State.Scope = "changed"
		if normalized.State.Scope != scope {
			t.Fatal("normalization retained a mutable state-config pointer")
		}
	}
	for _, state := range []GatewayAuthenticationStateConfig{
		{}, {Directory: "/var/lib/kelvo/auth", Scope: ""},
		{Directory: "/var/lib/kelvo/auth", Scope: "Uppercase"},
		{Directory: "/var/lib/kelvo/auth", Scope: "a" + strings.Repeat("b", 63)},
		{Directory: "/var/lib/kelvo/auth", Scope: "../gateway"},
		{Directory: "relative/state", Scope: "gateway"}, {Directory: "/", Scope: "gateway"},
		{Directory: "/var/lib/kelvo/../auth", Scope: "gateway"},
		{Directory: "/var/lib/kelvo/auth/", Scope: "gateway"},
		{Directory: "/var/lib/kelvo/auth\x00private", Scope: "gateway"},
	} {
		base.State = &state
		if _, err := base.normalized(); err != errGatewayAuthConfig {
			t.Fatal("invalid state configuration accepted")
		}
	}
}

func TestGatewayAuthStateYAMLValidatesWithoutTouchingStorage(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "gateway.yml")
	directory := filepath.Join(root, "state")
	base := strings.Replace(gatewayYAML, "token_env: KELVO_TOKEN_A", "", 1) +
		"authentication:\n  keys_file: keys.yml\n  state:\n    directory: " + directory + "\n    scope: gateway-a\n"
	if err := os.WriteFile(path, []byte(base), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadGateway(path)
	if err != nil || cfg.Authentication.State.Directory != directory || cfg.Authentication.KeysFile != filepath.Join(root, "keys.yml") {
		t.Fatal("durable authentication YAML failed to load")
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatal("configuration loading touched durable state")
	}
	for _, raw := range []string{
		strings.Replace(base, directory, "relative-state", 1),
		strings.Replace(base, "scope: gateway-a", "scope: gateway-a\n    reset: true", 1),
		strings.Replace(base, "scope: gateway-a", "scope: gateway-a\n    scope: gateway-b", 1),
		strings.Replace(base, "- \n", "- token_env: KELVO_TOKEN_A\n", 1),
	} {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadGateway(path); err == nil {
			t.Fatal("unsafe or ambiguous state configuration accepted")
		}
	}
}

func TestGatewayAuthStateScopeAndCandidateRetainOnlyDetachedFingerprints(t *testing.T) {
	set := principalKeySet(t, 3, map[string][]string{"analyst": {rotationOld}}, map[string][]string{"reports": {rotationOther}})
	candidate := gatewayAuthCandidate(set)
	scope := gatewayAuthScope(&GatewayAuthenticationStateConfig{Scope: "gateway-a"}, map[string]bool{"b": true, "a": true})
	if !reflect.DeepEqual(scope, authstate.Scope{ID: "gateway-a", Tenants: []string{"a", "b"}}) || candidate.Revision != set.revision || candidate.DocumentSHA256 != set.digest {
		t.Fatal("state scope or byte-level document digest changed")
	}
	for hash, owner := range candidate.Owners {
		if owner.TenantID != set.keys[hash] || owner.PrincipalID != set.principals[hash] {
			t.Fatal("principal ownership lost")
		}
		delete(set.keys, hash)
	}
	if len(candidate.Owners) != 2 {
		t.Fatal("candidate shares mutable input ownership")
	}
}

func TestGatewayAuthStateInitializerRejectsMissingConfigBeforeIO(t *testing.T) {
	for _, cfg := range []GatewayConfig{{}, {Authentication: &GatewayAuthenticationConfig{KeysFile: "/missing-private-key"}},
		{Authentication: &GatewayAuthenticationConfig{KeysFile: "/missing-private-key", State: &GatewayAuthenticationStateConfig{Directory: "relative", Scope: "gateway"}}}} {
		if err := InitializeGatewayAuthState(context.Background(), cfg); err != errGatewayAuthConfig {
			t.Fatal("initializer accepted missing or invalid configuration")
		}
	}
}
