// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/authfence"
	"go.yaml.in/yaml/v3"
)

func gatewayAuthorityBindingFixture(t *testing.T) GatewayConfig {
	t.Helper()
	scope, err := authfence.NewScope("fleet", []string{"a", "b"}, []string{"west", "east"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := GatewayConfig{Authentication: &GatewayAuthenticationConfig{
		KeysFile: "/missing/keys.yml", State: &GatewayAuthenticationStateConfig{Directory: "/missing/state", Scope: "local-east"},
		Authority: &GatewayKeyAuthorityConfig{Scope: "fleet", ReplicaID: "east", Gateways: []string{"west", "east"},
			NATS: NATSConfig{URL: "tls://localhost:4223", CAFile: "/missing/authority-ca.pem", CredentialsFile: "/missing/authority.creds"}},
	}}
	for _, id := range []string{"a", "b"} {
		p := principalTestPolicy()
		p.TenantID = id
		binding := keyAuthorityBinding(scope)
		p.Access.KeyAuthority = &binding
		cfg.Tenants = append(cfg.Tenants, TenantConfig{Policy: p})
	}
	return cfg
}

func TestGatewayKeyAuthorityBindingRequiresExactMembership(t *testing.T) {
	cfg := gatewayAuthorityBindingFixture(t)
	bound, err := bindGatewayKeyAuthority(cfg)
	if err != nil || bound == nil || bound.config.ReplicaID != "east" ||
		!slices.Equal(bound.config.Scope.Tenants(), []string{"a", "b"}) || !slices.Equal(bound.config.Scope.Gateways(), []string{"east", "west"}) {
		t.Fatal("complete authority policy did not bind", err)
	}
	// Input order is not membership. Only NewScope supplies the canonical digest.
	slices.Reverse(cfg.Tenants)
	slices.Reverse(cfg.Authentication.Authority.Gateways)
	other, err := bindGatewayKeyAuthority(cfg)
	if err != nil || other.binding != bound.binding || !other.config.Scope.Equal(bound.config.Scope) {
		t.Fatal("roster order changed authority membership", err)
	}
	for name, mutate := range map[string]func(*GatewayConfig){
		"authentication omitted": func(c *GatewayConfig) { c.Authentication = nil },
		"authority omitted":      func(c *GatewayConfig) { c.Authentication.Authority = nil },
		"state omitted":          func(c *GatewayConfig) { c.Authentication.State = nil },
		"slow renewal":           func(c *GatewayConfig) { c.Authentication.ReloadInterval = 2 * time.Second },
		"legacy token":           func(c *GatewayConfig) { c.Tenants[0].TokenEnv = "KELVO_LEGACY_TOKEN" },
		"access omitted":         func(c *GatewayConfig) { c.Tenants[0].Policy.Access = nil },
		"mixed binding":          func(c *GatewayConfig) { c.Tenants[0].Policy.Access.KeyAuthority = nil },
		"foreign scope":          func(c *GatewayConfig) { c.Authentication.Authority.Scope = "other" },
		"foreign policy scope":   func(c *GatewayConfig) { c.Tenants[0].Policy.Access.KeyAuthority.Scope = "other" },
		"foreign policy digest": func(c *GatewayConfig) {
			c.Tenants[0].Policy.Access.KeyAuthority.MembershipSHA256 = strings.Repeat("f", 64)
		},
		"missing tenant":    func(c *GatewayConfig) { c.Tenants = c.Tenants[:1] },
		"duplicate tenant":  func(c *GatewayConfig) { c.Tenants[1].Policy.TenantID = "a" },
		"changed tenant":    func(c *GatewayConfig) { c.Tenants[1].Policy.TenantID = "c" },
		"invalid tenant":    func(c *GatewayConfig) { c.Tenants[1].Policy.TenantID = "../c" },
		"no tenants":        func(c *GatewayConfig) { c.Tenants = nil },
		"no gateways":       func(c *GatewayConfig) { c.Authentication.Authority.Gateways = nil },
		"duplicate gateway": func(c *GatewayConfig) { c.Authentication.Authority.Gateways = []string{"east", "east"} },
		"missing gateway":   func(c *GatewayConfig) { c.Authentication.Authority.Gateways = []string{"east"} },
		"foreign replica":   func(c *GatewayConfig) { c.Authentication.Authority.ReplicaID = "north" },
		"control replica":   func(c *GatewayConfig) { c.Authentication.Authority.ReplicaID = authfence.ControlReplicaID },
		"control in roster": func(c *GatewayConfig) {
			c.Authentication.Authority.Gateways = []string{"east", authfence.ControlReplicaID}
		},
		"plaintext broker":     func(c *GatewayConfig) { c.Authentication.Authority.NATS.URL = "nats://localhost:4223" },
		"missing authority CA": func(c *GatewayConfig) { c.Authentication.Authority.NATS.CAFile = "" },
		"relative credential":  func(c *GatewayConfig) { c.Authentication.Authority.NATS.CredentialsFile = "authority.creds" },
		"mixed credentials":    func(c *GatewayConfig) { c.Authentication.Authority.NATS.PasswordEnv = "KELVO_AUTH_PASSWORD" },
	} {
		t.Run(name, func(t *testing.T) {
			c := gatewayAuthorityBindingFixture(t)
			mutate(&c)
			if value, err := bindGatewayKeyAuthority(c); value != nil || err != errGatewayAuthConfig {
				t.Fatal("invalid binding returned a usable authority", err)
			}
			if err := validateGatewayAuthentication(&c); err != errGatewayAuthConfig {
				t.Fatal("gateway validation bypassed authority binding", err)
			}
		})
	}
}

func TestGatewayKeyAuthorityConfigurationIsDetached(t *testing.T) {
	cfg := gatewayAuthorityBindingFixture(t)
	normalized, err := cfg.Authentication.normalized()
	if err != nil || normalized.State == cfg.Authentication.State || normalized.Authority == cfg.Authentication.Authority || normalized.ReloadInterval != time.Second {
		t.Fatal("normalization did not detach authority configuration", err)
	}
	bound, err := bindGatewayKeyAuthority(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := bound.binding
	cfg.Authentication.Authority.Scope = "changed"
	cfg.Authentication.Authority.Gateways[0] = "changed"
	cfg.Authentication.Authority.NATS.CredentialsFile = "/changed/credentials"
	cfg.Authentication.State.Scope = "changed"
	cfg.Tenants[0].Policy.Access.KeyAuthority.Scope = "changed"
	if normalized.Authority.Scope != "fleet" || normalized.Authority.Gateways[0] != "west" || normalized.State.Scope != "local-east" ||
		bound.binding != want || bound.config.Scope.ID() != "fleet" || bound.config.CredentialsFile != "/missing/authority.creds" {
		t.Fatal("caller mutation changed normalized or compiled authority")
	}
	returned := bound.config.Scope.Gateways()
	returned[0] = "changed"
	if bound.config.Scope.Gateways()[0] != "east" {
		t.Fatal("compiled scope exposed its roster")
	}
}

func TestGatewayKeyAuthorityRuntimeRequiresCompiledBinding(t *testing.T) {
	cfg := gatewayAuthorityBindingFixture(t)
	bound, err := bindGatewayKeyAuthority(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tenants := map[string]bool{"a": true, "b": true}
	if err := validateGatewayKeyAuthorityRuntime(*cfg.Authentication, tenants, bound); err != nil {
		t.Fatal(err)
	}
	if err := validateGatewayKeyAuthorityRuntime(*cfg.Authentication, tenants, nil); err != errGatewayAuthConfig {
		t.Fatal("raw authority configuration bypassed compiled binding")
	}
	legacy := *cfg.Authentication
	legacy.Authority = nil
	if err := validateGatewayKeyAuthorityRuntime(legacy, tenants, bound); err != errGatewayAuthConfig {
		t.Fatal("compiled authority bypassed opt-in")
	}
	if err := validateGatewayKeyAuthorityRuntime(legacy, tenants, nil); err != nil {
		t.Fatal("legacy authentication changed", err)
	}
	for name, mutate := range map[string]func(*gatewayKeyAuthority){
		"binding":     func(a *gatewayKeyAuthority) { a.binding.Scope = "other" },
		"URL":         func(a *gatewayKeyAuthority) { a.config.URL = "tls://localhost:4321" },
		"CA":          func(a *gatewayKeyAuthority) { a.config.CAFile = "/other/ca" },
		"certificate": func(a *gatewayKeyAuthority) { a.config.CertFile = "/other/cert" },
		"key":         func(a *gatewayKeyAuthority) { a.config.KeyFile = "/other/key" },
		"credentials": func(a *gatewayKeyAuthority) { a.config.CredentialsFile = "/other/creds" },
		"username":    func(a *gatewayKeyAuthority) { a.config.Username = "other" },
		"password":    func(a *gatewayKeyAuthority) { a.config.PasswordEnv = "KELVO_OTHER_PASSWORD" },
		"replica":     func(a *gatewayKeyAuthority) { a.config.ReplicaID = "west" },
		"scope": func(a *gatewayKeyAuthority) {
			a.config.Scope, _ = authfence.NewScope("other", []string{"a", "b"}, []string{"east", "west"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := *bound
			mutate(&changed)
			if err := validateGatewayKeyAuthorityRuntime(*cfg.Authentication, tenants, &changed); err != errGatewayAuthConfig {
				t.Fatal("inconsistent runtime authority accepted", err)
			}
		})
	}
	for _, changed := range []map[string]bool{{"a": true}, {"a": true, "b": false}, {"a": true, "c": true}} {
		if err := validateGatewayKeyAuthorityRuntime(*cfg.Authentication, changed, bound); err != errGatewayAuthConfig {
			t.Fatal("runtime membership mismatch accepted", err)
		}
	}
}

func TestGatewayKeyAuthorityYAMLValidatesReferencesBeforeIO(t *testing.T) {
	root := t.TempDir()
	var cfg GatewayConfig
	if err := yaml.Unmarshal([]byte(gatewayYAML), &cfg); err != nil {
		t.Fatal(err)
	}
	auth := gatewayAuthorityBindingFixture(t).Authentication
	auth.KeysFile = "keys.yml"
	auth.State.Directory = filepath.Join(root, "state")
	auth.Authority.NATS.CAFile = "authority-ca.pem"
	auth.Authority.NATS.CredentialsFile = "authority.creds"
	cfg.Authentication = auth
	cfg.Tenants[0].TokenEnv = ""
	cfg.Tenants[0].Policy.Access = principalTestPolicy().Access
	scope, err := authfence.NewScope("fleet", []string{"a"}, auth.Authority.Gateways)
	if err != nil {
		t.Fatal(err)
	}
	binding := keyAuthorityBinding(scope)
	cfg.Tenants[0].Policy.Access.KeyAuthority = &binding
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "gateway.yml")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadGateway(path)
	if err != nil || loaded.Authentication.Authority.NATS.CAFile != filepath.Join(root, "authority-ca.pem") ||
		loaded.Authentication.Authority.NATS.CredentialsFile != filepath.Join(root, "authority.creds") || loaded.Authentication.KeysFile != filepath.Join(root, "keys.yml") {
		t.Fatal("authority references did not resolve without opening files", err)
	}
	if _, err := os.Stat(auth.State.Directory); !os.IsNotExist(err) {
		t.Fatal("configuration loading touched the state directory")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 || entries[0].Name() != "gateway.yml" {
		t.Fatal("configuration loading created authority files", err)
	}
	for name, text := range map[string]string{
		"unknown field":   strings.Replace(string(raw), "replica_id: east", "replica_id: east\n        enabled: true", 1),
		"duplicate field": strings.Replace(string(raw), "replica_id: east", "replica_id: east\n        replica_id: west", 1),
		"membership":      strings.Replace(string(raw), binding.MembershipSHA256, strings.Repeat("f", 64), 1),
		"replica":         strings.Replace(string(raw), "replica_id: east", "replica_id: control", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if reflect.DeepEqual([]byte(text), raw) {
				t.Fatal("fixture mutation did not change YAML")
			}
			if err := os.WriteFile(path, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadGateway(path); err == nil {
				t.Fatal("invalid authority YAML accepted")
			}
		})
	}
}
