// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"encoding/hex"

	"github.com/SYNEHQ/kelvo-go/internal/authfence"
)

// GatewayKeyAuthorityConfig references a separate authority broker identity.
// The tenant roster comes only from the gateway's complete tenant policies.
type GatewayKeyAuthorityConfig struct {
	Scope     string     `yaml:"scope"`
	ReplicaID string     `yaml:"replica_id"`
	Gateways  []string   `yaml:"gateways"`
	NATS      NATSConfig `yaml:"nats"`
}

// KeyAuthorityBinding pins fleet membership, not a rotating key document.
// A change requires draining and reprovisioning every participating tenant.
type KeyAuthorityBinding struct {
	Version          int    `json:"version" yaml:"version"`
	Scope            string `json:"scope" yaml:"scope"`
	MembershipSHA256 string `json:"membership_sha256" yaml:"membership_sha256"`
}

// The compiled authority owns a detached Scope and scalar configuration.
// Authentication constructors retain their own copy of this value.
type gatewayKeyAuthority struct {
	config  authfence.Config
	binding KeyAuthorityBinding
}

func validKeyAuthorityBinding(binding *KeyAuthorityBinding) bool {
	if binding == nil {
		return true
	}
	if binding.Version != authfence.Version || !gatewayAuthStateScope.MatchString(binding.Scope) || len(binding.MembershipSHA256) != 64 {
		return false
	}
	raw, err := hex.DecodeString(binding.MembershipSHA256)
	return err == nil && hex.EncodeToString(raw) == binding.MembershipSHA256
}

func keyAuthorityBinding(scope authfence.Scope) KeyAuthorityBinding {
	return KeyAuthorityBinding{Version: authfence.Version, Scope: scope.ID(), MembershipSHA256: scope.MembershipSHA256()}
}

// compileGatewayKeyAuthority validates references only. It never reads a file,
// resolves credentials, constructs a client or contacts a broker.
func compileGatewayKeyAuthority(config GatewayAuthenticationConfig, tenants map[string]bool) (authfence.Config, error) {
	c, err := config.normalized()
	if err != nil || c.Authority == nil || len(tenants) == 0 || len(tenants) > authfence.MaxTenants {
		return authfence.Config{}, errGatewayAuthConfig
	}
	ids := make([]string, 0, len(tenants))
	for id, enabled := range tenants {
		if !enabled || !clusterID.MatchString(id) {
			return authfence.Config{}, errGatewayAuthConfig
		}
		ids = append(ids, id)
	}
	a := c.Authority
	scope, err := authfence.NewScope(a.Scope, ids, a.Gateways)
	if err != nil || !scope.HasReplica(a.ReplicaID) {
		return authfence.Config{}, errGatewayAuthConfig
	}
	n := a.NATS
	compiled := authfence.Config{
		URL: n.URL, CAFile: n.CAFile, CertFile: n.CertFile, KeyFile: n.KeyFile,
		CredentialsFile: n.CredentialsFile, Username: n.Username, PasswordEnv: n.PasswordEnv,
		Scope: scope, ReplicaID: a.ReplicaID,
	}
	if compiled.Validate() != nil {
		return authfence.Config{}, errGatewayAuthConfig
	}
	return compiled, nil
}

// bindGatewayKeyAuthority checks every tenant before any authentication or
// broker work. Omission is valid only when no policy requests this authority.
func bindGatewayKeyAuthority(cfg GatewayConfig) (*gatewayKeyAuthority, error) {
	if cfg.Authentication == nil || cfg.Authentication.Authority == nil {
		for _, tenant := range cfg.Tenants {
			if tenant.Policy.Access != nil && tenant.Policy.Access.KeyAuthority != nil {
				return nil, errGatewayAuthConfig
			}
		}
		return nil, nil
	}
	if len(cfg.Tenants) == 0 || len(cfg.Tenants) > authfence.MaxTenants {
		return nil, errGatewayAuthConfig
	}
	tenants := make(map[string]bool, len(cfg.Tenants))
	for _, tenant := range cfg.Tenants {
		p := tenant.Policy
		if tenants[p.TenantID] || tenant.TokenEnv != "" || p.Access == nil || p.Access.KeyAuthority == nil || validatePrincipalPolicy(p.Access) != nil {
			return nil, errGatewayAuthConfig
		}
		tenants[p.TenantID] = true
	}
	compiled, err := compileGatewayKeyAuthority(*cfg.Authentication, tenants)
	if err != nil {
		return nil, err
	}
	binding := keyAuthorityBinding(compiled.Scope)
	for _, tenant := range cfg.Tenants {
		if *tenant.Policy.Access.KeyAuthority != binding {
			return nil, errGatewayAuthConfig
		}
	}
	return &gatewayKeyAuthority{config: compiled, binding: binding}, nil
}

// validateGatewayKeyAuthorityRuntime prevents a direct authenticator caller
// from enabling raw configuration without its previously bound policy scope.
func validateGatewayKeyAuthorityRuntime(config GatewayAuthenticationConfig, tenants map[string]bool, authority *gatewayKeyAuthority) error {
	if config.Authority == nil {
		if authority != nil {
			return errGatewayAuthConfig
		}
		return nil
	}
	if authority == nil {
		return errGatewayAuthConfig
	}
	expected, err := compileGatewayKeyAuthority(config, tenants)
	if err != nil {
		return err
	}
	actual := authority.config
	if authority.binding != keyAuthorityBinding(expected.Scope) || !actual.Scope.Equal(expected.Scope) ||
		actual.URL != expected.URL || actual.CAFile != expected.CAFile || actual.CertFile != expected.CertFile || actual.KeyFile != expected.KeyFile ||
		actual.CredentialsFile != expected.CredentialsFile || actual.Username != expected.Username || actual.PasswordEnv != expected.PasswordEnv ||
		actual.ReplicaID != expected.ReplicaID {
		return errGatewayAuthConfig
	}
	return nil
}
