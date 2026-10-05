// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/secrets"
	"go.yaml.in/yaml/v3"
)

const gatewayKeyFileLimit = secrets.MaxDocumentBytes
const gatewayMaxTenantKeys = 4
const gatewayKeyReadTimeout = 2 * time.Second

var errGatewayAuthConfig = errors.New("cluster gateway: invalid authentication configuration")
var errGatewayAuthUnavailable = errors.New("cluster gateway: authentication keys unavailable")

// GatewayAuthenticationConfig is opt-in and excludes every legacy token_env.
// Its file contains service keys for the fixed, already-provisioned tenants.
type GatewayAuthenticationConfig struct {
	KeysFile       string                            `yaml:"keys_file"`
	ReloadInterval time.Duration                     `yaml:"reload_interval,omitempty"`
	MinRevision    uint64                            `yaml:"min_revision,omitempty"`
	State          *GatewayAuthenticationStateConfig `yaml:"state,omitempty"`
}

func (c GatewayAuthenticationConfig) normalized() (GatewayAuthenticationConfig, error) {
	if c.ReloadInterval == 0 {
		c.ReloadInterval = time.Second
	}
	if c.MinRevision == 0 {
		c.MinRevision = 1
	}
	if len(c.KeysFile) > 4096 || !filepath.IsAbs(c.KeysFile) || filepath.Clean(c.KeysFile) != c.KeysFile || c.KeysFile == "/" || c.ReloadInterval < time.Second || c.ReloadInterval > time.Minute {
		return c, errGatewayAuthConfig
	}
	if c.State != nil {
		state := *c.State
		if !validGatewayAuthStateConfig(state) {
			return c, errGatewayAuthConfig
		}
		c.State = &state
	}
	return c, nil
}

func validateGatewayAuthentication(c *GatewayConfig) error {
	if len(c.Tenants) == 0 || len(c.Tenants) > 256 {
		return errGatewayAuthConfig
	}
	if c.Authentication != nil {
		normalized, err := c.Authentication.normalized()
		if err != nil {
			return err
		}
		c.Authentication = &normalized
	}
	for _, tenant := range c.Tenants {
		if tenant.Policy.Access != nil && c.Authentication == nil {
			return errGatewayAuthConfig
		}
		if (c.Authentication == nil) != (tenant.TokenEnv != "") {
			return errGatewayAuthConfig
		}
	}
	return nil
}

type gatewayKeySet struct {
	version    int // Preserve provenance even when every configured key is disabled.
	revision   uint64
	digest     [32]byte
	keys       map[[32]byte]string
	principals map[[32]byte]string
}

type gatewayKeyDocument struct {
	Version    int                            `yaml:"version"`
	Revision   uint64                         `yaml:"revision"`
	Tenants    map[string][]string            `yaml:"tenants,omitempty"`
	Principals map[string]map[string][]string `yaml:"principals,omitempty"`
}

// Parse only a small plain YAML tree. Aliases/anchors/merge keys are unnecessary
// for secrets and must never turn the encoded bound into unbounded expansion.
func plainKeyYAML(node *yaml.Node, depth int, count *int) bool {
	(*count)++
	if depth > 6 || *count > 4096 || node.Kind == yaml.AliasNode || node.Anchor != "" || node.Value == "<<" {
		return false
	}
	for _, child := range node.Content {
		if !plainKeyYAML(child, depth+1, count) {
			return false
		}
	}
	return true
}

func parseGatewayKeys(raw []byte, tenants map[string]bool, minimum uint64) (gatewayKeySet, error) {
	bad := func() (gatewayKeySet, error) { return gatewayKeySet{}, errGatewayAuthUnavailable }
	if len(raw) == 0 || len(raw) > gatewayKeyFileLimit || len(tenants) == 0 || len(tenants) > 256 {
		return bad()
	}
	var tree yaml.Node
	if yaml.Unmarshal(raw, &tree) != nil || len(tree.Content) != 1 || tree.Content[0].Kind != yaml.MappingNode {
		return bad()
	}
	count := 0
	if !plainKeyYAML(&tree, 0, &count) {
		return bad()
	}
	var document gatewayKeyDocument
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if decoder.Decode(&document) != nil || decoder.Decode(new(any)) != io.EOF || (document.Version != 1 && document.Version != 2) || document.Revision == 0 || document.Revision < minimum {
		return bad()
	}
	result := gatewayKeySet{version: document.Version, revision: document.Revision, digest: sha256.Sum256(raw), keys: make(map[[32]byte]string), principals: make(map[[32]byte]string)}
	if document.Version == 2 {
		return parsePrincipalKeys(document, tenants, result)
	}
	if document.Principals != nil || len(document.Tenants) != len(tenants) {
		return bad()
	}
	for tenant, keys := range document.Tenants {
		if !tenants[tenant] || keys == nil || len(keys) > gatewayMaxTenantKeys {
			return bad()
		}
		for _, key := range keys {
			if len(key) < 32 || len(key) > 256 {
				return bad()
			}
			for _, character := range []byte(key) {
				if character < 33 || character > 126 || character == 44 {
					return bad()
				}
			}
			digest := sha256.Sum256([]byte(key))
			if _, duplicate := result.keys[digest]; duplicate {
				return bad()
			}
			result.keys[digest] = tenant
		}
	}
	return result, nil
}
