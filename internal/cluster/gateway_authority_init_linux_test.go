//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGatewayAuthorityOfflineInitializerRequiresV2(t *testing.T) {
	for _, test := range []struct {
		name string
		v2   bool
		keys bool
	}{
		{"v1-disabled", false, false}, {"v1-keys", false, true},
		{"v2-disabled", true, false}, {"v2-keys", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := gatewayAuthorityBindingFixture(t)
			root := t.TempDir()
			if err := os.Chmod(root, 0700); err != nil {
				t.Fatal(err)
			}
			cfg.Authentication.KeysFile = filepath.Join(root, "keys.yml")
			cfg.Authentication.State.Directory = filepath.Join(root, "state")
			var a, b = []string{}, []string{}
			if test.keys {
				a, b = []string{rotationOld}, []string{rotationOther}
			}
			raw := rotationDocument(t, 1, map[string][]string{"a": a, "b": b})
			if test.v2 {
				raw = principalStateDocument(t, 1, map[string][]string{"analyst": a}, map[string][]string{"reports": b})
			}
			if err := os.WriteFile(cfg.Authentication.KeysFile, raw, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := InitializeGatewayAuthState(ctx, cfg)
			if !test.v2 {
				if !errors.Is(err, errGatewayAuthUnavailable) {
					t.Fatal("v1 authority state initialization was not rejected")
				}
				if _, err := os.Lstat(cfg.Authentication.State.Directory); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("rejected v1 document created local state")
				}
				return
			}
			// Broker references deliberately remain nonexistent. Offline setup
			// only seeds local history; it neither verifies nor activates keys.
			if err != nil {
				t.Fatal("v2 offline initialization failed", err)
			}
			state, err := os.ReadFile(filepath.Join(cfg.Authentication.State.Directory, "state.yml"))
			if err != nil || bytes.Contains(state, []byte(rotationOld)) || bytes.Contains(state, []byte(rotationOther)) {
				t.Fatal("state missing or raw key persisted")
			}
		})
	}
}

func TestGatewayAuthorityPublicConstructorsRejectUnboundPolicy(t *testing.T) {
	for _, change := range []struct {
		name   string
		mutate func(*GatewayConfig)
	}{
		{"missing-config", func(c *GatewayConfig) { c.Authentication.Authority = nil }},
		{"missing-binding", func(c *GatewayConfig) { c.Tenants[0].Policy.Access.KeyAuthority = nil }},
		{"membership", func(c *GatewayConfig) { c.Authentication.Authority.Gateways = []string{"east"} }},
		{"control-role", func(c *GatewayConfig) { c.Authentication.Authority.ReplicaID = "control" }},
	} {
		t.Run(change.name, func(t *testing.T) {
			cfg := gatewayAuthorityBindingFixture(t)
			cfg.MaxHTTPRequests = 1
			change.mutate(&cfg)
			g, err := NewGateway(cfg, nil)
			if g != nil || !errors.Is(err, errGatewayAuthConfig) {
				if g != nil {
					_ = g.Close()
				}
				t.Fatal("gateway bypassed authority binding validation")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := InitializeGatewayAuthState(ctx, cfg); !errors.Is(err, errGatewayAuthConfig) {
				t.Fatal("initializer bypassed authority binding validation")
			}
		})
	}
}
