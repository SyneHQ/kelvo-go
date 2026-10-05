//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"testing"
)

func TestKeyAuthorityBindingRetainedByNodeAndExportRuntime(t *testing.T) {
	t.Run("node", func(t *testing.T) {
		p := gatewayAuthorityBindingFixture(t).Tenants[0].Policy
		stored, err := clonePolicy(p)
		if err != nil {
			t.Fatal(err)
		}
		store := &nodeTestStore{p: stored, jobs: map[string]Snapshot{}, queue: make(chan Delivery)}
		node, err := newNode(NodeConfig{Policy: p, WorkerID: "a1"}, store, probeExecutor{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = node.Close() })
		want := policyBytes(t, p)
		p.Access.KeyAuthority.Scope = "changed"
		if !bytes.Equal(policyBytes(t, node.cfg.Policy), want) {
			t.Fatal("node retained mutable key authority binding")
		}
	})
	t.Run("export", func(t *testing.T) {
		cfg := runtimeExportConfigFixture(t)
		cfg.WorkerID = "a1"
		cfg.Policy.Access = gatewayAuthorityBindingFixture(t).Tenants[0].Policy.Access
		stored, err := clonePolicy(cfg.Policy)
		if err != nil {
			t.Fatal(err)
		}
		store := &exportRuntimeStore{p: stored, jobs: map[string]ExportSnapshot{}, queue: make(chan Delivery)}
		pool, err := cfg.Resources.NewPool()
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := newExportRuntime(cfg, store, runtimeExportExecutor(runtimeExportRows), pool, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := runtime.Close(); err != nil {
				t.Error(err)
			}
		})
		want := policyBytes(t, cfg.Policy)
		cfg.Policy.Access.KeyAuthority.Scope = "changed"
		if !bytes.Equal(policyBytes(t, runtime.cfg.Policy), want) {
			t.Fatal("export runtime retained mutable key authority binding")
		}
	})
}
