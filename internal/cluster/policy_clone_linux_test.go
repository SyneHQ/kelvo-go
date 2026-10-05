//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"testing"
)

func TestExportRuntimeOwnsPolicyAfterConstruction(t *testing.T) {
	cfg := runtimeExportConfigFixture(t)
	cfg.WorkerID = "a1"
	cfg.Policy = policyIsolationFixture(t)
	store := &exportRuntimeStore{p: policyIsolationFixture(t), jobs: map[string]ExportSnapshot{}, queue: make(chan Delivery)}
	pool, err := cfg.Resources.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	want := policyBytes(t, cfg.Policy)
	runtime, err := newExportRuntime(cfg, store, runtimeExportExecutor(runtimeExportRows), pool, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Error(err)
		}
	})
	mutatePolicyGraph(cfg.Policy)
	if !bytes.Equal(policyBytes(t, runtime.cfg.Policy), want) {
		t.Fatal("export runtime retained caller policy aliases")
	}
}
