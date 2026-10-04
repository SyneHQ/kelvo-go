//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"os"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func TestProtectedNodeRefusesMissingRuntimeBeforeBrokerAdmission(t *testing.T) {
	c := catalog.Config{Acceleration: &catalog.AccelerationConfig{TenantID: "tenant-a", ObjectStorage: &catalog.ObjectStorage{ReaderRegistry: &catalog.ObjectReaderRegistry{}}}}
	executor := &worker.Executor{Config: c, SandboxPath: "/unused-fixture-launcher"}
	node, err := NewNode(NodeConfig{Policy: Policy{TenantID: "tenant-a"}}, nil, executor)
	if node != nil || err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" {
		t.Fatal("protected node reached broker admission without its contained runtime", err)
	}
}

func TestProtectedExportRefusesMissingRuntimeBeforeStorage(t *testing.T) {
	cfg := runtimeExportConfigFixture(t)
	cfg.WorkerID = "a1"
	pool, err := cfg.Resources.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	executor := &worker.Executor{SandboxPath: cfg.SandboxPath, ResourcePool: pool,
		Config: catalog.Config{Acceleration: &catalog.AccelerationConfig{TenantID: cfg.Policy.TenantID,
			ObjectStorage: &catalog.ObjectStorage{ReaderRegistry: &catalog.ObjectReaderRegistry{}}}}}
	store := &exportRuntimeStore{p: cfg.Policy, jobs: make(map[string]ExportSnapshot), queue: make(chan Delivery, 16)}
	runtime, err := NewExportRuntime(cfg, store, executor, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if runtime != nil || err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" ||
		query.PublicError(err).Message != "Protected snapshots require a contained node with managed scratch and an object runtime" {
		t.Fatal("protected export reached storage without its contained runtime", err)
	}
	if _, err := os.Stat(cfg.Exports.Directory); !os.IsNotExist(err) {
		t.Fatal("rejected protected export opened local storage", err)
	}
}
