//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
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
