// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"testing"
)

func TestContainmentRequiresNativeAndParentReservation(t *testing.T) {
	limits := query.DefaultLimits()
	limits.MemoryMB = 128
	c := ContainmentConfig{Config: containment.Config{Root: "/sys/fs/cgroup/owned/jobs", StateDirectory: "/private/containment"}, Budget: containment.Budget{NativeOverheadMB: 64, ParentOverheadMB: 32, MaxProcesses: 64}}
	resources := &ResourceConfig{MaxConcurrent: 2, MemoryMB: 1024, BaselineMB: 64, OverheadMB: 224, ScratchMB: 1024}
	if err := c.Validate(resources, limits); err != nil {
		t.Fatal(err)
	}
	resources.OverheadMB = 223
	if c.Validate(resources, limits) == nil {
		t.Fatal("parent IPC budget not reserved")
	}
	resources.OverheadMB = 224
	limits.MemoryMB = 256
	if c.Validate(resources, limits) == nil {
		t.Fatal("larger refresh budget not revalidated")
	}
	if c.Validate(nil, limits) == nil {
		t.Fatal("missing node resources accepted")
	}
	limits.MemoryMB = 128
	process, err := c.Budget.ProcessLimits(int64(limits.MemoryMB), 2)
	if err != nil || process.MemoryBytes != 192<<20 || process.CPUQuotaMicros != 200000 || process.CPUPeriodMicros != 100000 {
		t.Fatal("incorrect native cap", process, err)
	}
}
