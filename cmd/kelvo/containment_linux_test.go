//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"os"
	"testing"
)

func TestContainedNodeStartupPlacement(t *testing.T) {
	root, state, binary := os.Getenv("KELVO_TEST_CGROUP_ROOT"), os.Getenv("KELVO_TEST_CGROUP_STATE"), os.Getenv("KELVO_TEST_BINARY")
	if root == "" || state == "" || binary == "" {
		t.Skip("explicit disposable delegation and built binary required")
	}
	manager, err := containment.Open(containment.Config{Root: root, StateDirectory: state})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := manager.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 256 << 20})
	if err != nil {
		t.Fatal(err)
	}
	limits := query.DefaultLimits()
	limits.MemoryMB = 64
	limits.Threads = 1
	executor := &worker.Executor{Binary: binary, Limits: limits, Containment: manager, ContainmentBudget: containment.Budget{NativeOverheadMB: 64, ParentOverheadMB: 32, MaxProcesses: 64}, ResourcePool: pool, ResourceOverheadBytes: 160 << 20}
	err = verifyContainmentStartup(context.Background(), executor)
	if os.Getenv("KELVO_TEST_EXPECT_PLACEMENT_REJECTION") == "yes" {
		if err == nil {
			t.Fatal("unusable common-ancestor placement passed startup")
		}
	} else if err != nil {
		t.Fatal("valid delegated startup rejected", err)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("startup probe retained clean ownership")
	}
}
