//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func TestExportWorkerConstructorBindsKernelContainment(t *testing.T) {
	root, state := os.Getenv("KELVO_TEST_CGROUP_ROOT"), os.Getenv("KELVO_TEST_CGROUP_STATE")
	if root == "" || state == "" {
		t.Skip("explicit disposable cgroup delegation required")
	}
	if os.Geteuid() == 0 {
		t.Fatal("containment binding acceptance requires an unprivileged manager")
	}
	configured := ContainmentConfig{Config: containment.Config{Root: root, StateDirectory: state},
		Budget: containment.Budget{NativeOverheadMB: 16, ParentOverheadMB: 16, MaxProcesses: 32}}
	manager, err := containment.Open(configured.Config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	for _, kind := range []string{"valid", "explicit_defaults", "root", "state", "groups", "cleanup", "budget", "insufficient_overhead", "closed"} {
		t.Run(kind, func(t *testing.T) {
			cfg := runtimeExportConfigFixture(t)
			cfg.WorkerID = "a1"
			copyConfig := configured
			cfg.Containment = &copyConfig
			cfg.Resources.OverheadMB = int64(cfg.Policy.Limits.MemoryMB) + 32
			if err := os.Mkdir(cfg.ScratchDirectory, 0700); err != nil {
				t.Fatal(err)
			}
			scratch, err := worker.OpenScratchRoot(cfg.ScratchDirectory)
			if err != nil {
				t.Fatal(err)
			}
			defer scratch.Close()
			executor := &worker.Executor{SandboxPath: cfg.SandboxPath, ScratchRoot: scratch,
				Containment: manager, ContainmentBudget: configured.Budget}
			switch kind {
			case "explicit_defaults":
				cfg.Containment.MaxGroups = 128
				cfg.Containment.CleanupTimeout = 5 * time.Second
			case "root":
				cfg.Containment.Root = filepath.Join(root, "other")
			case "state":
				cfg.Containment.StateDirectory = filepath.Join(state, "other")
			case "groups":
				cfg.Containment.MaxGroups = 64
			case "cleanup":
				cfg.Containment.CleanupTimeout = 3 * time.Second
			case "budget":
				executor.ContainmentBudget.MaxProcesses = 64
			case "insufficient_overhead":
				cfg.Resources.OverheadMB--
			case "closed":
				if err := manager.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			executor.ResourcePool, err = cfg.Resources.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			store := &exportRuntimeStore{p: cfg.Policy, jobs: make(map[string]ExportSnapshot), queue: make(chan Delivery, 16)}
			r, err := NewExportRuntime(cfg, store, executor, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
			if kind != "valid" && kind != "explicit_defaults" {
				if err == nil {
					r.Close()
					t.Fatal("mismatched or unavailable containment accepted")
				}
				if _, statErr := os.Stat(cfg.Exports.Directory); !os.IsNotExist(statErr) {
					t.Fatal("rejected configuration opened export storage", statErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if r.executor.(*worker.Executor).Containment != manager || r.executor.(*worker.Executor).ContainmentBudget != configured.Budget {
				t.Fatal("export executor lost configured containment")
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if !manager.MatchesConfig(configured.Config) {
				t.Fatal("closing export runtime closed shared containment")
			}
		})
	}
}
