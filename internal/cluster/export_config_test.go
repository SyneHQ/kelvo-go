// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
)

func runtimeExportConfigFixture(t *testing.T) NodeConfig {
	t.Helper()
	p := principalTestPolicy()
	p.Exports = &ExportPolicy{AuthorizationVersion: "v1", MaxJobs: 8, QueueTimeout: 30 * time.Second,
		DefaultTTL: 10 * time.Minute, MaxTTL: time.Hour,
		Limits: exports.Limits{MaxRows: 1000, MaxEncodedBytes: 8 << 20, MaxDecodedBytes: 8 << 20,
			MaxPartBytes: 1 << 20, MaxPartDecodedBytes: 1 << 20, MaxParts: 8}}
	root := t.TempDir()
	return NodeConfig{Policy: p, SandboxPath: "/private/launcher", ScratchDirectory: filepath.Join(root, "scratch"),
		Resources: &ResourceConfig{MaxConcurrent: 4, MemoryMB: 2048, BaselineMB: 128, OverheadMB: 64,
			ScratchMB: 4096, QueryReserveSlots: 1, QueryReserveMemoryMB: 512, QueryReserveScratchMB: 1024,
			Export: &ResourceClassConfig{MaxConcurrent: 2, MemoryMB: 768, ScratchMB: 2048}},
		Exports: &ExportNodeConfig{Directory: filepath.Join(root, "exports"), MaxEntries: 32, MaxStoredBytes: 64 << 20,
			MaxConcurrent: 1, MaxDownloads: 2, DownloadMemoryMB: 32, CleanupInterval: time.Minute, CleanupMaxRemovals: 16}}
}

func TestExportConfigurationRequiresAuthorityAndBoundedResources(t *testing.T) {
	for name, change := range map[string]func(*NodeConfig){
		"missing principal":   func(c *NodeConfig) { c.Policy.Access = nil },
		"missing version":     func(c *NodeConfig) { c.Policy.Exports.AuthorizationVersion = "" },
		"invalid version":     func(c *NodeConfig) { c.Policy.Exports.AuthorizationVersion = "two words" },
		"expired default":     func(c *NodeConfig) { c.Policy.Exports.DefaultTTL = time.Second },
		"retention":           func(c *NodeConfig) { c.Policy.Exports.MaxTTL = 31 * 24 * time.Hour },
		"query overflow":      func(c *NodeConfig) { c.Policy.Limits.Timeout = time.Duration(math.MaxInt64) },
		"decoded output":      func(c *NodeConfig) { c.Policy.Exports.Limits.MaxDecodedBytes = c.Policy.Limits.MaxBytes + 1 },
		"codec":               func(c *NodeConfig) { c.Policy.Exports.Limits.Compression = "gzip" },
		"missing capacity":    func(c *NodeConfig) { c.Resources = nil },
		"missing class":       func(c *NodeConfig) { c.Resources.Export = nil },
		"class memory":        func(c *NodeConfig) { c.Resources.Export.MemoryMB = 1 },
		"class overflow":      func(c *NodeConfig) { c.Resources.Export.MemoryMB = math.MaxInt64 },
		"verification memory": func(c *NodeConfig) { c.Exports.DownloadMemoryMB = 1 },
		"writer memory":       func(c *NodeConfig) { c.Resources.OverheadMB = 1 },
		"download saturation": func(c *NodeConfig) { c.Exports.MaxDownloads = 65 },
		"unbounded cleanup":   func(c *NodeConfig) { c.Exports.CleanupMaxRemovals = 0 },
		"relative root":       func(c *NodeConfig) { c.Exports.Directory = "exports" },
		"scratch child":       func(c *NodeConfig) { c.Exports.Directory = filepath.Join(c.ScratchDirectory, "exports") },
		"scratch parent":      func(c *NodeConfig) { c.Exports.Directory = filepath.Dir(c.ScratchDirectory) },
	} {
		t.Run(name, func(t *testing.T) {
			config := runtimeExportConfigFixture(t)
			if err := validateNodeExports(config); err != nil {
				t.Fatal("valid fixture", err)
			}
			change(&config)
			if err := validateNodeExports(config); err == nil {
				t.Fatal("unsafe export configuration accepted")
			}
		})
	}
}

func TestExportResourceClassSharesGlobalAndProtectedCapacity(t *testing.T) {
	config := runtimeExportConfigFixture(t)
	pool, err := config.Resources.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	request := admission.Request{Class: admission.ClassExport, MemoryBytes: 384 << 20, ScratchBytes: 1024 << 20}
	a, err := pool.Acquire(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	b, err := pool.Acquire(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := pool.Acquire(ctx, request); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("export ceiling bypassed", err)
	}
	interactive, err := pool.Acquire(context.Background(), admission.Request{Class: admission.ClassInteractive, MemoryBytes: 512 << 20, ScratchBytes: 1024 << 20})
	if err != nil {
		t.Fatal("export consumed interactive reserve", err)
	}
	interactive.Release()
	if state := pool.Snapshot(); state.Active != 2 || state.Classes[admission.ClassExport].Active != 2 {
		t.Fatal("class reservation accounting changed")
	}
}

func TestGatewayExportConfigurationDefaultsOffAndRequiresFilePrincipals(t *testing.T) {
	if err := validateGatewayExports(GatewayConfig{}); err != nil {
		t.Fatal(err)
	}
	node := runtimeExportConfigFixture(t)
	config := GatewayConfig{Tenants: []TenantConfig{{Policy: node.Policy}}, Exports: &GatewayExportConfig{MaxSupervisors: 2, MaxDownloads: 4}}
	if err := validateGatewayExports(config); err == nil {
		t.Fatal("legacy token auth enabled durable exports")
	}
	config.Authentication = &GatewayAuthenticationConfig{KeysFile: "/private/keys.yml"}
	if err := validateGatewayExports(config); err != nil {
		t.Fatal(err)
	}
	config.Exports.MaxSupervisors = 0
	if err := validateGatewayExports(config); err == nil {
		t.Fatal("unbounded supervision enabled")
	}
}
