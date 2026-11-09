// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func TestValidateSheetsCache(t *testing.T) {
	config := func(cache *worker.SheetsCacheConfig) NodeConfig {
		c := NodeConfig{ScratchDirectory: "/var/lib/kelvo/scratch", Operations: &OperationNodeConfig{SheetsCache: cache}}
		c.Operations.Results.Directory = "/var/lib/kelvo/results"
		c.Audit = &ServiceAuditConfig{}
		c.Audit.Directory = "/var/lib/kelvo/audit"
		c.Containment = &ContainmentConfig{Config: containment.Config{StateDirectory: "/var/lib/kelvo/containment"}}
		return c
	}
	if err := validateSheetsCache(config(nil)); err != nil {
		t.Fatalf("absent cache must be off, got %v", err)
	}
	if err := validateSheetsCache(config(&worker.SheetsCacheConfig{Directory: "/var/lib/kelvo/sheets-cache", MaxMB: 4096})); err != nil {
		t.Fatalf("valid cache rejected: %v", err)
	}
	for name, cache := range map[string]worker.SheetsCacheConfig{
		"scratch":     {Directory: "/var/lib/kelvo/scratch", MaxMB: 4096},
		"in scratch":  {Directory: "/var/lib/kelvo/scratch/sheets", MaxMB: 4096},
		"parent":      {Directory: "/var/lib/kelvo", MaxMB: 4096},
		"results":     {Directory: "/var/lib/kelvo/results/c", MaxMB: 4096},
		"audit":       {Directory: "/var/lib/kelvo/audit", MaxMB: 4096},
		"containment": {Directory: "/var/lib/kelvo/containment/x", MaxMB: 4096},
		"relative":    {Directory: "sheets", MaxMB: 4096},
		"unclean":     {Directory: "/var/lib/kelvo/sheets/", MaxMB: 4096},
		"small":       {Directory: "/var/lib/kelvo/sheets", MaxMB: 63},
		"large":       {Directory: "/var/lib/kelvo/sheets", MaxMB: 1<<20 + 1},
	} {
		if validateSheetsCache(config(&cache)) == nil {
			t.Errorf("%s: accepted %+v", name, cache)
		}
	}
}
