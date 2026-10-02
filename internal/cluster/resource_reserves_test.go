// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestResourceQueryReservesValidateAgainstUsableCapacity(t *testing.T) {
	for name, change := range map[string]func(*ResourceConfig){
		"negative slots":           func(c *ResourceConfig) { c.QueryReserveSlots = -1 },
		"all slots":                func(c *ResourceConfig) { c.QueryReserveSlots = c.MaxConcurrent },
		"excess slots":             func(c *ResourceConfig) { c.QueryReserveSlots = c.MaxConcurrent + 1 },
		"negative memory":          func(c *ResourceConfig) { c.QueryReserveMemoryMB = -1 },
		"memory excludes baseline": func(c *ResourceConfig) { c.QueryReserveMemoryMB = c.MemoryMB - c.BaselineMB + 1 },
		"memory shift overflow":    func(c *ResourceConfig) { c.QueryReserveMemoryMB = math.MaxInt64 },
		"memory cap":               func(c *ResourceConfig) { c.QueryReserveMemoryMB = (1 << 30) + 1 },
		"negative scratch":         func(c *ResourceConfig) { c.QueryReserveScratchMB = -1 },
		"excess scratch":           func(c *ResourceConfig) { c.QueryReserveScratchMB = c.ScratchMB + 1 },
		"scratch shift overflow":   func(c *ResourceConfig) { c.QueryReserveScratchMB = math.MaxInt64 },
		"scratch cap":              func(c *ResourceConfig) { c.QueryReserveScratchMB = (1 << 30) + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			c := validResources()
			change(&c)
			if _, err := c.NewPool(); err == nil {
				t.Fatal("invalid query reserve accepted")
			}
			if c.Fits(query.DefaultLimits(), false) || c.Fits(query.DefaultLimits(), true) {
				t.Fatal("invalid reserve configuration reported fitting")
			}
		})
	}
}

func TestResourceQueryReserveMappingAndBoundaryBudgets(t *testing.T) {
	c := validResources()
	c.QueryReserveSlots = 1
	c.QueryReserveMemoryMB = 128
	c.QueryReserveScratchMB = 256
	p, err := c.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	limits := p.Snapshot().Limits
	if limits.ReservedSlots != 1 || limits.ReservedMemoryBytes != 128<<20 || limits.ReservedScratchBytes != 256<<20 {
		t.Fatalf("reserve conversion changed: %+v", limits)
	}
	for _, allMemory := range []bool{false, true} {
		whole := validResources()
		if allMemory {
			whole.QueryReserveMemoryMB = whole.MemoryMB - whole.BaselineMB
		} else {
			whole.QueryReserveScratchMB = whole.ScratchMB
		}
		if _, err := whole.NewPool(); err != nil {
			t.Fatal("full memory/scratch reserve should permit query-only capacity", err)
		}
		if whole.Fits(query.DefaultLimits(), true) {
			t.Fatal("refresh fit into fully protected capacity")
		}
		if !whole.Fits(query.DefaultLimits(), false) {
			t.Fatal("interactive query lost access to its protected capacity")
		}
	}
}

func TestResourceRefreshFitsIncludesReservesOverheadAndStaging(t *testing.T) {
	c := ResourceConfig{MaxConcurrent: 4, MemoryMB: 1024, BaselineMB: 128, OverheadMB: 64, ScratchMB: 2048, QueryReserveSlots: 3, QueryReserveMemoryMB: 576, QueryReserveScratchMB: 768}
	l := query.DefaultLimits()
	// Background memory: 1024-128-576=320 MiB; query256+overhead64.
	// Background scratch: 2048-768=1280 MiB; spill1024+staging256.
	if !c.Fits(l, true) {
		t.Fatal("exact refresh boundaries rejected")
	}
	memory := c
	memory.QueryReserveMemoryMB++
	if memory.Fits(l, true) {
		t.Fatal("refresh ignored protected memory")
	}
	scratch := c
	scratch.QueryReserveScratchMB++
	if scratch.Fits(l, true) {
		t.Fatal("refresh ignored protected scratch")
	}
	bytes := l
	bytes.MaxBytes++
	if c.Fits(bytes, true) {
		t.Fatal("refresh omitted final staging byte")
	}
	overhead := c
	overhead.OverheadMB++
	if overhead.Fits(l, true) {
		t.Fatal("refresh omitted worker overhead")
	}
	interactive := l
	interactive.MemoryMB = 512
	interactive.MaxTempMB = 2048
	if !c.Fits(interactive, false) {
		t.Fatal("interactive query was incorrectly limited to background capacity")
	}
	if c.Fits(interactive, true) {
		t.Fatal("oversized background query fit")
	}
}

func TestLoadNodeQueryReservesYAMLAndInvalidBoundaries(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sandbox"), []byte("fixture"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "node.yml")
	resources := "resources: {max_concurrent: 2, memory_mb: 512, baseline_mb: 64, overhead_mb: 64, scratch_mb: 1024, query_reserve_slots: 1, query_reserve_memory_mb: 128, query_reserve_scratch_mb: 256}\n"
	for name, test := range map[string]struct {
		text  string
		valid bool
	}{
		"configured":      {resourceNodeYAML + resources, true},
		"zero opt in":     {resourceNodeYAML + strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(resources, "query_reserve_slots: 1", "query_reserve_slots: 0"), "query_reserve_memory_mb: 128", "query_reserve_memory_mb: 0"), "query_reserve_scratch_mb: 256", "query_reserve_scratch_mb: 0"), true},
		"slots exceed":    {resourceNodeYAML + strings.Replace(resources, "query_reserve_slots: 1", "query_reserve_slots: 2", 1), false},
		"memory baseline": {resourceNodeYAML + strings.Replace(resources, "query_reserve_memory_mb: 128", "query_reserve_memory_mb: 449", 1), false},
		"scratch exceeds": {resourceNodeYAML + strings.Replace(resources, "query_reserve_scratch_mb: 256", "query_reserve_scratch_mb: 1025", 1), false},
		"negative memory": {resourceNodeYAML + strings.Replace(resources, "query_reserve_memory_mb: 128", "query_reserve_memory_mb: -1", 1), false},
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(test.text), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadNode(path)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v error=%v", test.valid, err)
			}
			if name == "configured" && err == nil && (cfg.Resources.QueryReserveSlots != 1 || cfg.Resources.QueryReserveMemoryMB != 128 || cfg.Resources.QueryReserveScratchMB != 256) {
				t.Fatal("YAML reserves lost")
			}
		})
	}
}
