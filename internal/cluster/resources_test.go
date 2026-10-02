// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func validResources() ResourceConfig {
	return ResourceConfig{MaxConcurrent: 2, MemoryMB: 1024, BaselineMB: 128, OverheadMB: 64, ScratchMB: 2048}
}

func TestResourceBudgetsRejectInvalidConfiguration(t *testing.T) {
	for name, mutate := range map[string]func(*ResourceConfig){
		"zero slots":                         func(c *ResourceConfig) { c.MaxConcurrent = 0 },
		"too many slots":                     func(c *ResourceConfig) { c.MaxConcurrent = 65 },
		"zero memory":                        func(c *ResourceConfig) { c.MemoryMB = 0 },
		"memory overflow":                    func(c *ResourceConfig) { c.MemoryMB = math.MaxInt64 },
		"memory cap":                         func(c *ResourceConfig) { c.MemoryMB = (1 << 30) + 1 },
		"missing baseline":                   func(c *ResourceConfig) { c.BaselineMB = 0 },
		"negative baseline":                  func(c *ResourceConfig) { c.BaselineMB = -1 },
		"baseline consumes memory":           func(c *ResourceConfig) { c.BaselineMB = c.MemoryMB },
		"missing overhead":                   func(c *ResourceConfig) { c.OverheadMB = 0 },
		"overhead consumes more than usable": func(c *ResourceConfig) { c.OverheadMB = c.MemoryMB - c.BaselineMB + 1 },
		"zero scratch":                       func(c *ResourceConfig) { c.ScratchMB = 0 },
		"negative scratch":                   func(c *ResourceConfig) { c.ScratchMB = -1 },
		"scratch cap":                        func(c *ResourceConfig) { c.ScratchMB = (1 << 30) + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			c := validResources()
			mutate(&c)
			if _, err := c.NewPool(); err == nil {
				t.Fatal("invalid resources accepted")
			}
		})
	}
}

func TestResourcePoolSubtractsBaselineOnceAndReservesPerJobOverhead(t *testing.T) {
	c := ResourceConfig{MaxConcurrent: 2, MemoryMB: 768, BaselineMB: 128, OverheadMB: 64, ScratchMB: 2048}
	p, err := c.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	if s := p.Snapshot(); s.Limits.MemoryBytes != 640<<20 || s.Limits.ScratchBytes != 2048<<20 || s.Limits.MaxConcurrent != 2 {
		t.Fatalf("wrong usable capacity: %+v", s)
	}
	l := query.DefaultLimits()
	if !c.Fits(l, false) {
		t.Fatal("fitting query rejected")
	}
	request := admission.Request{MemoryBytes: (int64(l.MemoryMB) + c.OverheadMB) << 20, ScratchBytes: int64(l.MaxTempMB) << 20}
	first, err := p.TryAcquire(request)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := p.TryAcquire(request)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if _, err = p.TryAcquire(request); !errors.Is(err, admission.ErrBusy) {
		t.Fatalf("oversubscribed: %v", err)
	}
}

func TestResourceFitsAccountsForRefreshStagingAndExactBoundaries(t *testing.T) {
	l := query.DefaultLimits()
	c := ResourceConfig{MaxConcurrent: 1, MemoryMB: 384, BaselineMB: 64, OverheadMB: 64, ScratchMB: 1024}
	if !c.Fits(l, false) {
		t.Fatal("exact query memory/scratch boundary rejected")
	}
	if c.Fits(l, true) {
		t.Fatal("refresh staging excluded from scratch budget")
	}
	c.ScratchMB += l.MaxBytes >> 20
	if !c.Fits(l, true) {
		t.Fatal("exact refresh staging boundary rejected")
	}
	l.MaxBytes++
	if c.Fits(l, true) {
		t.Fatal("one-byte refresh overflow accepted")
	}
	l.MaxBytes--
	c.MemoryMB--
	if c.Fits(l, false) {
		t.Fatal("per-job overhead excluded from memory budget")
	}
}

func TestResourceMaximumConfigurationConvertsWithoutOverflow(t *testing.T) {
	c := ResourceConfig{MaxConcurrent: 64, MemoryMB: 1 << 30, BaselineMB: 1, OverheadMB: 1, ScratchMB: 1 << 30}
	p, err := c.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	if s := p.Snapshot(); s.Limits.MemoryBytes != ((1<<30)-1)<<20 || s.Limits.ScratchBytes != 1<<50 {
		t.Fatalf("capacity conversion overflow: %+v", s)
	}
	l := query.DefaultLimits()
	l.MemoryMB = 1048576
	l.MaxTempMB = 1048576
	l.MaxBytes = 1 << 40
	if err = l.Validate(); err != nil {
		t.Fatal(err)
	}
	if !c.Fits(l, true) {
		t.Fatal("maximum valid query unexpectedly overflowed")
	}
}

const resourceNodeYAML = `listen: 127.0.0.1:8444
worker_id: a1
catalog_file: kelvo.yml
sandbox_path: sandbox
tls: {cert_file: node.pem, key_file: node.key, ca_file: ca.pem}
nats: {url: "tls://localhost:4222", ca_file: ca.pem, username: worker, password_env: KELVO_NATS_PASSWORD}
policy:
  tenant_id: a
  max_queries: 8
  job_ttl: 60s
  lease_duration: 5s
  replicas: 3
  workers: {a1: 2}
  limits: {max_rows: 1000, max_bytes: 1048576, timeout: 30s, memory_mb: 256, threads: 1, max_temp_mb: 256}
`

func TestLoadNodeResourceBudgetsAreOptionalAndFailClosed(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sandbox"), []byte("test fixture"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "node.yml")
	resources := "resources: {max_concurrent: 2, memory_mb: 384, baseline_mb: 64, overhead_mb: 64, scratch_mb: 256}\n"
	for name, tc := range map[string]struct {
		text  string
		valid bool
	}{
		"legacy":                 {resourceNodeYAML, true},
		"exact":                  {resourceNodeYAML + resources, true},
		"memory too small":       {resourceNodeYAML + strings.Replace(resources, "memory_mb: 384", "memory_mb: 383", 1), false},
		"scratch too small":      {resourceNodeYAML + strings.Replace(resources, "scratch_mb: 256", "scratch_mb: 255", 1), false},
		"no baseline":            {resourceNodeYAML + strings.Replace(resources, "baseline_mb: 64", "baseline_mb: 0", 1), false},
		"unknown resource field": {resourceNodeYAML + strings.Replace(resources, "max_concurrent:", "unknown_budget:", 1), false},
		"invalid query memory":   {strings.Replace(resourceNodeYAML, "memory_mb: 256", "memory_mb: -1", 1) + resources, false},
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.text), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadNode(path)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
			if err == nil && name == "exact" && cfg.Resources == nil {
				t.Fatal("resource config lost")
			}
		})
	}
}

func TestResourceFitsRejectsInvalidDirectArguments(t *testing.T) {
	c := validResources()
	for name, mutate := range map[string]func(*query.Limits){
		"negative memory":  func(l *query.Limits) { l.MemoryMB = -1 },
		"negative scratch": func(l *query.Limits) { l.MaxTempMB = -1 },
		"oversized result": func(l *query.Limits) { l.MaxBytes = math.MaxInt64 },
		"zero timeout":     func(l *query.Limits) { l.Timeout = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			l := query.DefaultLimits()
			mutate(&l)
			if c.Fits(l, false) || c.Fits(l, true) {
				t.Fatal("invalid limits fit")
			}
		})
	}
	c.MemoryMB = math.MaxInt64
	if c.Fits(query.DefaultLimits(), true) {
		t.Fatal("invalid resource config fit")
	}
}
