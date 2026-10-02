// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"errors"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// ResourceConfig reserves configured budgets; it is not a process RSS cap.
// MemoryMB includes baseline headroom. OverheadMB covers allocations outside
// DuckDB for each job. Operators must size both using workload measurements.
type ResourceConfig struct {
	MaxConcurrent int   `yaml:"max_concurrent"`
	MemoryMB      int64 `yaml:"memory_mb"`
	BaselineMB    int64 `yaml:"baseline_mb"`
	OverheadMB    int64 `yaml:"overhead_mb"`
	ScratchMB     int64 `yaml:"scratch_mb"`
}

func (c ResourceConfig) NewPool() (*admission.Pool, error) {
	if c.MemoryMB <= 0 || c.MemoryMB > 1<<30 || c.BaselineMB <= 0 || c.BaselineMB >= c.MemoryMB || c.OverheadMB <= 0 || c.OverheadMB > c.MemoryMB-c.BaselineMB || c.ScratchMB < 1 || c.ScratchMB > 1<<30 || c.MaxConcurrent < 1 || c.MaxConcurrent > 64 {
		return nil, errors.New("invalid node resource budgets")
	}
	return admission.New(admission.Limits{MaxConcurrent: c.MaxConcurrent, MemoryBytes: (c.MemoryMB - c.BaselineMB) << 20, ScratchBytes: c.ScratchMB << 20})
}

// Fits fails closed for invalid limits. Node startup checks every dataset in
// its catalog, including manual refreshes, so any configured work fits alone.
func (c ResourceConfig) Fits(l query.Limits, refresh bool) bool {
	if err := l.Validate(); err != nil {
		return false
	}
	if _, err := c.NewPool(); err != nil {
		return false
	}
	scratch := int64(l.MaxTempMB) << 20
	if refresh {
		scratch += l.MaxBytes
	}
	return (int64(l.MemoryMB)+c.OverheadMB)<<20 <= (c.MemoryMB-c.BaselineMB)<<20 && scratch <= c.ScratchMB<<20
}
