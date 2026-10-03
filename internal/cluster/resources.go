// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"errors"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// ResourceConfig reserves configured budgets; it is not a process RSS cap.
// MemoryMB includes baseline headroom. OverheadMB covers allocations outside
// DuckDB for each job. Operators must size both using workload measurements.
type ResourceConfig struct {
	Export                *ResourceClassConfig `yaml:"export,omitempty"`
	QueryReserveSlots     int                  `yaml:"query_reserve_slots,omitempty"`
	QueryReserveMemoryMB  int64                `yaml:"query_reserve_memory_mb,omitempty"`
	QueryReserveScratchMB int64                `yaml:"query_reserve_scratch_mb,omitempty"`

	MaxConcurrent int   `yaml:"max_concurrent"`
	MemoryMB      int64 `yaml:"memory_mb"`
	BaselineMB    int64 `yaml:"baseline_mb"`
	OverheadMB    int64 `yaml:"overhead_mb"`
	ScratchMB     int64 `yaml:"scratch_mb"`
}

func (c ResourceConfig) NewPool() (*admission.Pool, error) {
	if c.QueryReserveMemoryMB < 0 || c.QueryReserveMemoryMB > 1<<30 || c.QueryReserveScratchMB < 0 || c.QueryReserveScratchMB > 1<<30 || c.MemoryMB <= 0 || c.MemoryMB > 1<<30 || c.BaselineMB <= 0 || c.BaselineMB >= c.MemoryMB || c.OverheadMB <= 0 || c.OverheadMB > c.MemoryMB-c.BaselineMB || c.ScratchMB < 1 || c.ScratchMB > 1<<30 || c.MaxConcurrent < 1 || c.MaxConcurrent > 64 {
		return nil, errors.New("invalid node resource budgets")
	}
	limits := admission.Limits{ReservedSlots: c.QueryReserveSlots, ReservedMemoryBytes: c.QueryReserveMemoryMB << 20, ReservedScratchBytes: c.QueryReserveScratchMB << 20, MaxConcurrent: c.MaxConcurrent, MemoryBytes: (c.MemoryMB - c.BaselineMB) << 20, ScratchBytes: c.ScratchMB << 20}
	if c.Export != nil {
		if c.Export.MaxConcurrent < 1 || c.Export.MaxConcurrent > 64 || c.Export.MemoryMB < 1 || c.Export.MemoryMB > 1<<30 || c.Export.ScratchMB < 0 || c.Export.ScratchMB > 1<<30 {
			return nil, errors.New("invalid export resource budgets")
		}
		limits.Classes = map[admission.Class]admission.ClassLimits{admission.ClassExport: {
			MaxConcurrent: c.Export.MaxConcurrent, MemoryBytes: c.Export.MemoryMB << 20, ScratchBytes: c.Export.ScratchMB << 20,
		}}
	}
	return admission.New(limits)
}

// ResourceClassConfig narrows background work within the shared node capacity.
// It neither adds capacity nor bypasses protected interactive reservations.
type ResourceClassConfig struct {
	MaxConcurrent int   `yaml:"max_concurrent"`
	MemoryMB      int64 `yaml:"memory_mb"`
	ScratchMB     int64 `yaml:"scratch_mb"`
}

func (c ResourceConfig) FitsExport(l query.Limits) bool {
	if l.Validate() != nil || c.Export == nil {
		return false
	}
	if _, err := c.NewPool(); err != nil {
		return false
	}
	memory := int64(l.MemoryMB) + c.OverheadMB
	return c.Export.MaxConcurrent <= c.MaxConcurrent-c.QueryReserveSlots &&
		memory <= c.Export.MemoryMB && memory <= c.MemoryMB-c.BaselineMB-c.QueryReserveMemoryMB &&
		int64(l.MaxTempMB) <= c.Export.ScratchMB && int64(l.MaxTempMB) <= c.ScratchMB-c.QueryReserveScratchMB
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
	memoryCapacity := (c.MemoryMB - c.BaselineMB) << 20
	scratchCapacity := c.ScratchMB << 20
	if refresh {
		memoryCapacity -= c.QueryReserveMemoryMB << 20
		scratchCapacity -= c.QueryReserveScratchMB << 20
	}
	return (int64(l.MemoryMB)+c.OverheadMB)<<20 <= memoryCapacity && scratch <= scratchCapacity
}

// ContainmentConfig is opt-in kernel enforcement for disposable native workers.
// Config and Budget are immutable shared configuration; custody is per request.
type ContainmentConfig struct {
	containment.Config `yaml:",inline"`
	containment.Budget `yaml:",inline"`
}

func (c ContainmentConfig) Validate(resources *ResourceConfig, limits query.Limits) error {
	if c.Config.Validate() != nil || c.Budget.Validate() != nil || resources == nil || !c.Budget.FitsOverhead(int64(limits.MemoryMB), resources.OverheadMB) {
		return errors.New("containment requires explicit delegation and separate native and parent memory reservations")
	}
	if c.MaxGroups != 0 && c.MaxGroups < resources.MaxConcurrent {
		return errors.New("containment max_groups is smaller than node concurrency")
	}
	return nil
}
