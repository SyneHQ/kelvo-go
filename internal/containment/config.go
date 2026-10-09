// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"errors"
	"path/filepath"
	"time"
)

var (
	ErrUnsupported = errors.New("process containment requires delegated Linux cgroup v2")
	ErrInvalid     = errors.New("invalid process containment configuration")
	ErrUnavailable = errors.New("process containment is unavailable")
	ErrDraining    = errors.New("process containment is draining")
	ErrOwnership   = errors.New("process containment ownership could not be verified")
	ErrQuarantined = errors.New("process containment cleanup is uncertain; reservations retained")
)

// Config selects an existing, exclusively delegated cgroup-v2 directory and a
// separate private local state directory. The manager never discovers or adopts
// an ambient session/service cgroup and never falls back to unenforced execution.
type Config struct {
	Root           string        `yaml:"root"`
	StateDirectory string        `yaml:"state_directory"`
	MaxGroups      int           `yaml:"max_groups,omitempty"`
	CleanupTimeout time.Duration `yaml:"cleanup_timeout,omitempty"`
}

func (c Config) normalized() (Config, error) {
	for _, path := range []string{c.Root, c.StateDirectory} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || len(path) > 4096 {
			return c, ErrInvalid
		}
	}
	if c.Root == c.StateDirectory {
		return c, ErrInvalid
	}
	if c.MaxGroups == 0 {
		c.MaxGroups = 128
	}
	if c.CleanupTimeout == 0 {
		c.CleanupTimeout = 5 * time.Second
	}
	if c.MaxGroups < 1 || c.MaxGroups > 4096 || c.CleanupTimeout < 100*time.Millisecond || c.CleanupTimeout > 30*time.Second {
		return c, ErrInvalid
	}
	return c, nil
}

// Limits cap the contained native process tree. MemoryBytes must include native
// runtime/allocator overhead; caller-owned Arrow and IPC buffers remain outside
// this cap and require independent admission/host headroom. Swap is always zero.
type Limits struct {
	MemoryBytes     int64
	MaxProcesses    int64
	CPUQuotaMicros  int64
	CPUPeriodMicros int64
}

func (l Limits) validate() error {
	if l.MemoryBytes < 16<<20 || l.MemoryBytes > 1<<50 || l.MaxProcesses < 8 || l.MaxProcesses > 65536 ||
		l.CPUPeriodMicros < 1000 || l.CPUPeriodMicros > 1_000_000 || l.CPUQuotaMicros < 1000 ||
		l.CPUQuotaMicros > l.CPUPeriodMicros*1024 {
		return ErrInvalid
	}
	return nil
}

// Usage is charged cgroup memory and kernel event counters, not RSS, a logical
// reservation, whole-node memory or proof of a particular source-side outcome.
type Usage struct {
	// Scope is empty for a delegated per-operation group. Namespace execution
	// reports container_cgroup_lifetime, which must not be attributed to a query.
	Scope               string `json:"scope,omitempty"`
	MemoryCurrentBytes  uint64 `json:"memory_current_bytes"`
	MemoryPeakBytes     uint64 `json:"memory_peak_bytes"`
	OOMEvents           uint64 `json:"oom_events"`
	OOMKills            uint64 `json:"oom_kills"`
	PIDsLimitEvents     uint64 `json:"pids_limit_events"`
	CPUUsageMicros      uint64 `json:"cpu_usage_micros"`
	CPUThrottledPeriods uint64 `json:"cpu_throttled_periods"`
}

type Status struct {
	Active      int
	Quarantined int
	Draining    bool
	Closed      bool
}

// Validate checks syntax and bounds without opening operator-owned paths.
func (c Config) Validate() error { _, err := c.normalized(); return err }

// Budget separates the native child cap from the parent IPC allowance.
// It is admission accounting, not a claim that the parent RSS is bounded.
type Budget struct {
	NativeOverheadMB int64 `yaml:"native_overhead_mb"`
	ParentOverheadMB int64 `yaml:"parent_overhead_mb"`
	MaxProcesses     int64 `yaml:"max_processes"`
}

func (b Budget) Validate() error {
	if b.NativeOverheadMB < 16 || b.NativeOverheadMB > 65536 || b.ParentOverheadMB < 16 || b.ParentOverheadMB > 65536 || b.MaxProcesses < 8 || b.MaxProcesses > 65536 {
		return ErrInvalid
	}
	return nil
}
func (b Budget) ProcessLimits(memoryMB int64, threads int) (Limits, error) {
	if b.Validate() != nil || memoryMB < 16 || memoryMB > 1<<30 || threads < 1 || threads > 1024 {
		return Limits{}, ErrInvalid
	}
	limits := Limits{MemoryBytes: (memoryMB + b.NativeOverheadMB) << 20, MaxProcesses: b.MaxProcesses, CPUQuotaMicros: int64(threads) * 100000, CPUPeriodMicros: 100000}
	return limits, limits.validate()
}
func (b Budget) FitsOverhead(memoryMB, overheadMB int64) bool {
	return b.Validate() == nil && memoryMB >= 16 && memoryMB <= 1<<30 && overheadMB >= memoryMB+b.NativeOverheadMB+b.ParentOverheadMB
}
