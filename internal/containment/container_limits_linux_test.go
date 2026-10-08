//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func containerPolicy() Limits {
	return Limits{MemoryBytes: 256 << 20, MaxProcesses: 64, CPUQuotaMicros: 100000, CPUPeriodMicros: 100000}
}

func limitControls() map[string]string {
	return map[string]string{
		"cgroup.type": "domain\n", "memory.swap.max": "0\n", "memory.oom.group": "1\n",
		"cpu.max.burst": "0\n",
		"memory.max":    "268435456\n", "pids.max": "64\n", "cpu.max": "100000 100000\n",
	}
}

func controlReader(values map[string]string) func(string) (string, error) {
	return func(name string) (string, error) {
		value, ok := values[name]
		if !ok {
			return "", os.ErrNotExist
		}
		return value, nil
	}
}

func TestContainerVisibleUpperBounds(t *testing.T) {
	observed, err := containerUpperBounds(controlReader(limitControls()))
	if err != nil || observed != containerPolicy() || !limitsWithin(observed, containerPolicy()) {
		t.Fatal("valid visible upper bounds were rejected", observed, err)
	}
	// A stricter hidden ancestor can lower this bound. It cannot justify
	// accepting a visible limit that exceeds the requested policy.
	observed.MaxProcesses = 18999
	if limitsWithin(observed, containerPolicy()) {
		t.Fatal("finite task count exceeded policy")
	}
	observed = containerPolicy()
	observed.CPUQuotaMicros, observed.CPUPeriodMicros = 50000, 50000
	if !limitsWithin(observed, containerPolicy()) {
		t.Fatal("equivalent CPU ratio was rejected")
	}
	observed.CPUQuotaMicros++
	if limitsWithin(observed, containerPolicy()) {
		t.Fatal("larger CPU ratio passed through rounding")
	}
	observed = containerPolicy()
	observed.MemoryBytes++
	if limitsWithin(observed, containerPolicy()) {
		t.Fatal("larger memory bound was accepted")
	}
}

func TestContainerBoundsFailClosed(t *testing.T) {
	for _, test := range []struct{ name, field, value string }{
		{"unlimited_memory", "memory.max", "max\n"},
		{"unlimited_tasks", "pids.max", "max\n"},
		{"unlimited_cpu", "cpu.max", "max 100000\n"},
		{"burst_cpu", "cpu.max.burst", "1000\n"},
		{"swap_enabled", "memory.swap.max", "1\n"},
		{"swap_unlimited", "memory.swap.max", "max\n"},
		{"partial_oom", "memory.oom.group", "0\n"},
		{"threaded_group", "cgroup.type", "threaded\n"},
		{"memory_overflow", "memory.max", "9223372036854775808\n"},
		{"negative_tasks", "pids.max", "-1\n"},
		{"empty_tasks", "pids.max", ""},
		{"zero_period", "cpu.max", "100000 0\n"},
		{"extra_cpu_field", "cpu.max", "100000 100000 1\n"},
		{"cpu_overflow", "cpu.max", "9223372036854775807 100000\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			controls := limitControls()
			controls[test.field] = test.value
			if _, err := containerUpperBounds(controlReader(controls)); err == nil {
				t.Fatal("invalid kernel control was accepted")
			}
		})
	}
	for field := range limitControls() {
		controls := limitControls()
		delete(controls, field)
		if _, err := containerUpperBounds(controlReader(controls)); err == nil {
			t.Fatal("missing kernel control was accepted", field)
		}
	}
}

func TestContainerPIDMembership(t *testing.T) {
	for _, raw := range []string{"1\n", "1\n2\n", "2\n1\n"} {
		if !containsContainerPID(raw) {
			t.Fatal("PID 1 membership rejected")
		}
	}
	for _, raw := range []string{"", "2\n", "01x\n", "1\n0\n", "1\n4294967296\n"} {
		if containsContainerPID(raw) {
			t.Fatal("invalid or unrelated cgroup membership accepted")
		}
	}
}

func TestContainerKernelReadRejectsOrdinaryFile(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "memory.max"), []byte("268435456\n"), 0600); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	if _, err := containerKernelRead(int(parent.Fd()), "memory.max", cgroupMagic, 128); !errors.Is(err, ErrOwnership) {
		t.Fatal("ordinary file accepted as a cgroup control", err)
	}
	if err := os.Symlink("/proc/self/status", filepath.Join(directory, "status")); err != nil {
		t.Fatal(err)
	}
	if _, err := containerKernelRead(int(parent.Fd()), "status", unix.PROC_SUPER_MAGIC, controlLimit); err == nil {
		t.Fatal("symlink accepted as a procfs observation")
	}
}

const containerStatus = "Pid:\t1\nNSpid:\t1\nUid:\t65532 65532 65532 65532\nGid:\t65532 65532 65532 65532\nNoNewPrivs:\t1\nSeccomp:\t2\nCapInh:\t0000000000000000\nCapPrm:\t0000000000000000\nCapEff:\t0000000000000000\nCapBnd:\t0000000000000000\nCapAmb:\t0000000000000000\n"

func TestContainerCredentialsRejectPrivilegeAndNamespaceMismatch(t *testing.T) {
	if !containerCredentials(containerStatus, 65532, 65532) {
		t.Fatal("valid credentials were rejected")
	}
	for _, test := range []struct{ name, old, replacement string }{
		{"different_pid", "Pid:\t1\n", "Pid:\t2\n"},
		{"host_procfs", "NSpid:\t1\n", "NSpid:\t100 1\n"},
		{"saved_root", "Uid:\t65532 65532 65532 65532", "Uid:\t65532 65532 0 65532"},
		{"saved_group", "Gid:\t65532 65532 65532 65532", "Gid:\t65532 65532 0 65532"},
		{"privilege_gain", "NoNewPrivs:\t1", "NoNewPrivs:\t0"},
		{"no_seccomp", "Seccomp:\t2", "Seccomp:\t0"},
		{"effective_cap", "CapEff:\t0000000000000000", "CapEff:\t0000000000000001"},
		{"permitted_cap", "CapPrm:\t0000000000000000", "CapPrm:\t0000000000000001"},
		{"bounding_cap", "CapBnd:\t0000000000000000", "CapBnd:\t0000000000000001"},
		{"ambient_cap", "CapAmb:\t0000000000000000", "CapAmb:\t0000000000000001"},
		{"missing_field", "CapInh:\t0000000000000000\n", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if containerCredentials(strings.Replace(containerStatus, test.old, test.replacement, 1), 65532, 65532) {
				t.Fatal("unsafe process credentials were accepted")
			}
		})
	}
	if containerCredentials(containerStatus+"Pid:\t1\n", 65532, 65532) || containerCredentials(containerStatus, 0, 0) {
		t.Fatal("duplicate fields or root identity were accepted")
	}
}

func TestContainerLimitsRejectOrdinaryProcess(t *testing.T) {
	if os.Getpid() == 1 {
		t.Skip("ordinary process check does not apply to PID 1")
	}
	boundary, err := OpenContainerLimits(containerPolicy())
	if boundary != nil || !errors.Is(err, ErrContainerUnsupported) {
		t.Fatal("ordinary process accepted as container PID 1", err)
	}
}

func TestContainerLimitsLive(t *testing.T) {
	if os.Getenv("KELVO_TEST_CONTAINER_LIMITS") != "1" {
		t.Skip("explicit read-only container limits gate required")
	}
	boundary, err := OpenContainerLimits(containerPolicy())
	if err != nil {
		t.Fatal("container boundary rejected", err)
	}
	defer boundary.Close()
	observation, err := boundary.Check(containerPolicy())
	if err != nil || observation.Scope != containerLimitScope || observation.UpperBounds != containerPolicy() {
		t.Fatal("container limits differ from fixture", observation, err)
	}
	strict := containerPolicy()
	strict.MaxProcesses = 32
	if _, err := boundary.Check(strict); !errors.Is(err, ErrUnavailable) {
		t.Fatal("container task bound exceeded policy", err)
	}
	if err := boundary.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := boundary.Check(containerPolicy()); !errors.Is(err, ErrUnavailable) {
		t.Fatal("closed boundary remained available", err)
	}
}
