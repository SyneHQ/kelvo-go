//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// These gates run only in an explicitly provisioned, disposable delegation.
// scripts/containment_acceptance.py provisions it and rejects missing gates.
func kernelConfig(t *testing.T) Config {
	t.Helper()
	root, state := os.Getenv("KELVO_TEST_CGROUP_ROOT"), os.Getenv("KELVO_TEST_CGROUP_STATE")
	if root == "" || state == "" {
		t.Skip("explicit disposable cgroup delegation required")
	}
	if os.Geteuid() == 0 {
		t.Fatal("live acceptance must exercise an unprivileged manager")
	}
	return Config{Root: root, StateDirectory: state, MaxGroups: 16, CleanupTimeout: 3 * time.Second}
}
func kernelManager(t *testing.T) *Manager {
	t.Helper()
	m, err := Open(kernelConfig(t))
	if err != nil {
		t.Fatal("open delegation", err)
	}
	t.Cleanup(func() {
		if err := m.Close(context.Background()); err != nil {
			t.Error("close delegation", err)
		}
	})
	return m
}
func kernelLimits() Limits {
	return Limits{MemoryBytes: 64 << 20, MaxProcesses: 32, CPUQuotaMicros: 25000, CPUPeriodMicros: 100000}
}
func kernelHelper(mode string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestKernelHelper$")
	cmd.Env = append(os.Environ(), "KELVO_KERNEL_HELPER="+mode, "GOMAXPROCS=1")
	return cmd
}
func TestKernelPrestartMembershipAndControls(t *testing.T) {
	m := kernelManager(t)
	var released atomic.Bool
	job, err := m.Prepare(kernelLimits(), func() { released.Store(true) })
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	cmd := kernelHelper("membership")
	cmd.Stdout = &output
	if err := job.Start(cmd); err != nil {
		t.Fatal("pre-start clone3 placement", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "/"+job.name+"\n") {
		t.Fatalf("child started outside assigned cgroup: %q", output.String())
	}
	group := job.group.(*kernelGroup)
	for name, want := range map[string]string{"memory.max": "67108864", "memory.swap.max": "0", "memory.oom.group": "1", "pids.max": "32", "cpu.max": "25000 100000"} {
		got, err := readFile(int(group.fd.Fd()), name, 128)
		if err != nil || strings.TrimSpace(got) != want {
			t.Fatalf("control %s: %q %v", name, got, err)
		}
	}
	if _, err := job.Finish(context.Background()); err != nil || !released.Load() {
		t.Fatal("verified cleanup", err)
	}
}
func TestKernelNativeMemoryOOM(t *testing.T) {
	m := kernelManager(t)
	job, err := m.Prepare(kernelLimits(), func() {})
	if err != nil {
		t.Fatal(err)
	}
	cmd := kernelHelper("memory")
	if err := job.Start(cmd); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("native allocator exceeded cap without OOM")
	}
	usage, err := job.Finish(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if usage.OOMKills == 0 || usage.MemoryPeakBytes < 32<<20 {
		t.Fatal("no charged native-memory OOM evidence", usage)
	}
	t.Logf("charged_native_peak_bytes=%d oom_kills=%d", usage.MemoryPeakBytes, usage.OOMKills)
}
func TestKernelCPUQuota(t *testing.T) {
	m := kernelManager(t)
	job, err := m.Prepare(kernelLimits(), func() {})
	if err != nil {
		t.Fatal(err)
	}
	cmd := kernelHelper("cpu")
	started := time.Now()
	if err := job.Start(cmd); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	usage, err := job.Finish(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if usage.CPUThrottledPeriods == 0 || usage.CPUUsageMicros == 0 || usage.CPUUsageMicros > uint64(time.Since(started).Microseconds()/2) {
		t.Fatal("CPU quota did not constrain process", usage)
	}
	t.Logf("cpu_usage_usec=%d throttled_periods=%d", usage.CPUUsageMicros, usage.CPUThrottledPeriods)
}
func TestKernelPIDsLimit(t *testing.T) {
	m := kernelManager(t)
	limits := kernelLimits()
	limits.MaxProcesses = 16
	job, err := m.Prepare(limits, func() {})
	if err != nil {
		t.Fatal(err)
	}
	cmd := kernelHelper("pids")
	if err := job.Start(cmd); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	usage, err := job.Finish(context.Background())
	if err != nil || usage.PIDsLimitEvents == 0 {
		t.Fatal("no process-limit evidence", usage, err)
	}
}
func TestKernelDetachedDescendantCleanup(t *testing.T) {
	m := kernelManager(t)
	job, err := m.Prepare(kernelLimits(), func() {})
	if err != nil {
		t.Fatal(err)
	}
	cmd := kernelHelper("descendant")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := job.Start(cmd); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	populated, err := job.group.populated()
	if err != nil || !populated {
		t.Fatal("detached descendant did not survive leader", err)
	}
	if _, err := job.Finish(context.Background()); err != nil {
		t.Fatal("cgroup.kill did not clean detached descendant", err)
	}
}
func TestKernelFailedCleanupQuarantinesCustody(t *testing.T) {
	m := kernelManager(t)
	var released, notified atomic.Int32
	custody, _ := NewCustody(func() { released.Add(1) })
	hold, _ := custody.Hold()
	job, err := m.Prepare(kernelLimits(), hold)
	if err != nil {
		hold()
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/sleep", "10")
	if err := job.Start(cmd); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(m.config.Root, job.name, "cgroup.kill")
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(path, 0600) })
	m.SetOnQuarantine(func(error) { notified.Add(1) })
	custody.Complete()
	if _, err := job.Finish(context.Background()); err != ErrQuarantined {
		t.Fatal("uncertain cleanup accepted", err)
	}
	if released.Load() != 0 || notified.Load() != 1 || m.Healthy() {
		t.Fatal("quarantine released reservation or readiness")
	}
	if _, err := m.Prepare(kernelLimits(), func() {}); err != ErrDraining {
		t.Fatal("admitted work during quarantine", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := job.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if released.Load() != 1 || m.Healthy() {
		t.Fatal("retry must release custody while staying drained")
	}
}
func TestKernelRestartRecoversOwnedProcess(t *testing.T) {
	config := kernelConfig(t)
	cmd := kernelHelper("orphan")
	output, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	name := strings.TrimSpace(string(output))
	if !safeName(name) {
		t.Fatal("invalid orphan fixture", name)
	}
	m, err := Open(config)
	if err != nil {
		t.Fatal("restart recovery", err)
	}
	defer m.Close(context.Background())
	if _, err := os.Stat(filepath.Join(config.Root, name)); !os.IsNotExist(err) {
		t.Fatal("orphan cgroup survived restart", err)
	}
}
func TestKernelRestartRecoversIncompleteIntent(t *testing.T) {
	config := kernelConfig(t)
	for _, stage := range []string{"intent", "group"} {
		cmd := kernelHelper("intent-" + stage)
		if output, err := cmd.CombinedOutput(); err == nil {
			t.Fatal("fixture did not crash", string(output))
		}
		m, err := Open(config)
		if err != nil {
			t.Fatal("incomplete intent recovery", stage, err)
		}
		if err := m.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
func TestKernelHierarchyOwnershipLock(t *testing.T) {
	m := kernelManager(t)
	other := t.TempDir()
	os.Chmod(other, 0700)
	config := m.config
	config.StateDirectory = other
	if duplicate, err := Open(config); err != ErrOwnership {
		if duplicate != nil {
			duplicate.Close(context.Background())
		}
		t.Fatal("same hierarchy admitted second manager", err)
	}
}
func TestKernelStaleRootIdentityRejected(t *testing.T) {
	config := kernelConfig(t)
	// A durable record pinned to an earlier root inode cannot authorize recovery.
	// Simulate that identity mismatch without replacing the running service.
	tr, err := openTree(config.StateDirectory, true, false)
	if err != nil {
		t.Fatal(err)
	}
	hierarchy, err := openTree(config.Root, false, true)
	if err != nil {
		tr.close()
		t.Fatal(err)
	}
	name := groupPrefix + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	r := record{Name: name, RootDevice: hierarchy.device, RootInode: hierarchy.inode + 1}
	if _, err := writeNewPrivate(tr, name+".intent", r.encode()); err != nil {
		t.Fatal(err)
	}
	hierarchy.close()
	tr.close()
	defer os.Remove(filepath.Join(config.StateDirectory, name+".intent"))
	if m, err := Open(config); err != ErrOwnership {
		if m != nil {
			m.Close(context.Background())
		}
		t.Fatal("stale hierarchy identity adopted", err)
	}
}
func TestKernelHelper(t *testing.T) {
	mode := os.Getenv("KELVO_KERNEL_HELPER")
	if mode == "" {
		return
	}
	switch mode {
	case "membership":
		body, err := os.ReadFile("/proc/self/cgroup")
		if err != nil {
			os.Exit(2)
		}
		fmt.Print(string(body))
	case "memory":
		data, err := unix.Mmap(-1, 0, 256<<20, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
		if err != nil {
			os.Exit(3)
		}
		for i := 0; i < len(data); i += 4096 {
			data[i] = 1
		}
		unix.Munmap(data)
	case "cpu":
		until := time.Now().Add(1600 * time.Millisecond)
		var value uint64
		for time.Now().Before(until) {
			for range 10000 {
				value++
			}
		}
		fmt.Fprintln(io.Discard, value)
	case "pids":
		var children []*exec.Cmd
		blocked := false
		for range 40 {
			child := exec.Command("/bin/sleep", "8")
			child.Stdout = io.Discard
			child.Stderr = io.Discard
			if child.Start() != nil {
				blocked = true
				break
			}
			children = append(children, child)
		}
		for _, child := range children {
			child.Process.Kill()
			child.Wait()
		}
		if !blocked {
			os.Exit(4)
		}
	case "descendant":
		child := exec.Command("/bin/sleep", "10")
		child.Stdout = io.Discard
		child.Stderr = io.Discard
		child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if child.Start() != nil {
			os.Exit(5)
		}
	case "orphan":
		m, err := Open(kernelConfig(t))
		if err != nil {
			os.Exit(6)
		}
		job, err := m.Prepare(kernelLimits(), func() {})
		if err != nil {
			os.Exit(7)
		}
		child := exec.Command("/bin/sleep", "20")
		child.Stdout = io.Discard
		child.Stderr = io.Discard
		if job.Start(child) != nil {
			os.Exit(8)
		}
		fmt.Println(job.name)
	case "intent-intent", "intent-group":
		config := kernelConfig(t)
		m, err := Open(config)
		if err != nil {
			os.Exit(9)
		}
		name := groupPrefix + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		r := record{Name: name, RootDevice: m.hierarchy.device, RootInode: m.hierarchy.inode}
		if _, err := writeNewPrivate(m.state, name+".intent", r.encode()); err != nil {
			os.Exit(10)
		}
		if mode == "intent-group" {
			if unix.Mkdirat(m.hierarchy.fd, name, 0755) != nil {
				os.Exit(11)
			}
		}
		os.Exit(77)
	default:
		fmt.Fprintln(os.Stderr, strconv.Quote(mode))
		os.Exit(12)
	}
	os.Exit(0)
}

func TestKernelSandboxRuntime(t *testing.T) {
	launcher, helper := os.Getenv("KELVO_TEST_SANDBOX"), os.Getenv("KELVO_TEST_RUNTIME_HELPER")
	if launcher == "" || helper == "" {
		t.Skip("built launcher and harmless runtime helper required")
	}
	m := kernelManager(t)
	job, err := m.Prepare(kernelLimits(), func() {})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(launcher, "--write", t.TempDir(), "--", helper, "worker")
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := job.Start(cmd); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal("sandbox runtime compatibility", err, output.String())
	}
	if !strings.Contains(output.String(), "clone3_denied native_threads_ok ordinary_fork_ok") {
		t.Fatal("missing safe denial/runtime evidence")
	}
	if _, err := job.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestKernelMissingControllersFailsClosed(t *testing.T) {
	config := kernelConfig(t)
	parent := filepath.Join(filepath.Dir(config.Root), "missing-controllers")
	if err := os.Mkdir(parent, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(parent)
	child := filepath.Join(parent, "jobs")
	if err := os.Mkdir(child, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(child)
	config.Root = child
	if m, err := Open(config); err != ErrUnsupported {
		if m != nil {
			m.Close(context.Background())
		}
		t.Fatal("missing controllers did not fail closed", err)
	}
}
