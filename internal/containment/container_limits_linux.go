//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// ContainerLimits observes an existing, read-only cgroup namespace. It does not
// create an execution backend, launch children or authorize custody release.
// A finite visible limit is an upper bound even when hidden ancestors impose
// lower limits. The observation is not a measurement of those effective limits.
type ContainerLimits struct {
	mu       sync.Mutex
	group    *os.File
	procRoot *os.File
	proc     *os.File
	procNS   *os.File
	device   uint64
	inode    uint64
	ns       map[string]string
	closed   bool
}

// ContainerLimitObservation describes the visible cgroup that contains PID 1.
// Memory includes all charged processes and file cache. These observations do
// not attest exclusive container ownership or the absence of writable aliases.
// CPU is the configured fair-class bandwidth quota with zero configured burst.
// MaxProcesses counts kernel tasks, including threads. Scheduling policy and
// descendant ownership require a separate execution contract.
type ContainerLimitObservation struct {
	UpperBounds Limits
	Scope       string
}

const containerLimitScope = "visible_cgroup_upper_bounds"

var ErrContainerUnsupported = errors.New("container limit inspection requires PID 1 and read-only Linux cgroup v2")

// OpenContainerLimits requires PID 1, cgroup membership relative to /,
// non-root credentials, zero capabilities and read-only cgroup controls.
// It rejects a visible limit that exceeds policy. It never changes a limit or
// infers a tighter limit from an ancestor that the container cannot inspect.
func OpenContainerLimits(policy Limits) (_ *ContainerLimits, resultErr error) {
	if policy.validate() != nil {
		return nil, ErrInvalid
	}
	if os.Getpid() != 1 || os.Getuid() == 0 || os.Getuid() != os.Geteuid() {
		return nil, ErrContainerUnsupported
	}
	b := &ContainerLimits{ns: make(map[string]string)}
	defer func() {
		if resultErr != nil {
			_ = b.Close()
		}
	}()
	rootFD, err := unix.Openat2(unix.AT_FDCWD, "/proc", &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return nil, ErrOwnership
	}
	b.procRoot = os.NewFile(uintptr(rootFD), "container proc root")
	var procFS unix.Statfs_t
	self, selfErr := containerNamespace(rootFD, "self")
	if unix.Fstatfs(rootFD, &procFS) != nil || procFS.Type != unix.PROC_SUPER_MAGIC || selfErr != nil || self != "1" {
		return nil, ErrOwnership
	}
	procFD, err := unix.Openat2(rootFD, "1", &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV,
	})
	if err != nil {
		return nil, ErrOwnership
	}
	b.proc = os.NewFile(uintptr(procFD), "container proc directory")
	nsFD, err := unix.Openat2(procFD, "ns", &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV,
	})
	if err != nil {
		return nil, ErrOwnership
	}
	b.procNS = os.NewFile(uintptr(nsFD), "container namespace directory")
	for _, name := range []string{"pid", "cgroup", "mnt", "user"} {
		value, err := containerNamespace(nsFD, name)
		if err != nil {
			return nil, ErrOwnership
		}
		b.ns[name] = value
	}
	fd, err := unix.Openat2(unix.AT_FDCWD, "/sys/fs/cgroup", &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return nil, ErrContainerUnsupported
	}
	b.group = os.NewFile(uintptr(fd), "container cgroup limits")
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != 0 {
		return nil, ErrOwnership
	}
	b.device, b.inode = uint64(st.Dev), st.Ino
	if _, err := b.Check(policy); err != nil {
		return nil, err
	}
	return b, nil
}

// Check revalidates the current namespace and reads current kernel limits.
// Runtime authorities can change limits. Callers must check before admission
// and handle later runtime changes through their separate execution contract.
func (b *ContainerLimits) Check(policy Limits) (ContainerLimitObservation, error) {
	if b == nil || policy.validate() != nil {
		return ContainerLimitObservation{}, ErrInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.group == nil || b.proc == nil || b.procNS == nil || b.procRoot == nil {
		return ContainerLimitObservation{}, ErrUnavailable
	}
	if os.Getpid() != 1 || os.Getuid() == 0 || os.Getuid() != os.Geteuid() {
		return ContainerLimitObservation{}, ErrOwnership
	}
	self, err := containerNamespace(int(b.procRoot.Fd()), "self")
	if err != nil || self != "1" {
		return ContainerLimitObservation{}, ErrOwnership
	}
	for name, expected := range b.ns {
		current, err := containerNamespace(int(b.procNS.Fd()), name)
		if err != nil || current != expected {
			return ContainerLimitObservation{}, ErrOwnership
		}
	}
	status, err := containerKernelRead(int(b.proc.Fd()), "status", unix.PROC_SUPER_MAGIC, controlLimit)
	if err != nil || !containerCredentials(status, os.Getuid(), os.Getgid()) {
		return ContainerLimitObservation{}, ErrOwnership
	}
	membership, err := containerKernelRead(int(b.proc.Fd()), "cgroup", unix.PROC_SUPER_MAGIC, controlLimit)
	if err != nil || membership != "0::/\n" {
		return ContainerLimitObservation{}, ErrOwnership
	}
	var fs unix.Statfs_t
	if unix.Fstatfs(int(b.group.Fd()), &fs) != nil || fs.Type != cgroupMagic || fs.Flags&unix.ST_RDONLY == 0 {
		return ContainerLimitObservation{}, ErrContainerUnsupported
	}
	var current unix.Stat_t
	if unix.Lstat("/sys/fs/cgroup", &current) != nil || current.Mode&unix.S_IFMT != unix.S_IFDIR ||
		uint64(current.Dev) != b.device || current.Ino != b.inode {
		return ContainerLimitObservation{}, ErrOwnership
	}
	read := func(name string) (string, error) {
		return containerKernelRead(int(b.group.Fd()), name, cgroupMagic, 128)
	}
	processes, err := containerKernelRead(int(b.group.Fd()), "cgroup.procs", cgroupMagic, controlLimit)
	if err != nil || !containsContainerPID(processes) {
		return ContainerLimitObservation{}, ErrOwnership
	}
	limits, err := containerUpperBounds(read)
	if err != nil {
		return ContainerLimitObservation{}, err
	}
	if !limitsWithin(limits, policy) {
		return ContainerLimitObservation{}, ErrUnavailable
	}
	// A stable mounted inode alone does not bind the process to that cgroup.
	// Recheck actual membership after reading controls to detect migration.
	processes, err = containerKernelRead(int(b.group.Fd()), "cgroup.procs", cgroupMagic, controlLimit)
	if err != nil || !containsContainerPID(processes) {
		return ContainerLimitObservation{}, ErrOwnership
	}
	for name, expected := range b.ns {
		current, err := containerNamespace(int(b.procNS.Fd()), name)
		if err != nil || current != expected {
			return ContainerLimitObservation{}, ErrOwnership
		}
	}
	self, err = containerNamespace(int(b.procRoot.Fd()), "self")
	if err != nil || self != "1" {
		return ContainerLimitObservation{}, ErrOwnership
	}
	return ContainerLimitObservation{UpperBounds: limits, Scope: containerLimitScope}, nil
}

func (b *ContainerLimits) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	var result error
	for _, file := range []*os.File{b.group, b.procNS, b.proc, b.procRoot} {
		if file != nil {
			if err := file.Close(); err != nil && result == nil {
				result = err
			}
		}
	}
	return result
}

func containerKernelRead(parent int, name string, filesystem int64, limit int64) (string, error) {
	fd, err := unix.Openat2(parent, name, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV,
	})
	if err != nil {
		return "", ErrUnavailable
	}
	file := os.NewFile(uintptr(fd), "container kernel observation")
	defer file.Close()
	var fs unix.Statfs_t
	var st unix.Stat_t
	if unix.Fstatfs(fd, &fs) != nil || int64(fs.Type) != filesystem || unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG {
		return "", ErrOwnership
	}
	// Procfs reports a zero file size. Read a fixed maximum instead of trusting
	// stat size or permitting an unbounded read from an unexpected mount.
	buffer, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(buffer)) > limit {
		return "", ErrUnavailable
	}
	return string(buffer), nil
}

func containerNamespace(parent int, name string) (string, error) {
	buffer := make([]byte, 128)
	n, err := unix.Readlinkat(parent, name, buffer)
	if err != nil || n == 0 || n == len(buffer) {
		return "", ErrOwnership
	}
	return string(buffer[:n]), nil
}

func containsContainerPID(raw string) bool {
	found := false
	for _, value := range strings.Fields(raw) {
		pid, err := strconv.ParseUint(value, 10, 32)
		if err != nil || pid == 0 {
			return false
		}
		found = found || pid == 1
	}
	return found
}

func containerCredentials(status string, uid, gid int) bool {
	fields := make(map[string]string)
	for _, line := range strings.Split(status, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if _, duplicate := fields[key]; duplicate {
			return false
		}
		fields[key] = strings.TrimSpace(value)
	}
	if uid <= 0 || gid <= 0 || fields["Pid"] != "1" || fields["NSpid"] != "1" || fields["NoNewPrivs"] != "1" || fields["Seccomp"] != "2" {
		return false
	}
	for key, expected := range map[string]int{"Uid": uid, "Gid": gid} {
		values := strings.Fields(fields[key])
		if len(values) != 4 {
			return false
		}
		for _, value := range values {
			if value != strconv.Itoa(expected) {
				return false
			}
		}
	}
	for _, key := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
		value, err := strconv.ParseUint(fields[key], 16, 64)
		if err != nil || value != 0 {
			return false
		}
	}
	return true
}

func containerUpperBounds(read func(string) (string, error)) (Limits, error) {
	var limits Limits
	for name, expected := range map[string]string{"cgroup.type": "domain\n", "memory.swap.max": "0\n", "memory.oom.group": "1\n", "cpu.max.burst": "0\n"} {
		value, err := read(name)
		if err != nil || value != expected {
			return limits, ErrContainerUnsupported
		}
	}
	for name, target := range map[string]*int64{"memory.max": &limits.MemoryBytes, "pids.max": &limits.MaxProcesses} {
		value, err := read(name)
		if err != nil {
			return limits, ErrUnavailable
		}
		parsed, err := strconv.ParseInt(strings.TrimSuffix(value, "\n"), 10, 64)
		if err != nil || parsed <= 0 {
			return limits, ErrUnavailable
		}
		*target = parsed
	}
	value, err := read("cpu.max")
	if err != nil {
		return limits, ErrUnavailable
	}
	parts := strings.Fields(value)
	if len(parts) != 2 {
		return limits, ErrUnavailable
	}
	limits.CPUQuotaMicros, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return limits, ErrUnavailable
	}
	limits.CPUPeriodMicros, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || limits.validate() != nil {
		return limits, ErrUnavailable
	}
	return limits, nil
}

func limitsWithin(observed, policy Limits) bool {
	// Validation bounds both products well below MaxInt64. Compare ratios,
	// because equivalent quotas can use different cgroup periods.
	return observed.validate() == nil && policy.validate() == nil &&
		observed.MemoryBytes <= policy.MemoryBytes && observed.MaxProcesses <= policy.MaxProcesses &&
		observed.CPUQuotaMicros*policy.CPUPeriodMicros <= policy.CPUQuotaMicros*observed.CPUPeriodMicros
}
