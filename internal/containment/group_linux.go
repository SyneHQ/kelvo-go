//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// groupIO is deliberately private: production always uses inode-pinned cgroup
// descriptors. Tests inject failures without pretending ordinary files enforce
// kernel limits.
type groupIO interface {
	attach(*exec.Cmd) error
	kill() error
	populated() (bool, error)
	usage() (Usage, error)
	remove() error
	close() error
}

type kernelGroup struct {
	parent *tree
	fd     *os.File
	record record
}

func openGroup(parent *tree, name string) (*kernelGroup, error) {
	file, err := openAt(parent.fd, name, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if unix.Fstat(int(file.Fd()), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || int(st.Uid) != os.Geteuid() {
		file.Close()
		return nil, ErrOwnership
	}
	return &kernelGroup{parent: parent, fd: file, record: record{Name: name, RootDevice: parent.device, RootInode: parent.inode, GroupInode: st.Ino}}, nil
}

func (g *kernelGroup) configure(limits Limits) error {
	wanted := []struct{ name, value string }{
		{"memory.max", strconv.FormatInt(limits.MemoryBytes, 10)}, {"memory.swap.max", "0"},
		{"memory.oom.group", "1"}, {"pids.max", strconv.FormatInt(limits.MaxProcesses, 10)},
		{"cpu.max", fmt.Sprintf("%d %d", limits.CPUQuotaMicros, limits.CPUPeriodMicros)},
	}
	kind, err := readFile(int(g.fd.Fd()), "cgroup.type", 128)
	if err != nil || kind != "domain\n" {
		return ErrUnsupported
	}
	for _, control := range wanted {
		if err := writeControl(int(g.fd.Fd()), control.name, control.value); err != nil {
			return ErrUnavailable
		}
		actual, err := readFile(int(g.fd.Fd()), control.name, 128)
		if err != nil || strings.TrimSuffix(actual, "\n") != control.value {
			return ErrUnavailable
		}
	}
	populated, err := g.populated()
	if err != nil || populated {
		return ErrOwnership
	}
	_, err = g.usage()
	return err
}

func (g *kernelGroup) attach(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process != nil {
		return ErrInvalid
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	if cmd.SysProcAttr.UseCgroupFD {
		return ErrInvalid
	}
	cmd.SysProcAttr.UseCgroupFD = true
	cmd.SysProcAttr.CgroupFD = int(g.fd.Fd())
	return nil
}
func (g *kernelGroup) kill() error  { return writeControl(int(g.fd.Fd()), "cgroup.kill", "1") }
func (g *kernelGroup) close() error { return g.fd.Close() }
func (g *kernelGroup) populated() (bool, error) {
	raw, err := readFile(int(g.fd.Fd()), "cgroup.events", controlLimit)
	if err != nil {
		return false, ErrUnavailable
	}
	values, err := counters(raw)
	if err != nil {
		return false, err
	}
	value, present := values["populated"]
	if !present || value > 1 {
		return false, ErrUnavailable
	}
	return value == 1, nil
}
func (g *kernelGroup) usage() (Usage, error) {
	var observed Usage
	for _, target := range []struct {
		name  string
		value *uint64
	}{
		{"memory.current", &observed.MemoryCurrentBytes}, {"memory.peak", &observed.MemoryPeakBytes},
	} {
		raw, err := readFile(int(g.fd.Fd()), target.name, 128)
		if err != nil {
			return Usage{}, ErrUnavailable
		}
		*target.value, err = number(raw)
		if err != nil {
			return Usage{}, err
		}
	}
	for _, target := range []struct {
		name   string
		fields map[string]*uint64
	}{
		{"memory.events", map[string]*uint64{"oom": &observed.OOMEvents, "oom_kill": &observed.OOMKills}},
		{"pids.events", map[string]*uint64{"max": &observed.PIDsLimitEvents}},
		{"cpu.stat", map[string]*uint64{"usage_usec": &observed.CPUUsageMicros, "nr_throttled": &observed.CPUThrottledPeriods}},
	} {
		raw, err := readFile(int(g.fd.Fd()), target.name, controlLimit)
		if err != nil {
			return Usage{}, ErrUnavailable
		}
		values, err := counters(raw)
		if err != nil {
			return Usage{}, err
		}
		for key, destination := range target.fields {
			value, exists := values[key]
			if !exists {
				return Usage{}, ErrUnavailable
			}
			*destination = value
		}
	}
	return observed, nil
}
func (g *kernelGroup) remove() error {
	// Never recursively delete cgroups or adopt a replaced same-name group.
	var current unix.Stat_t
	if unix.Fstatat(g.parent.fd, g.record.Name, &current, unix.AT_SYMLINK_NOFOLLOW) != nil || current.Ino != g.record.GroupInode ||
		uint64(current.Dev) != g.record.RootDevice || current.Mode&unix.S_IFMT != unix.S_IFDIR {
		return ErrOwnership
	}
	return unix.Unlinkat(g.parent.fd, g.record.Name, unix.AT_REMOVEDIR)
}
