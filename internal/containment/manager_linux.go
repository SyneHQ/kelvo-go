//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

type Manager struct {
	closeMu          sync.Mutex
	mu               sync.Mutex
	config           Config
	hierarchy, state *tree
	lock             *os.File
	jobs             map[string]*Job
	quarantined      map[string]bool
	draining, closed bool
	onQuarantine     func(error)
}

type Job struct {
	mu                                 sync.Mutex
	manager                            *Manager
	name                               string
	group                              groupIO
	release                            func()
	attached, empty, removed, complete bool
	observed                           Usage
}

// Open validates explicit delegation, recovers only recorded owned groups, and
// exercises control writes/readback before admitting work. Existing service or
// session membership is never changed. Missing enforcement fails closed.
func Open(config Config) (_ *Manager, resultErr error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	hierarchy, err := openTree(config.Root, false, true)
	if err != nil {
		return nil, err
	}
	// Lock the actual delegated hierarchy, not only its local state path.
	// Different state directories cannot create concurrent managers here.
	if unix.Flock(hierarchy.fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		hierarchy.close()
		return nil, ErrOwnership
	}
	state, err := openTree(config.StateDirectory, true, false)
	if err != nil {
		hierarchy.close()
		return nil, err
	}
	lock, err := privateFile(state, ".kelvo-containment.lock", unix.O_RDWR|unix.O_CREAT)
	if err != nil {
		state.close()
		hierarchy.close()
		return nil, ErrOwnership
	}
	if unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB) != nil {
		lock.Close()
		state.close()
		hierarchy.close()
		return nil, ErrOwnership
	}
	m := &Manager{config: config, hierarchy: hierarchy, state: state, lock: lock, jobs: map[string]*Job{}, quarantined: map[string]bool{}}
	defer func() {
		if resultErr != nil {
			m.closeFiles()
		}
	}()
	kind, err := readFile(hierarchy.fd, "cgroup.type", 128)
	if err != nil || kind != "domain\n" {
		return nil, ErrUnsupported
	}
	processes, err := readFile(hierarchy.fd, "cgroup.procs", controlLimit)
	if err != nil || strings.TrimSpace(processes) != "" {
		return nil, ErrOwnership
	}
	available, err := readFile(hierarchy.fd, "cgroup.controllers", controlLimit)
	if err != nil || !requiredControllers(available) {
		return nil, ErrUnsupported
	}
	if _, _, _, err = m.inventory(); err != nil {
		return nil, err
	}
	if err = writeControl(hierarchy.fd, "cgroup.subtree_control", "+cpu +memory +pids"); err != nil {
		return nil, ErrUnavailable
	}
	enabled, err := readFile(hierarchy.fd, "cgroup.subtree_control", controlLimit)
	if err != nil || !requiredControllers(enabled) {
		return nil, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.CleanupTimeout)
	defer cancel()
	if err := m.recover(ctx); err != nil {
		return nil, err
	}
	probe, err := m.Prepare(Limits{MemoryBytes: 64 << 20, MaxProcesses: 64, CPUQuotaMicros: 100000, CPUPeriodMicros: 100000}, func() {})
	if err != nil {
		return nil, err
	}
	if _, err = probe.Finish(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

func requiredControllers(raw string) bool {
	found := map[string]bool{}
	for _, controller := range strings.Fields(raw) {
		found[controller] = true
	}
	return found["cpu"] && found["memory"] && found["pids"]
}

// SetOnQuarantine installs a notification for integration with node readiness
// and admission drain. The callback runs outside manager/job locks. Installing
// one after a quarantine immediately notifies the caller.
func (m *Manager) SetOnQuarantine(callback func(error)) {
	m.mu.Lock()
	m.onQuarantine = callback
	draining := m.draining
	m.mu.Unlock()
	if draining && callback != nil {
		callback(ErrQuarantined)
	}
}

// QuarantineOperation preserves admission and ownership locks when caller-side
// scratch or publication cleanup remains uncertain after process cleanup.
func (m *Manager) QuarantineOperation() {
	if notify := m.quarantine("operation"); notify != nil {
		notify(ErrQuarantined)
	}
}

func (m *Manager) Healthy() bool { return m.Err() == nil }
func (m *Manager) Err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrUnavailable
	}
	if len(m.quarantined) > 0 {
		return ErrQuarantined
	}
	if m.draining {
		return ErrDraining
	}
	return nil
}
func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Status{Active: len(m.jobs), Quarantined: len(m.quarantined), Draining: m.draining, Closed: m.closed}
}

// Prepare transfers release custody only on success. On failure the caller
// still owns release; no process can have been attached or started. A successful
// job retains it until Finish verifies populated=0 AND removes owned state.
func (m *Manager) Prepare(limits Limits, release func()) (*Job, error) {
	if limits.validate() != nil || release == nil {
		return nil, ErrInvalid
	}
	m.mu.Lock()
	if m.closed || m.draining {
		m.mu.Unlock()
		return nil, ErrDraining
	}
	if len(m.jobs) >= m.config.MaxGroups {
		m.mu.Unlock()
		return nil, ErrUnavailable
	}
	job, err := m.prepare(limits, release)
	var notify func(error)
	if errors.Is(err, ErrQuarantined) {
		m.draining = true
		m.quarantined["preparation"] = true
		notify = m.onQuarantine
	}
	if err == nil {
		m.jobs[job.name] = job
	}
	m.mu.Unlock()
	if notify != nil {
		notify(ErrQuarantined)
	}
	return job, err
}

func (m *Manager) prepare(limits Limits, release func()) (_ *Job, resultErr error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, ErrUnavailable
	}
	name := groupPrefix + hex.EncodeToString(nonce[:])
	intent := record{Name: name, RootDevice: m.hierarchy.device, RootInode: m.hierarchy.inode}
	intentCreated, intentErr := writeNewPrivate(m.state, name+".intent", intent.encode())
	if intentErr != nil {
		if intentCreated && m.removeState(name+".intent") != nil {
			return nil, ErrQuarantined
		}
		return nil, ErrUnavailable
	}
	var group *kernelGroup
	created, ownedCreated := false, false
	defer func() {
		if resultErr == nil {
			return
		}
		// No Attach is possible until prepare returns. Incomplete groups must
		// still be verified empty before rollback removes their ownership.
		if created {
			if group == nil {
				group, _ = openGroup(m.hierarchy, name)
			}
			if group == nil {
				resultErr = ErrQuarantined
				return
			}
			defer group.close()
			populated, err := group.populated()
			if err != nil || populated || group.remove() != nil {
				resultErr = ErrQuarantined
				return
			}
		}
		if ownedCreated {
			if err := m.removeState(name + ".owned"); err != nil {
				resultErr = ErrQuarantined
				return
			}
		}
		if err := m.removeState(name + ".intent"); err != nil {
			resultErr = ErrQuarantined
		}
	}()
	if err := unix.Mkdirat(m.hierarchy.fd, name, 0o755); err != nil {
		return nil, ErrUnavailable
	}
	created = true
	var err error
	group, err = openGroup(m.hierarchy, name)
	if err != nil {
		return nil, ErrOwnership
	}
	if err = group.configure(limits); err != nil {
		return nil, err
	}
	ownedCreated, err = writeNewPrivate(m.state, name+".owned", group.record.encode())
	if err != nil {
		return nil, ErrUnavailable
	}
	return &Job{manager: m, name: name, group: group, release: release}, nil
}

func (m *Manager) removeState(name string) error {
	file, err := privateFile(m.state, name, unix.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return ErrOwnership
	}
	file.Close()
	if err := m.state.root.Remove(name); err != nil {
		return err
	}
	return unix.Fsync(m.state.fd)
}

// Start holds both job ownership and the manager's admission boundary through
// cmd.Start, so concurrent cleanup cannot close/reuse CgroupFD before clone3.
// A clone3 error never falls back to post-start migration or uncontained work.
func (j *Job) Start(cmd *exec.Cmd) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.complete || j.attached || j.empty || j.removed {
		return ErrInvalid
	}
	j.manager.mu.Lock()
	defer j.manager.mu.Unlock()
	if j.manager.closed || j.manager.draining {
		return ErrDraining
	}
	if err := j.group.attach(cmd); err != nil {
		return err
	}
	j.attached = true
	return cmd.Start()
}

func (j *Job) Usage() (Usage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.removed {
		return j.observed, nil
	}
	return j.group.usage()
}

func (m *Manager) quarantine(name string) func(error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	first := !m.draining
	m.draining = true
	m.quarantined[name] = true
	if first {
		return m.onQuarantine
	}
	return nil
}

// Finish never releases custody on uncertainty. cgroup.kill covers descendants
// even after they leave the leader's process group or session. A later Finish
// can retry quarantine cleanup, but the manager remains permanently draining.
func (j *Job) Finish(parent context.Context) (Usage, error) {
	j.mu.Lock()
	var notify func(error)
	var releaseAfterUnlock func()
	var failedStage string
	defer func() {
		j.mu.Unlock()
		if releaseAfterUnlock != nil {
			releaseAfterUnlock()
		}
		if notify != nil {
			notify(ErrQuarantined)
		}
		if failedStage != "" {
			// Emit after unlocking and notifying admission. A slow log sink must
			// never keep an uncertain manager available for new work.
			log.Printf("Kelvo process cleanup uncertain: stage=%s", failedStage)
		}
	}()
	if j.complete {
		return j.observed, nil
	}
	ctx, cancel := context.WithTimeout(parent, j.manager.config.CleanupTimeout)
	defer cancel()
	fail := func(stage string) (Usage, error) {
		// Fixed labels identify failures even if Close later succeeds in a retry.
		// Never log cgroup paths, operation IDs or underlying OS error strings.
		failedStage = stage
		notify = j.manager.quarantine(j.name)
		return j.observed, ErrQuarantined
	}
	if !j.empty {
		if ctx.Err() != nil {
			return fail("deadline_before_kill")
		}
		if j.group.kill() != nil {
			return fail("group_kill")
		}
		for {
			populated, err := j.group.populated()
			if err != nil {
				return fail("population_read")
			}
			if !populated {
				j.empty = true
				break
			}
			select {
			case <-ctx.Done():
				return fail("population_deadline")
			case <-time.After(10 * time.Millisecond):
			}
		}
		observed, err := j.group.usage()
		if err != nil {
			j.empty = false
			return fail("usage_read")
		}
		j.observed = observed
	}
	if !j.removed {
		if err := j.group.remove(); err != nil {
			return fail("group_remove")
		}
		j.removed = true
		if err := j.group.close(); err != nil {
			return fail("group_close")
		}
	}
	if j.manager.removeState(j.name+".owned") != nil {
		return fail("owned_record_remove")
	}
	if j.manager.removeState(j.name+".intent") != nil {
		return fail("intent_record_remove")
	}
	j.complete = true
	j.manager.mu.Lock()
	delete(j.manager.jobs, j.name)
	delete(j.manager.quarantined, j.name)
	j.manager.mu.Unlock()
	releaseAfterUnlock = j.release
	j.release = nil
	// Custody callbacks only update an operation's latch; their eventual pool
	// release may occur later, after caller-side scratch/publication cleanup.
	return j.observed, nil
}

// Close drains and tries all owned jobs within one shared cleanup deadline.
// Failure retains manager locks, records and callbacks until explicit retry or
// process exit; restarting Open must recover those owned groups before admission.
func (m *Manager) Close(parent context.Context) error {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.draining = true
	jobs := make([]*Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		jobs = append(jobs, job)
	}
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(parent, m.config.CleanupTimeout)
	defer cancel()
	var failed bool
	for _, job := range jobs {
		if _, err := job.Finish(ctx); err != nil {
			failed = true
		}
	}
	if failed {
		return ErrQuarantined
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.quarantined) > 0 {
		return ErrQuarantined
	}
	m.closed = true
	return m.closeFiles()
}

func (m *Manager) closeFiles() error {
	for _, job := range m.jobs {
		_ = job.group.close()
	}
	var lockErr error
	if m.lock != nil {
		lockErr = m.lock.Close()
		m.lock = nil
	}
	return errors.Join(lockErr, m.state.close(), m.hierarchy.close())
}

func (m *Manager) inventory() (map[string]record, map[string]record, map[string]bool, error) {
	entries, err := names(m.state, m.config.MaxGroups*3+16)
	if err != nil {
		return nil, nil, nil, ErrOwnership
	}
	intents, owned := map[string]record{}, map[string]record{}
	for _, entry := range entries {
		name := entry.Name()
		if name == ".kelvo-containment.lock" {
			continue
		}
		ending := ""
		if strings.HasSuffix(name, ".intent") {
			ending = ".intent"
		} else if strings.HasSuffix(name, ".owned") {
			ending = ".owned"
		}
		if ending == "" || !safeName(strings.TrimSuffix(name, ending)) {
			return nil, nil, nil, ErrOwnership
		}
		raw, err := readPrivate(m.state, name)
		if err != nil {
			return nil, nil, nil, ErrOwnership
		}
		r, err := decodeRecord(raw)
		if err != nil || r.Name != strings.TrimSuffix(name, ending) || r.RootDevice != m.hierarchy.device || r.RootInode != m.hierarchy.inode {
			return nil, nil, nil, ErrOwnership
		}
		if ending == ".intent" {
			if r.GroupInode != 0 {
				return nil, nil, nil, ErrOwnership
			}
			intents[r.Name] = r
		} else {
			if r.GroupInode == 0 {
				return nil, nil, nil, ErrOwnership
			}
			owned[r.Name] = r
		}
	}
	if len(intents) > m.config.MaxGroups {
		return nil, nil, nil, ErrOwnership
	}
	for name := range owned {
		if _, ok := intents[name]; !ok {
			return nil, nil, nil, ErrOwnership
		}
	}
	groups, err := names(m.hierarchy, m.config.MaxGroups+256)
	if err != nil {
		return nil, nil, nil, ErrOwnership
	}
	present := map[string]bool{}
	for _, entry := range groups {
		if !entry.IsDir() {
			continue
		}
		if _, ok := intents[entry.Name()]; !ok {
			return nil, nil, nil, ErrOwnership
		}
		present[entry.Name()] = true
	}
	return intents, owned, present, nil
}

func (m *Manager) recover(ctx context.Context) error {
	intents, owned, present, err := m.inventory()
	if err != nil {
		return err
	}
	for name := range intents {
		if ctx.Err() != nil {
			return ErrQuarantined
		}
		if !present[name] {
			if m.removeState(name+".owned") != nil || m.removeState(name+".intent") != nil {
				return ErrOwnership
			}
			continue
		}
		group, err := openGroup(m.hierarchy, name)
		if err != nil {
			return ErrOwnership
		}
		if pin, committed := owned[name]; committed {
			if group.record != pin {
				group.close()
				return ErrOwnership
			}
			job := &Job{manager: m, name: name, group: group, release: func() {}}
			m.jobs[name] = job
			if _, err := job.Finish(ctx); err != nil {
				return err
			}
		} else {
			// Durable intent precedes mkdir, but no committed ownership means no
			// process could have been attached. Never kill an uncommitted group.
			populated, err := group.populated()
			if err != nil || populated || group.remove() != nil {
				group.close()
				return ErrOwnership
			}
			group.close()
			if m.removeState(name+".intent") != nil {
				return ErrOwnership
			}
		}
	}
	return nil
}
