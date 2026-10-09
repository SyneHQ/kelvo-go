//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// PrepareProcess keeps one command wait owner alongside delegated group custody.
// Finish uses the manager cleanup timeout and any earlier caller deadline.
func (m *Manager) PrepareProcess(_ *Custody, limits Limits, release func()) (Process, error) {
	if m == nil {
		return nil, ErrInvalid
	}
	return prepareProcess(m, limits, release, m.config.CleanupTimeout)
}

func commandExit(command *exec.Cmd) (ChildExit, bool) {
	if command.ProcessState == nil {
		return ChildExit{}, false
	}
	exit := ChildExit{Code: command.ProcessState.ExitCode()}
	if status, ok := command.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		exit.Signal = status.Signal()
	}
	return exit, true
}

// Terminate permits source cancellation, then drains the owned cgroup. Cgroup
// identity remains authoritative even if another cleanup owner reaped the leader.
func (p *delegatedProcess) Terminate(grace time.Duration) error {
	if grace < 0 || grace > 30*time.Second {
		return ErrInvalid
	}
	p.mu.Lock()
	command := p.command
	exited := p.exited
	p.mu.Unlock()
	if command == nil || command.Process == nil {
		return os.ErrProcessDone
	}
	if grace > 0 && !exited {
		_ = command.Process.Signal(syscall.SIGTERM)
		done, err := p.startWait()
		if err != nil {
			return err
		}
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.cleanupTimeout)
	defer cancel()
	_, err := p.group.Finish(ctx)
	return err
}
