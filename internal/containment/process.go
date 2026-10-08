// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"os/exec"
	"sync"
)

// Process owns a command from Start through Wait and resource cleanup. After
// Start, callers must use Wait instead of calling Cmd.Wait or Process.Wait.
// Finish prevents further starts and retains custody until cleanup and reaping
// both complete. Usage belongs to the containment domain, including descendants.
// This interface does not enable a container executor or a PID-1 reaper.
type Process interface {
	Start(*exec.Cmd) error
	Wait() error
	Usage() (Usage, error)
	Finish(context.Context) (Usage, error)
}

type processGroup interface {
	Start(*exec.Cmd) error
	Usage() (Usage, error)
	Finish(context.Context) (Usage, error)
}

// PrepareProcess preserves Prepare's admission contract. On error, the caller
// still owns release. On success, release also waits for the command's sole
// Wait owner, including when Manager.Close cleans the group concurrently.
func (m *Manager) PrepareProcess(limits Limits, release func()) (Process, error) {
	if m == nil {
		return nil, ErrInvalid
	}
	custody, err := NewCustody(release)
	if err != nil {
		return nil, err
	}
	reaped, err := custody.Hold()
	if err != nil {
		return nil, err
	}
	job, err := m.Prepare(limits, custody.Complete)
	if err != nil {
		return nil, err
	}
	return &delegatedProcess{group: job, releaseReap: reaped}, nil
}

// Delegated execution keeps os/exec as the sole wait owner. A future PID-1
// backend must implement the same lifecycle without adding a competing reaper.
type delegatedProcess struct {
	mu             sync.Mutex
	group          processGroup
	command        *exec.Cmd
	startAttempted bool
	finishing      bool
	waitOnce       sync.Once
	waitErr        error
	releaseReap    func()
}

func (p *delegatedProcess) Start(command *exec.Cmd) error {
	p.mu.Lock()
	if command == nil || p.startAttempted || p.finishing {
		p.mu.Unlock()
		return ErrInvalid
	}
	p.startAttempted = true
	err := p.group.Start(command)
	if err == nil {
		p.command = command
	}
	p.mu.Unlock()
	if err != nil {
		// Cmd.Start owns rollback for an unsuccessful launch. The group still
		// retains its separate custody until Finish proves cleanup.
		p.releaseReap()
	}
	return err
}

func (p *delegatedProcess) Wait() error {
	p.mu.Lock()
	command := p.command
	p.mu.Unlock()
	if command == nil {
		return ErrInvalid
	}
	p.waitOnce.Do(func() {
		p.waitErr = command.Wait()
	})
	p.releaseReap()
	return p.waitErr
}

func (p *delegatedProcess) Usage() (Usage, error) { return p.group.Usage() }

func (p *delegatedProcess) Finish(ctx context.Context) (Usage, error) {
	p.mu.Lock()
	p.finishing = true
	command := p.command
	p.mu.Unlock()
	usage, err := p.group.Finish(ctx)
	if command == nil {
		p.releaseReap()
	} else if err == nil {
		// Group cleanup has stopped every descendant. Joining the same Wait
		// owner also drains os/exec's pipe bookkeeping before custody returns.
		// A nonzero child exit is an execution result, not cleanup uncertainty.
		_ = p.Wait()
	}
	return usage, err
}
