// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"os/exec"
	"sync"
	"time"
)

// Process owns a command from Start through Wait and resource cleanup. After
// Start, callers must use Wait instead of exec.Cmd.Wait or os.Process.Wait.
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

// prepareProcess preserves Prepare's admission contract. On error, the caller
// still owns release. On success, release also waits for the command's sole
// Wait owner, including when Manager.Close cleans the group concurrently.
func prepareProcess(m *Manager, limits Limits, release func(), cleanupTimeout time.Duration) (Process, error) {
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
	return &delegatedProcess{group: job, releaseReap: reaped, cleanupTimeout: cleanupTimeout, quarantine: m.QuarantineOperation}, nil
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
	waitDone       chan struct{}
	waitErr        error
	releaseReap    func()
	cleanupTimeout time.Duration
	quarantine     func()
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
	done, err := p.startWait()
	if err != nil {
		return err
	}
	<-done
	return p.waitErr
}

func (p *delegatedProcess) startWait() (<-chan struct{}, error) {
	p.mu.Lock()
	command := p.command
	p.mu.Unlock()
	if command == nil {
		return nil, ErrInvalid
	}
	p.waitOnce.Do(func() {
		p.waitDone = make(chan struct{})
		// Waiting starts lazily: callers can still kill a pinned process group
		// before reaping the leader. No second waiter or adopted-child reaper
		// competes with this owner.
		go func() {
			p.waitErr = command.Wait()
			p.releaseReap()
			close(p.waitDone)
		}()
	})
	return p.waitDone, nil
}

func (p *delegatedProcess) Usage() (Usage, error) { return p.group.Usage() }

func (p *delegatedProcess) Finish(ctx context.Context) (Usage, error) {
	ctx, cancel := context.WithTimeout(ctx, p.cleanupTimeout)
	defer cancel()
	p.mu.Lock()
	p.finishing = true
	command := p.command
	p.mu.Unlock()
	usage, err := p.group.Finish(ctx)
	if command == nil {
		p.releaseReap()
	} else {
		// A parent-side pipe writer can still block after every descendant has
		// stopped. Keep the reap hold until the sole wait owner really returns;
		// a cleanup deadline must not turn that blocked writer into free capacity.
		done, waitErr := p.startWait()
		if waitErr != nil {
			p.quarantine()
			return usage, ErrQuarantined
		}
		if err != nil {
			// Keep a wait owner even when domain cleanup needs a later retry.
			return usage, err
		}
		select {
		case <-done:
			// A nonzero child exit is execution failure, not uncertain cleanup.
		case <-ctx.Done():
			select {
			case <-done:
				return usage, nil
			default:
			}
			p.quarantine()
			return usage, ErrQuarantined
		}
	}
	return usage, err
}
