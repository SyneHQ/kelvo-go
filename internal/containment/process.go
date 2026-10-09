// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// ErrLaunchUncertain means the OS may already have executed the child. Callers
// must not classify this error as no source effect or safe to replay.
var ErrLaunchUncertain = errors.New("native process may have started; source effects are unknown")

// Domain owns the native process boundary used by a worker. The application's
// existing admission pool owns query, refresh and export reservations.
type Domain interface {
	PrepareProcess(*Custody, Limits, func()) (Process, error)
	Err() error
	QuarantineOperation()
	SetOnQuarantine(func(error))
	MatchesConfig(Config) bool
	Close(context.Context) error
}

// CheckReservation applies the internal namespace backend's full-container
// admission contract before connection resolution. Delegated groups keep their
// existing independent process limits and do not require a pool binding.
func CheckReservation(domain Domain, custody *Custody) error {
	if checked, ok := domain.(interface{ checkReservation(*Custody) error }); ok {
		return checked.checkReservation(custody)
	}
	return nil
}

// IsNamespaceDomain identifies the internal PID-1 backend. Worker command
// construction must not install an os/exec context watcher for that backend.
func IsNamespaceDomain(domain Domain) bool {
	_, ok := domain.(interface{ namespaceDomain() })
	return ok
}

// ChildExit is native process termination, not proof of output or domain cleanup.
type ChildExit struct {
	Code   int
	Signal syscall.Signal
}

// Process owns a command from Start through Wait and resource cleanup. After
// Start, callers must use Wait instead of exec.Cmd.Wait or os.Process.Wait.
// Finish prevents further starts and retains custody until cleanup and reaping
// both complete. Usage belongs to the containment domain, including descendants.
// This interface does not enable a container executor or a PID-1 reaper.
type Process interface {
	Start(context.Context, *exec.Cmd) error
	Terminate(time.Duration) error
	Wait() error
	Exit() (ChildExit, bool)
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
	exit           ChildExit
	exited         bool
	releaseReap    func()
	cleanupTimeout time.Duration
	quarantine     func()
}

func (p *delegatedProcess) Start(ctx context.Context, command *exec.Cmd) error {
	p.mu.Lock()
	if ctx == nil || command == nil || p.startAttempted || p.finishing {
		p.mu.Unlock()
		return ErrInvalid
	}
	p.startAttempted = true
	err := ctx.Err()
	if err == nil {
		err = p.group.Start(command)
	}
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
			p.mu.Lock()
			p.exit, p.exited = commandExit(command)
			p.mu.Unlock()
			p.releaseReap()
			close(p.waitDone)
		}()
	})
	return p.waitDone, nil
}

func (p *delegatedProcess) Exit() (ChildExit, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exit, p.exited
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
