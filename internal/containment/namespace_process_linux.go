//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
)

// NamespacePolicy reserves the entire fixed container for one operation.
// Native is the expected engine/allocator budget, not an independently enforced
// child cgroup. ParentMemoryBytes leaves space inside the hard container cap for
// PID 1 and IPC. The kernel enforces Container across every resident and cache.
type NamespacePolicy struct {
	Container         Limits
	Native            Limits
	ParentMemoryBytes int64
	CancellationGrace time.Duration
	CleanupTimeout    time.Duration
}

func (p NamespacePolicy) validate() error {
	if p.Container.validate() != nil || p.Native.validate() != nil ||
		p.ParentMemoryBytes < 16<<20 || p.ParentMemoryBytes > p.Container.MemoryBytes ||
		p.Native.MemoryBytes > p.Container.MemoryBytes-p.ParentMemoryBytes ||
		!limitsWithin(p.Native, p.Container) || p.CancellationGrace < 0 || p.CancellationGrace > 30*time.Second ||
		p.CleanupTimeout < 100*time.Millisecond || p.CleanupTimeout > 30*time.Second {
		return ErrInvalid
	}
	return nil
}

// NamespaceDomain connects physical PID-1 execution to the worker's existing
// shared admission pool. It is an internal qualification backend. No public
// configuration selects it until crash recovery and external fencing pass.
type NamespaceDomain struct {
	mu           sync.Mutex
	policy       NamespacePolicy
	pool         *admission.Pool
	supervisor   *NamespaceSupervisor
	active       *namespaceProcess
	onQuarantine func(error)
	draining     atomic.Bool
	closeOnce    sync.Once
	closeDone    chan struct{}
	closeErr     error
}

func OpenNamespaceDomain(policy NamespacePolicy, pool *admission.Pool) (*NamespaceDomain, error) {
	if policy.validate() != nil || pool == nil {
		return nil, ErrInvalid
	}
	snapshot := pool.Snapshot()
	if snapshot.Limits.MaxConcurrent != 1 || snapshot.Limits.MemoryBytes != policy.Container.MemoryBytes ||
		snapshot.Limits.ReservedMemoryBytes != 0 || snapshot.Active != 0 || snapshot.Waiting != 0 || snapshot.Draining {
		return nil, ErrInvalid
	}
	supervisor, err := OpenNamespaceSupervisor(policy.Container, policy.CleanupTimeout)
	if err != nil {
		return nil, err
	}
	observed, err := supervisor.limits.Check(policy.Container)
	if err != nil || observed.UpperBounds != policy.Container {
		return nil, errors.Join(ErrInvalid, supervisor.Close(context.Background()))
	}
	return &NamespaceDomain{policy: policy, pool: pool, supervisor: supervisor, closeDone: make(chan struct{})}, nil
}

func (*NamespaceDomain) namespaceDomain() {}

// A namespace backend must never match a delegated-directory configuration.
func (*NamespaceDomain) MatchesConfig(Config) bool { return false }

func (d *NamespaceDomain) Err() error {
	if d == nil || d.supervisor == nil {
		return ErrInvalid
	}
	if d.draining.Load() || d.pool.Snapshot().Draining || d.supervisor.State().Draining {
		return ErrDraining
	}
	return nil
}

func (d *NamespaceDomain) QuarantineOperation() {
	if d == nil || !d.draining.CompareAndSwap(false, true) {
		return
	}
	d.pool.Drain()
	d.mu.Lock()
	callback := d.onQuarantine
	d.mu.Unlock()
	if callback != nil {
		callback(ErrQuarantined)
	}
}

func (d *NamespaceDomain) SetOnQuarantine(callback func(error)) {
	d.mu.Lock()
	d.onQuarantine = callback
	draining := d.draining.Load()
	d.mu.Unlock()
	if draining && callback != nil {
		callback(ErrQuarantined)
	}
}

func (d *NamespaceDomain) checkReservation(custody *Custody) error {
	if err := d.Err(); err != nil {
		return err
	}
	if !custody.ownsReservation(d.pool, d.policy.Container.MemoryBytes) {
		return ErrInvalid
	}
	observed, err := d.supervisor.limits.Check(d.policy.Container)
	if err != nil || observed.UpperBounds != d.policy.Container {
		d.QuarantineOperation()
		return ErrOwnership
	}
	return nil
}

func (d *NamespaceDomain) PrepareProcess(custody *Custody, native Limits, release func()) (Process, error) {
	if d == nil || release == nil || native != d.policy.Native {
		return nil, ErrInvalid
	}
	if err := d.checkReservation(custody); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.draining.Load() {
		return nil, ErrDraining
	}
	// Native exit is insufficient. The previous operation must release its
	// output, publication and scratch custody before this domain is reusable.
	if d.active != nil && !d.active.custody.State().Released {
		return nil, ErrUnavailable
	}
	p := &namespaceProcess{owner: d, custody: custody, release: release,
		launchDone: make(chan struct{}), cancelDone: make(chan struct{}),
		terminateDone: make(chan struct{}), finishDone: make(chan struct{})}
	d.active = p
	return p, nil
}

func (d *NamespaceDomain) Close(ctx context.Context) error {
	if d == nil || ctx == nil || d.closeDone == nil {
		return ErrInvalid
	}
	d.draining.Store(true)
	d.pool.Drain()
	d.closeOnce.Do(func() {
		go func() {
			d.mu.Lock()
			active := d.active
			d.mu.Unlock()
			var finishErr error
			if active != nil {
				active.requestTermination(0)
				_, finishErr = active.Finish(context.Background())
				// A timed-out launch/usage owner still owns these descriptors.
				// Do not close the supervisor's limits underneath that owner.
				<-active.finishDone
				finishErr = errors.Join(finishErr, active.finishErr)
			}
			d.closeErr = errors.Join(finishErr, d.supervisor.Close(context.Background()))
			close(d.closeDone)
		}()
	})
	ctx, cancel := context.WithTimeout(ctx, d.policy.CancellationGrace+2*d.policy.CleanupTimeout)
	defer cancel()
	select {
	case <-d.closeDone:
		return d.closeErr
	case <-ctx.Done():
		return ErrQuarantined
	}
}

type namespaceProcess struct {
	mu             sync.Mutex
	owner          *NamespaceDomain
	custody        *Custody
	release        func()
	child          *SupervisedChild
	startAttempted bool
	finishing      bool
	launchDone     chan struct{}
	stopCancel     func() bool
	cancelDone     chan struct{}
	terminateOnce  sync.Once
	terminateDone  chan struct{}
	terminateErr   error
	finishOnce     sync.Once
	finishDone     chan struct{}
	finishErr      error
	usage          Usage
}

func namespaceCommand(command *exec.Cmd) (ChildSpec, error) {
	if command == nil || command.Process != nil || command.ProcessState != nil || command.Err != nil ||
		command.Cancel != nil || command.WaitDelay != 0 || command.SysProcAttr != nil {
		return ChildSpec{}, ErrInvalid
	}
	files := make([]*os.File, 3, 3+len(command.ExtraFiles))
	var ok bool
	if files[0], ok = command.Stdin.(*os.File); !ok || files[0] == nil {
		return ChildSpec{}, ErrInvalid
	}
	if files[1], ok = command.Stdout.(*os.File); !ok || files[1] == nil {
		return ChildSpec{}, ErrInvalid
	}
	if files[2], ok = command.Stderr.(*os.File); !ok || files[2] == nil {
		return ChildSpec{}, ErrInvalid
	}
	spec := ChildSpec{Path: command.Path, Args: command.Args, Env: command.Env, Dir: command.Dir,
		Files: append(files, command.ExtraFiles...)}
	if !validChildSpec(spec) {
		return ChildSpec{}, ErrInvalid
	}
	return spec, nil
}

func (p *namespaceProcess) Start(ctx context.Context, command *exec.Cmd) error {
	if ctx == nil {
		return ErrInvalid
	}
	p.mu.Lock()
	if p.startAttempted || p.finishing {
		p.mu.Unlock()
		return ErrInvalid
	}
	p.startAttempted = true
	p.mu.Unlock()
	// No lock spans ForkExec. A cleanup caller can return at its deadline
	// while this sole launch owner retains the operation's capacity.
	spec, err := namespaceCommand(command)
	if err == nil {
		err = p.owner.checkReservation(p.custody)
	}
	var child *SupervisedChild
	if err == nil {
		child, err = p.owner.supervisor.Spawn(ctx, spec)
	}
	p.mu.Lock()
	p.child = child
	if err == nil {
		p.stopCancel = context.AfterFunc(ctx, func() {
			p.requestTermination(p.owner.policy.CancellationGrace)
			close(p.cancelDone)
		})
	} else {
		close(p.cancelDone)
	}
	close(p.launchDone)
	p.mu.Unlock()
	return err
}

func (p *namespaceProcess) requestTermination(grace time.Duration) {
	p.mu.Lock()
	p.finishing = true
	if !p.startAttempted {
		p.startAttempted = true
		close(p.launchDone)
		close(p.cancelDone)
	}
	p.mu.Unlock()
	p.terminateOnce.Do(func() {
		go func() {
			// An in-flight ForkExec keeps this owner alive after caller timeout.
			// Once launch returns, cleanup still kills and reaps its descendants.
			<-p.launchDone
			p.mu.Lock()
			child := p.child
			p.mu.Unlock()
			if child != nil && grace > 0 {
				_ = child.Signal(syscall.SIGTERM)
				ctx, cancel := context.WithTimeout(context.Background(), grace)
				_, _ = child.Wait(ctx)
				cancel()
			}
			ctx, cancel := context.WithTimeout(context.Background(), p.owner.policy.CleanupTimeout)
			defer cancel()
			p.terminateErr = p.owner.supervisor.quiesce(ctx)
			if p.terminateErr != nil {
				p.owner.QuarantineOperation()
			}
			close(p.terminateDone)
		}()
	})
}

func (p *namespaceProcess) Terminate(grace time.Duration) error {
	if grace < 0 || grace > 30*time.Second {
		return ErrInvalid
	}
	p.requestTermination(grace)
	timer := time.NewTimer(grace + p.owner.policy.CleanupTimeout)
	defer timer.Stop()
	select {
	case <-p.terminateDone:
		return p.terminateErr
	case <-timer.C:
		p.owner.QuarantineOperation()
		return ErrQuarantined
	}
}

var errNativeExit = errors.New("native child exited unsuccessfully")

func (p *namespaceProcess) Wait() error {
	p.mu.Lock()
	child := p.child
	p.mu.Unlock()
	if child == nil {
		return ErrInvalid
	}
	select {
	case <-child.done:
	case <-p.terminateDone:
		if p.terminateErr != nil {
			return p.terminateErr // No native exit is implied by cleanup failure.
		}
		<-child.done
	}
	if child.exit.Code != 0 || child.exit.Signal != 0 {
		return errNativeExit
	}
	return nil
}

func (p *namespaceProcess) Exit() (ChildExit, bool) {
	p.mu.Lock()
	child := p.child
	p.mu.Unlock()
	if child != nil {
		select {
		case <-child.done:
			return child.exit, true
		default:
		}
	}
	return ChildExit{}, false
}

func (p *namespaceProcess) Usage() (Usage, error) {
	return p.owner.supervisor.limits.Usage(p.owner.policy.Container)
}

func (p *namespaceProcess) Finish(ctx context.Context) (Usage, error) {
	if ctx == nil {
		return Usage{}, ErrInvalid
	}
	p.requestTermination(0)
	p.finishOnce.Do(func() {
		go func() {
			<-p.launchDone
			p.mu.Lock()
			stop := p.stopCancel
			p.mu.Unlock()
			if stop != nil && stop() {
				close(p.cancelDone)
			}
			<-p.cancelDone
			<-p.terminateDone
			p.finishErr = p.terminateErr
			if p.finishErr == nil {
				p.usage, p.finishErr = p.Usage()
			}
			if p.finishErr == nil {
				p.release()
			} else {
				p.owner.QuarantineOperation()
			}
			close(p.finishDone)
		}()
	})
	ctx, cancel := context.WithTimeout(ctx, p.owner.policy.CleanupTimeout)
	defer cancel()
	select {
	case <-p.finishDone:
		return p.usage, p.finishErr
	case <-ctx.Done():
		select {
		case <-p.finishDone:
			return p.usage, p.finishErr
		default:
		}
		p.owner.QuarantineOperation()
		return Usage{}, ErrQuarantined
	}
}
