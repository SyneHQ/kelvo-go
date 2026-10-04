//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authstate

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

const operationTimeout = 2 * time.Second

func fresh(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	deadline, hasDeadline := ctx.Deadline()
	return !hasDeadline || time.Now().Before(deadline)
}

// Do not rely on timely delivery of Context.Done: admission is authority, and
// even a custom context with delayed cancellation cannot extend its deadline.
func waitOperation(ctx context.Context, done <-chan struct{}) {
	deadline, _ := ctx.Deadline() // all operations carry the internal ceiling
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-done:
	case <-ctx.Done():
	case <-timer.C:
	}
}

type operation struct {
	ctx              context.Context
	candidate        Candidate
	done             chan struct{}
	complete, fenced bool // guarded by Store.mu
	err              error
}

// Store owns a single lifetime I/O worker. Call cancellation fences authority;
// it never closes descriptors under an outstanding syscall. At most one Admit
// is in flight; overlapping calls are rejected without creating more workers.
type Store struct {
	mu                         sync.Mutex
	scope                      Scope
	disk                       *diskState
	jobs                       chan *operation
	stop, done                 chan struct{}
	pending, closing, poisoned bool
	closeErr                   error
}

func Open(ctx context.Context, directory string, scope Scope) (*Store, error) {
	return openWithIO(ctx, directory, scope, nil, defaultIO())
}

// Initialize is offline and create-only. It never overwrites state or adopts a
// staging file. Open, in contrast, requires the existing directory/lock/state.
func Initialize(ctx context.Context, directory string, scope Scope, candidate Candidate) error {
	store, err := openWithIO(ctx, directory, scope, &candidate, defaultIO())
	if err != nil {
		return err
	}
	return store.Close(ctx)
}

func openWithIO(ctx context.Context, directory string, scope Scope, initial *Candidate, operations diskIO) (*Store, error) {
	if ctx == nil || !validDirectory(directory) {
		return nil, ErrUnavailable
	}
	copyScope, err := copyScope(scope)
	if err != nil {
		return nil, err
	}
	var candidate Candidate
	if initial != nil {
		candidate, err = copyCandidate(*initial, copyScope)
		if err != nil {
			return nil, err
		}
	}
	if !fresh(ctx) {
		return nil, ErrUncertain
	}
	bounded, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	op := &operation{ctx: bounded, candidate: candidate, done: make(chan struct{})}
	s := &Store{scope: copyScope, disk: &diskState{path: strings.Clone(directory), scope: copyScope, io: operations}, jobs: make(chan *operation, 1), stop: make(chan struct{}), done: make(chan struct{}), pending: true}
	go s.run(op, initial != nil)
	if err = s.await(op); err != nil {
		s.beginClose() // the worker owns cleanup even when no Store is returned
		return nil, err
	}
	return s, nil
}

func (s *Store) beginClose() {
	s.mu.Lock()
	if !s.closing {
		s.closing = true
		close(s.stop)
	}
	s.mu.Unlock()
}

func (s *Store) finish(op *operation, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if op.fenced || !fresh(op.ctx) || s.poisoned || s.closing {
		err, s.poisoned = ErrUncertain, true
	} else if errors.Is(err, ErrUncertain) {
		s.poisoned = true
	}
	op.complete, op.err, s.pending = true, err, false
	close(op.done)
}

func (s *Store) await(op *operation) error {
	waitOperation(op.ctx, op.done)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !fresh(op.ctx) || !op.complete || s.closing || s.poisoned {
		op.fenced, s.poisoned = true, true
		return ErrUncertain
	}
	return op.err
}

func (s *Store) run(startup *operation, initialize bool) {
	defer func() {
		err := s.disk.close()
		s.mu.Lock()
		if s.poisoned || err != nil {
			s.closeErr = ErrUncertain
		}
		s.mu.Unlock()
		close(s.done)
	}()
	s.mu.Lock()
	blocked := s.closing || s.poisoned || startup.fenced || !fresh(startup.ctx)
	s.mu.Unlock()
	if blocked {
		s.finish(startup, ErrUncertain)
		return
	}
	err := s.disk.start(initialize, startup.candidate)
	s.finish(startup, err)
	if err != nil {
		return
	}
	for {
		select {
		case <-s.stop:
			select {
			case op := <-s.jobs:
				s.finish(op, ErrUncertain)
			default:
			}
			return
		case op := <-s.jobs:
			s.mu.Lock()
			blocked := s.closing || s.poisoned || op.fenced || !fresh(op.ctx)
			s.mu.Unlock()
			if blocked {
				s.finish(op, ErrUncertain)
				continue
			}
			next, changed, err := advance(s.disk.state, op.candidate)
			if err == nil && changed {
				err = s.disk.publish(next)
			}
			s.finish(op, err)
		}
	}
}

func (s *Store) Admit(ctx context.Context, candidate Candidate) error {
	if s == nil || ctx == nil {
		return ErrUnavailable
	}
	s.mu.Lock()
	if s.closing || s.poisoned {
		s.mu.Unlock()
		return ErrUncertain
	}
	if s.pending {
		s.mu.Unlock()
		return ErrRejected
	}
	// Reserve the one slot before copying; overlapping calls cannot queue
	// unbounded candidate maps behind a blocked writer.
	s.pending = true
	s.mu.Unlock()
	candidate, err := copyCandidate(candidate, s.scope)
	if err != nil {
		s.mu.Lock()
		s.pending = false
		s.mu.Unlock()
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	op := &operation{ctx: bounded, candidate: candidate, done: make(chan struct{})}
	s.mu.Lock()
	if s.closing || s.poisoned || !fresh(bounded) {
		s.pending, s.poisoned = false, true
		s.mu.Unlock()
		return ErrUncertain
	}
	s.jobs <- op
	s.mu.Unlock()
	return s.await(op)
}

// Close is bounded even when a syscall is stuck. The worker retains all FDs
// until actual quiescence; later Close calls may observe completed cleanup.
func (s *Store) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		return ErrUncertain
	}
	s.beginClose()
	bounded, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	waitOperation(bounded, s.done)
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.done:
		return s.closeErr
	default:
	}
	s.poisoned = true
	return ErrUncertain
}
