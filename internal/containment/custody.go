// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package containment owns optional operating-system process-tree limits and
// coordinates reservation release with verified process cleanup.
package containment

import (
	"context"
	"errors"
	"sync"
)

var ErrCompleted = errors.New("containment reservation operation is complete")

// Custody releases an operation's reservation only after the caller completes
// all of its work AND every contained process tree is proven empty. A refresh
// owner calls Complete after publication and cleanup, not when its source query
// exits. It must not separately release the reservation after ownership transfer.
type Custody struct {
	mu        sync.Mutex
	held      int
	completed bool
	released  bool
	release   func()
}

type CustodyState struct {
	Held      int
	Completed bool
	Released  bool
}

func NewCustody(release func()) (*Custody, error) {
	if release == nil {
		return nil, errors.New("containment reservation release is required")
	}
	return &Custody{release: release}, nil
}

// Hold returns an idempotent callback for one process tree. The caller owns
// this callback until Prepare succeeds; on Prepare failure it must invoke the
// callback itself because no process can have been attached. After success only
// the Job may invoke it, following verified-empty group and ownership cleanup.
func (c *Custody) Hold() (func(), error) {
	c.mu.Lock()
	if c.completed {
		c.mu.Unlock()
		return nil, ErrCompleted
	}
	c.held++
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			c.held--
			release := c.maybeRelease()
			c.mu.Unlock()
			if release != nil {
				release()
			}
		})
	}, nil
}

func (c *Custody) maybeRelease() func() {
	if c.completed && c.held == 0 && !c.released {
		c.released = true
		return c.release
	}
	return nil
}

// Complete is safe to call repeatedly. It cannot release a reservation retained
// by a quarantined process group; the corresponding Hold remains outstanding.
func (c *Custody) Complete() {
	c.mu.Lock()
	c.completed = true
	release := c.maybeRelease()
	c.mu.Unlock()
	if release != nil {
		release()
	}
}

func (c *Custody) State() CustodyState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CustodyState{Held: c.held, Completed: c.completed, Released: c.released}
}

type custodyContextKey struct{}

// WithCustody carries an individual operation's custody through a shared
// executor factory without storing mutable per-request ownership in the factory.
func WithCustody(ctx context.Context, custody *Custody) context.Context {
	return context.WithValue(ctx, custodyContextKey{}, custody)
}

func FromContext(ctx context.Context) *Custody {
	custody, _ := ctx.Value(custodyContextKey{}).(*Custody)
	return custody
}
