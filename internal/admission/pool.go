// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package admission accounts for conservative, configured process reservations.
// It is not an RSS or filesystem quota enforcer. Callers must include native
// memory, Arrow buffers and scratch overhead, and leave runtime headroom outside
// the pool's capacity. All competing work must share the same pool.
package admission

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrInvalid  = errors.New("invalid resource reservation")
	ErrOversize = errors.New("resource reservation exceeds capacity")
	ErrBusy     = errors.New("resource capacity unavailable")
	ErrDraining = errors.New("resource admission is draining")
)

// Limits are usable workload capacity after baseline runtime headroom.
// Zero scratch capacity disables jobs requiring temporary storage.
type Limits struct {
	MaxConcurrent int
	MemoryBytes   int64
	ScratchBytes  int64
	// Protected capacity is unavailable to background work. Interactive work
	// may use all idle capacity; these are not separate preallocated pools.
	ReservedSlots        int
	ReservedMemoryBytes  int64
	ReservedScratchBytes int64
}

type Request struct {
	Background   bool
	MemoryBytes  int64
	ScratchBytes int64
}

// Snapshot contains aggregate accounting only; no query or tenant identifiers.
type Snapshot struct {
	BackgroundActive int
	BackgroundUsed   Request
	Limits           Limits
	Used             Request
	Active           int
	Waiting          int
	Draining         bool
}

type Pool struct {
	backgroundActive int
	backgroundUsed   Request
	mu               sync.Mutex
	limits           Limits
	used             Request
	active           int
	waiting          int
	draining         bool
	changed          chan struct{}
}

// New rejects invalid capacity instead of silently permitting unlimited work.
func New(limits Limits) (*Pool, error) {
	if limits.MaxConcurrent < 1 || limits.MemoryBytes <= 0 || limits.ScratchBytes < 0 || limits.ReservedSlots < 0 || limits.ReservedSlots >= limits.MaxConcurrent || limits.ReservedMemoryBytes < 0 || limits.ReservedMemoryBytes > limits.MemoryBytes || limits.ReservedScratchBytes < 0 || limits.ReservedScratchBytes > limits.ScratchBytes {
		return nil, ErrInvalid
	}
	return &Pool{limits: limits, changed: make(chan struct{})}, nil
}

type Reservation struct {
	pool    *Pool
	request Request
	once    sync.Once
}

func (p *Pool) validate(r Request) error {
	if r.MemoryBytes <= 0 || r.ScratchBytes < 0 {
		return ErrInvalid
	}
	if r.MemoryBytes > p.limits.MemoryBytes || r.ScratchBytes > p.limits.ScratchBytes {
		return ErrOversize
	}
	if r.Background && (r.MemoryBytes > p.limits.MemoryBytes-p.limits.ReservedMemoryBytes || r.ScratchBytes > p.limits.ScratchBytes-p.limits.ReservedScratchBytes) {
		return ErrOversize
	}
	return nil
}

func (p *Pool) fits(r Request) bool {
	// Subtraction avoids signed overflow for capacities near MaxInt64.
	if p.active >= p.limits.MaxConcurrent || r.MemoryBytes > p.limits.MemoryBytes-p.used.MemoryBytes || r.ScratchBytes > p.limits.ScratchBytes-p.used.ScratchBytes {
		return false
	}
	if !r.Background {
		return true
	}
	return p.backgroundActive < p.limits.MaxConcurrent-p.limits.ReservedSlots && r.MemoryBytes <= p.limits.MemoryBytes-p.limits.ReservedMemoryBytes-p.backgroundUsed.MemoryBytes && r.ScratchBytes <= p.limits.ScratchBytes-p.limits.ReservedScratchBytes-p.backgroundUsed.ScratchBytes
}

func (p *Pool) reserve(r Request) *Reservation {
	p.active++
	p.used.MemoryBytes += r.MemoryBytes
	p.used.ScratchBytes += r.ScratchBytes
	if r.Background {
		p.backgroundActive++
		p.backgroundUsed.MemoryBytes += r.MemoryBytes
		p.backgroundUsed.ScratchBytes += r.ScratchBytes
	}
	return &Reservation{pool: p, request: r}
}

// Acquire waits for capacity or cancellation. No ordering/fairness is promised;
// callers must bound upstream queues. Cancellation does not release an acquired
// reservation: actual execution cleanup must finish before Release is called.
func (p *Pool) Acquire(ctx context.Context, r Request) (*Reservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.validate(r); err != nil {
		return nil, err
	}
	waiting := false
	defer func() {
		if waiting {
			p.waiting--
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if p.draining {
			return nil, ErrDraining
		}
		if p.fits(r) {
			return p.reserve(r), nil
		}
		if !waiting {
			p.waiting++
			waiting = true
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		p.mu.Lock()
	}
}

// TryAcquire does not allocate or wait in an internal queue.
func (p *Pool) TryAcquire(r Request) (*Reservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.validate(r); err != nil {
		return nil, err
	}
	if p.draining {
		return nil, ErrDraining
	}
	if !p.fits(r) {
		return nil, ErrBusy
	}
	return p.reserve(r), nil
}

func (p *Pool) notify() { close(p.changed); p.changed = make(chan struct{}) }

// Release is safe to call concurrently and multiple times.
func (r *Reservation) Release() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		p := r.pool
		p.mu.Lock()
		defer p.mu.Unlock()
		p.active--
		p.used.MemoryBytes -= r.request.MemoryBytes
		p.used.ScratchBytes -= r.request.ScratchBytes
		if r.request.Background {
			p.backgroundActive--
			p.backgroundUsed.MemoryBytes -= r.request.MemoryBytes
			p.backgroundUsed.ScratchBytes -= r.request.ScratchBytes
		}
		p.notify()
	})
}

// Drain permanently stops admission and wakes queued callers. Existing work
// retains its resources and is not canceled. Use Wait with a grace deadline,
// then cancel remaining execution through its existing owner if necessary.
func (p *Pool) Drain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.draining {
		p.draining = true
		p.notify()
	}
}

// Wait waits for reservations to be released. Call Drain first if new work must
// not be admitted while waiting. A timeout never resets resource accounting.
func (p *Pool) Wait(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.active != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		p.mu.Lock()
	}
	return nil
}

func (p *Pool) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Snapshot{BackgroundActive: p.backgroundActive, BackgroundUsed: p.backgroundUsed, Limits: p.limits, Used: p.used, Active: p.active, Waiting: p.waiting, Draining: p.draining}
}
