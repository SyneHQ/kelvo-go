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
	ErrDisabled = errors.New("workload class is disabled")
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
	// Classes supplies optional per-class ceilings. Interactive and refresh
	// inherit global capacity when absent. Export is disabled unless explicitly
	// configured. New copies this bounded map; later mutation has no effect.
	Classes map[Class]ClassLimits
}

type Request struct {
	Class Class
	// Background is retained for existing callers: without Class, false means
	// interactive and true means refresh. With Class, true is valid only for
	// ClassRefresh. Both exports and refreshes share protected-background limits.
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
	// Classes always contains the three fixed classes, including disabled export.
	// This map and Limits.Classes are independent copies owned by the caller.
	Classes map[Class]ClassSnapshot
}

type Pool struct {
	backgroundActive int
	backgroundUsed   Request
	mu               sync.Mutex
	limits           Limits
	used             Request
	active           int
	waiting          int
	classes          [classCount]ClassSnapshot
	queueHead        [classCount]*waiter
	queueTail        [classCount]*waiter
	draining         bool
	changed          chan struct{}
}

// Each class has its own FIFO so a refresh blocked by protected capacity cannot
// block interactive work. Only class heads are notified, not every queued call.
type waiter struct {
	previous, next *waiter
	ready          chan struct{}
}

// New rejects invalid capacity instead of silently permitting unlimited work.
func New(limits Limits) (*Pool, error) {
	if limits.MaxConcurrent < 1 || limits.MemoryBytes <= 0 || limits.ScratchBytes < 0 || limits.ReservedSlots < 0 || limits.ReservedSlots >= limits.MaxConcurrent || limits.ReservedMemoryBytes < 0 || limits.ReservedMemoryBytes > limits.MemoryBytes || limits.ReservedScratchBytes < 0 || limits.ReservedScratchBytes > limits.ScratchBytes {
		return nil, ErrInvalid
	}
	if len(limits.Classes) > classCount {
		return nil, ErrInvalid
	}
	limits.Classes = cloneClassLimits(limits.Classes)
	p := &Pool{limits: limits, changed: make(chan struct{})}
	global := ClassLimits{MaxConcurrent: limits.MaxConcurrent, MemoryBytes: limits.MemoryBytes, ScratchBytes: limits.ScratchBytes}
	p.classes[interactiveIndex] = ClassSnapshot{Enabled: true, Limits: global}
	p.classes[refreshIndex] = ClassSnapshot{Enabled: true, Limits: global}
	for class, ceiling := range limits.Classes {
		index, err := classIndex(class)
		if err != nil || !ceiling.validWithin(limits) {
			return nil, ErrInvalid
		}
		p.classes[index] = ClassSnapshot{Enabled: true, Limits: ceiling}
	}
	return p, nil
}

type Reservation struct {
	pool     *Pool
	request  Request
	class    int
	identity *reservationIdentity
}

type reservationIdentity struct {
	owner    *Reservation
	once     sync.Once
	released bool // protected by owner.pool.mu
}

// Owns reports whether this live reservation belongs to pool and charges the
// exact memory cost. It does not transfer ownership or extend its lifetime.
func (r *Reservation) Owns(pool *Pool, memoryBytes int64) bool {
	if r == nil || pool == nil || r.pool != pool || r.identity == nil || r.identity.owner != r {
		return false
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return !r.identity.released && r.request.MemoryBytes == memoryBytes
}

func (p *Pool) validate(r Request) (int, error) {
	class, err := requestClass(r)
	if err != nil {
		return 0, err
	}
	if r.MemoryBytes <= 0 || r.ScratchBytes < 0 {
		return 0, ErrInvalid
	}
	ceiling := p.classes[class]
	if !ceiling.Enabled {
		return 0, ErrDisabled
	}
	if r.MemoryBytes > p.limits.MemoryBytes || r.ScratchBytes > p.limits.ScratchBytes {
		return 0, ErrOversize
	}
	if r.MemoryBytes > ceiling.Limits.MemoryBytes || r.ScratchBytes > ceiling.Limits.ScratchBytes {
		return 0, ErrOversize
	}
	if class != interactiveIndex && (r.MemoryBytes > p.limits.MemoryBytes-p.limits.ReservedMemoryBytes || r.ScratchBytes > p.limits.ScratchBytes-p.limits.ReservedScratchBytes) {
		return 0, ErrOversize
	}
	return class, nil
}

func (p *Pool) fits(r Request, class int) bool {
	// Subtraction avoids signed overflow for capacities near MaxInt64.
	if p.active >= p.limits.MaxConcurrent || r.MemoryBytes > p.limits.MemoryBytes-p.used.MemoryBytes || r.ScratchBytes > p.limits.ScratchBytes-p.used.ScratchBytes {
		return false
	}
	ceiling := p.classes[class]
	if ceiling.Active >= ceiling.Limits.MaxConcurrent || r.MemoryBytes > ceiling.Limits.MemoryBytes-ceiling.Used.MemoryBytes || r.ScratchBytes > ceiling.Limits.ScratchBytes-ceiling.Used.ScratchBytes {
		return false
	}
	if class == interactiveIndex {
		return true
	}
	return p.backgroundActive < p.limits.MaxConcurrent-p.limits.ReservedSlots && r.MemoryBytes <= p.limits.MemoryBytes-p.limits.ReservedMemoryBytes-p.backgroundUsed.MemoryBytes && r.ScratchBytes <= p.limits.ScratchBytes-p.limits.ReservedScratchBytes-p.backgroundUsed.ScratchBytes
}

func (p *Pool) reserve(r Request, class int) *Reservation {
	p.active++
	p.used.MemoryBytes += r.MemoryBytes
	p.used.ScratchBytes += r.ScratchBytes
	p.classes[class].Active++
	p.classes[class].Used.MemoryBytes += r.MemoryBytes
	p.classes[class].Used.ScratchBytes += r.ScratchBytes
	if class != interactiveIndex {
		p.backgroundActive++
		p.backgroundUsed.MemoryBytes += r.MemoryBytes
		p.backgroundUsed.ScratchBytes += r.ScratchBytes
	}
	reservation := &Reservation{pool: p, request: r, class: class}
	reservation.identity = &reservationIdentity{owner: reservation}
	return reservation
}

// Acquire waits FIFO within its workload class. New calls cannot overtake a
// queued larger reservation. Classes compete for shared capacity independently;
// this is not tenant fairness or a guarantee of service across classes. Callers
// must bound upstream queues. Cancellation does not release an acquired
// reservation: actual execution cleanup must finish before Release is called.
func (p *Pool) Acquire(ctx context.Context, r Request) (*Reservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	class, err := p.validate(r)
	if err != nil {
		return nil, err
	}
	var queued *waiter
	defer func() {
		if queued != nil {
			p.removeWaiter(queued, class)
			p.wakeHeads()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if p.draining {
			return nil, ErrDraining
		}
		if p.queueHead[class] == queued && p.fits(r, class) {
			return p.reserve(r, class), nil
		}
		if queued == nil {
			queued = &waiter{previous: p.queueTail[class], ready: make(chan struct{}, 1)}
			if queued.previous == nil {
				p.queueHead[class] = queued
			} else {
				queued.previous.next = queued
			}
			p.queueTail[class] = queued
			p.waiting++
			p.classes[class].Waiting++
		}
		p.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-queued.ready:
		}
		p.mu.Lock()
	}
}

// TryAcquire does not enqueue or wait for capacity. It cannot bypass its class's FIFO.
func (p *Pool) TryAcquire(r Request) (*Reservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	class, err := p.validate(r)
	if err != nil {
		return nil, err
	}
	if p.draining {
		return nil, ErrDraining
	}
	if p.queueHead[class] != nil || !p.fits(r, class) {
		return nil, ErrBusy
	}
	return p.reserve(r, class), nil
}

func (p *Pool) removeWaiter(w *waiter, class int) {
	if w.previous == nil {
		p.queueHead[class] = w.next
	} else {
		w.previous.next = w.next
	}
	if w.next == nil {
		p.queueTail[class] = w.previous
	} else {
		w.next.previous = w.previous
	}
	p.waiting--
	p.classes[class].Waiting--
}

func (p *Pool) wakeHeads() {
	for _, head := range p.queueHead {
		if head != nil {
			select {
			case head.ready <- struct{}{}:
			default:
			}
		}
	}
}

func (p *Pool) notify() {
	close(p.changed)
	p.changed = make(chan struct{})
	p.wakeHeads()
}

// Release is safe to call concurrently and multiple times.
func (r *Reservation) Release() {
	if r == nil || r.identity == nil {
		return
	}
	// Copies share release idempotence, but only the original handle is an
	// ownership proof. Use its immutable request for all accounting.
	r = r.identity.owner
	r.identity.once.Do(func() {
		p := r.pool
		p.mu.Lock()
		defer p.mu.Unlock()
		r.identity.released = true
		p.active--
		p.used.MemoryBytes -= r.request.MemoryBytes
		p.used.ScratchBytes -= r.request.ScratchBytes
		p.classes[r.class].Active--
		p.classes[r.class].Used.MemoryBytes -= r.request.MemoryBytes
		p.classes[r.class].Used.ScratchBytes -= r.request.ScratchBytes
		if r.class != interactiveIndex {
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
	limits := p.limits
	limits.Classes = cloneClassLimits(limits.Classes)
	classes := map[Class]ClassSnapshot{
		ClassInteractive: p.classes[interactiveIndex],
		ClassExport:      p.classes[exportIndex],
		ClassRefresh:     p.classes[refreshIndex],
	}
	return Snapshot{BackgroundActive: p.backgroundActive, BackgroundUsed: p.backgroundUsed, Limits: limits, Used: p.used, Active: p.active, Waiting: p.waiting, Draining: p.draining, Classes: classes}
}
