// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package admission

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

// ErrQueueFull refuses a caller without waiting: MaxWaiting callers already wait.
var ErrQueueFull = errors.New("capacity wait queue is full")

// Caps bounds concurrent work per tenant and per connection inside one
// workload class, so one tenant or one connection cannot take the whole class.
// Zero means no cap. A connection is always counted under its tenant: equal
// connection IDs of different tenants never share a count. An empty
// connection (a federated read over several) is bounded by the tenant cap only.
//
// Waiters are served FIFO per tenant: a later caller never overtakes an
// earlier one of the same tenant, and callers of other tenants are not held
// behind them. A release wakes only its tenant's first waiter. At most
// MaxWaiting callers wait (zero: unbounded); beyond that Acquire returns
// ErrQueueFull at once.
type Caps struct {
	MaxPerTenant     int
	MaxPerConnection int
	MaxWaiting       int

	mu          sync.Mutex
	tenants     map[string]int
	connections map[string]int
	queues      map[string]*list.List // tenant -> waiters, oldest first
	waiting     int
	wakeups     int // for tests: waiters woken
}

type capsWaiter struct {
	tenant, connection string
	ready              chan struct{}
}

// Acquire waits until tenant and connection are both under their caps and no
// earlier waiter of the tenant is still queued, or until ctx ends. The
// returned release is idempotent.
func (c *Caps) Acquire(ctx context.Context, tenant, connection string) (func(), error) {
	if c == nil || (c.MaxPerTenant == 0 && c.MaxPerConnection == 0) {
		return func() {}, nil
	}
	w := &capsWaiter{tenant: tenant, ready: make(chan struct{}, 1)}
	if connection != "" {
		w.connection = tenant + "\x00" + connection
	}
	c.mu.Lock()
	if c.tenants == nil {
		c.tenants, c.connections, c.queues = map[string]int{}, map[string]int{}, map[string]*list.List{}
	}
	queue := c.queues[tenant]
	if (queue == nil || queue.Len() == 0) && c.underLocked(w) {
		c.takeLocked(w)
		c.mu.Unlock()
		return c.release(w), nil
	}
	if c.MaxWaiting > 0 && c.waiting >= c.MaxWaiting {
		c.mu.Unlock()
		return nil, ErrQueueFull
	}
	if queue == nil {
		queue = list.New()
		c.queues[tenant] = queue
	}
	element := queue.PushBack(w)
	c.waiting++
	c.mu.Unlock()
	for {
		select {
		case <-w.ready:
		case <-ctx.Done():
			c.mu.Lock()
			head := queue.Front() == element
			c.dequeueLocked(queue, element)
			if head {
				c.wakeHeadLocked(tenant) // the next waiter may now proceed
			}
			c.mu.Unlock()
			return nil, ctx.Err()
		}
		c.mu.Lock()
		if queue.Front() == element && c.underLocked(w) {
			c.dequeueLocked(queue, element)
			c.takeLocked(w)
			c.wakeHeadLocked(tenant) // the new head may fit as well
			c.mu.Unlock()
			return c.release(w), nil
		}
		c.mu.Unlock()
	}
}

// Waiting is the number of callers waiting for a cap.
func (c *Caps) Waiting() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waiting
}

func (c *Caps) underLocked(w *capsWaiter) bool {
	return (c.MaxPerTenant == 0 || c.tenants[w.tenant] < c.MaxPerTenant) &&
		(c.MaxPerConnection == 0 || w.connection == "" || c.connections[w.connection] < c.MaxPerConnection)
}

func (c *Caps) takeLocked(w *capsWaiter) {
	c.tenants[w.tenant]++
	if w.connection != "" {
		c.connections[w.connection]++
	}
}

func (c *Caps) dequeueLocked(queue *list.List, element *list.Element) {
	queue.Remove(element)
	c.waiting--
	if queue.Len() == 0 {
		delete(c.queues, element.Value.(*capsWaiter).tenant)
	}
}

// wakeHeadLocked wakes the tenant's first waiter, and only it, when it fits.
func (c *Caps) wakeHeadLocked(tenant string) {
	queue := c.queues[tenant]
	if queue == nil || queue.Len() == 0 {
		return
	}
	head := queue.Front().Value.(*capsWaiter)
	if !c.underLocked(head) {
		return // a later release wakes it
	}
	select {
	case head.ready <- struct{}{}:
		c.wakeups++
	default:
	}
}

func (c *Caps) release(w *capsWaiter) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.tenants[w.tenant]--; c.tenants[w.tenant] == 0 {
				delete(c.tenants, w.tenant)
			}
			if w.connection != "" {
				if c.connections[w.connection]--; c.connections[w.connection] == 0 {
					delete(c.connections, w.connection)
				}
			}
			c.wakeHeadLocked(w.tenant)
		})
	}
}
