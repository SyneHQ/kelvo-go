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
// Waiters are served FIFO per tenant among those that fit: freed capacity is
// granted to the tenant's earliest waiters whose connection (and tenant) have
// room, so a waiter held by a full connection never holds back the tenant's
// reads on other connections, and callers of other tenants are never held
// behind it. At most MaxWaiting callers wait (zero: unbounded); beyond that
// Acquire returns ErrQueueFull at once.
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
	ready              chan struct{} // receives once, when granted
	granted            bool          // guarded by mu
}

// Acquire waits until tenant and connection are both under their caps, or
// until ctx ends. The returned release is idempotent.
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
	// Every release grants to the waiters that fit, so none of the tenant's
	// queued waiters fits now: a caller that fits overtakes nobody.
	if c.underLocked(w) {
		c.takeLocked(w)
		c.mu.Unlock()
		return c.release(w), nil
	}
	if c.MaxWaiting > 0 && c.waiting >= c.MaxWaiting {
		c.mu.Unlock()
		return nil, ErrQueueFull
	}
	queue := c.queues[tenant]
	if queue == nil {
		queue = list.New()
		c.queues[tenant] = queue
	}
	element := queue.PushBack(w)
	c.waiting++
	c.mu.Unlock()
	select {
	case <-w.ready:
		return c.release(w), nil
	case <-ctx.Done():
		c.mu.Lock()
		if w.granted {
			c.mu.Unlock()
			c.release(w)() // hand it to the next waiter that fits
			return nil, ctx.Err()
		}
		c.dequeueLocked(queue, element)
		c.mu.Unlock()
		return nil, ctx.Err()
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

// grantLocked grants freed capacity to the tenant's waiters that fit, oldest
// first, until the tenant is at its cap.
func (c *Caps) grantLocked(tenant string) {
	queue := c.queues[tenant]
	if queue == nil {
		return
	}
	for e := queue.Front(); e != nil; {
		next := e.Next()
		w := e.Value.(*capsWaiter)
		if c.underLocked(w) {
			c.dequeueLocked(queue, e)
			c.takeLocked(w)
			w.granted = true
			w.ready <- struct{}{}
			c.wakeups++
		} else if c.MaxPerTenant != 0 && c.tenants[tenant] >= c.MaxPerTenant {
			return
		}
		e = next
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
			c.grantLocked(w.tenant)
		})
	}
}
