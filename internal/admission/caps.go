// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package admission

import (
	"context"
	"slices"
	"sync"
)

// Caps bounds concurrent work per tenant and per connection inside one
// workload class, so one tenant or one connection cannot take the whole class.
// Zero means no cap. A connection is always counted under its tenant: equal
// connection IDs of different tenants never share a count.
//
// Waiters are served FIFO per tenant and per connection: a later caller never
// overtakes an earlier one for the same tenant or connection. Callers of other
// tenants are not held behind them.
type Caps struct {
	MaxPerTenant     int
	MaxPerConnection int

	mu          sync.Mutex
	tenants     map[string]int
	connections map[string]int
	waiters     []*capsWaiter
	changed     chan struct{}
}

type capsWaiter struct{ tenant, connection string }

// Acquire waits until tenant and connection are both under their caps and no
// earlier waiter for either is still queued, or until ctx ends. The returned
// release is idempotent.
func (c *Caps) Acquire(ctx context.Context, tenant, connection string) (func(), error) {
	if c == nil || (c.MaxPerTenant == 0 && c.MaxPerConnection == 0) {
		return func() {}, nil
	}
	w := &capsWaiter{tenant: tenant, connection: tenant + "\x00" + connection}
	c.mu.Lock()
	if c.tenants == nil {
		c.tenants, c.connections, c.changed = map[string]int{}, map[string]int{}, make(chan struct{})
	}
	c.waiters = append(c.waiters, w)
	for !c.admissibleLocked(w) {
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			c.mu.Lock()
			c.removeLocked(w) // may unblock a later waiter for the same key
			c.mu.Unlock()
			return nil, ctx.Err()
		}
		c.mu.Lock()
	}
	c.removeLocked(w)
	c.tenants[w.tenant]++
	c.connections[w.connection]++
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.tenants[w.tenant]--; c.tenants[w.tenant] == 0 {
				delete(c.tenants, w.tenant)
			}
			if c.connections[w.connection]--; c.connections[w.connection] == 0 {
				delete(c.connections, w.connection)
			}
			c.broadcastLocked()
		})
	}, nil
}

func (c *Caps) admissibleLocked(w *capsWaiter) bool {
	if (c.MaxPerTenant > 0 && c.tenants[w.tenant] >= c.MaxPerTenant) || (c.MaxPerConnection > 0 && c.connections[w.connection] >= c.MaxPerConnection) {
		return false
	}
	for _, earlier := range c.waiters {
		if earlier == w {
			return true
		}
		if earlier.tenant == w.tenant || earlier.connection == w.connection {
			return false
		}
	}
	return true
}

func (c *Caps) removeLocked(w *capsWaiter) {
	if i := slices.Index(c.waiters, w); i >= 0 {
		c.waiters = slices.Delete(c.waiters, i, i+1)
		c.broadcastLocked()
	}
}

func (c *Caps) broadcastLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}
