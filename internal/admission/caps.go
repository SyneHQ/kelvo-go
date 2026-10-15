// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package admission

import (
	"context"
	"sync"
)

// Caps bounds concurrent work per tenant and per connection inside one
// workload class, so one tenant or one connection cannot take the whole class.
// Zero means no cap. A connection is always counted under its tenant: equal
// connection IDs of different tenants never share a count.
type Caps struct {
	MaxPerTenant     int
	MaxPerConnection int

	mu          sync.Mutex
	tenants     map[string]int
	connections map[string]int
	changed     chan struct{}
}

// Acquire waits until tenant and connection are both under their caps, or ctx
// ends. The returned release is idempotent.
func (c *Caps) Acquire(ctx context.Context, tenant, connection string) (func(), error) {
	if c == nil || (c.MaxPerTenant == 0 && c.MaxPerConnection == 0) {
		return func() {}, nil
	}
	key := tenant + "\x00" + connection
	c.mu.Lock()
	if c.tenants == nil {
		c.tenants, c.connections, c.changed = map[string]int{}, map[string]int{}, make(chan struct{})
	}
	for (c.MaxPerTenant > 0 && c.tenants[tenant] >= c.MaxPerTenant) || (c.MaxPerConnection > 0 && c.connections[key] >= c.MaxPerConnection) {
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		c.mu.Lock()
	}
	c.tenants[tenant]++
	c.connections[key]++
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.tenants[tenant]--; c.tenants[tenant] == 0 {
				delete(c.tenants, tenant)
			}
			if c.connections[key]--; c.connections[key] == 0 {
				delete(c.connections, key)
			}
			close(c.changed)
			c.changed = make(chan struct{})
		})
	}, nil
}
