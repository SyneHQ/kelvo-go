package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

// SessionCache keeps opened adapter sessions warm between requests.
// Entries are keyed by the caller (tenant+team+connection+source revision), so a
// revision change misses the cache and the stale entry ages out. A session is
// checked out exclusively: one request at a time, never shared.
type SessionCache struct {
	TTL time.Duration // idle lifetime, e.g. 30m
	Max int           // max idle sessions; oldest evicted

	mu   sync.Mutex
	idle map[string]cached
	now  func() time.Time
}

type cached struct {
	session adapter.Session
	at      time.Time
}

// Get returns a warm session for key, or opens one. Call Put on success, or
// session.Close on any failure so a broken connection is never reused.
func (c *SessionCache) Get(ctx context.Context, key string, open func(context.Context) (adapter.Session, error)) (adapter.Session, error) {
	c.mu.Lock()
	e, ok := c.idle[key]
	delete(c.idle, key)
	c.mu.Unlock()
	if ok && c.clock().Sub(e.at) < c.TTL {
		return e.session, nil
	}
	if ok {
		_ = e.session.Close()
	}
	return open(ctx)
}

// Put returns a healthy session to the cache, evicting expired and surplus entries.
func (c *SessionCache) Put(key string, s adapter.Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.idle == nil {
		c.idle = map[string]cached{}
	}
	now := c.clock()
	if old, ok := c.idle[key]; ok {
		_ = old.session.Close()
	}
	c.idle[key] = cached{s, now}
	for k, e := range c.idle {
		if now.Sub(e.at) >= c.TTL {
			_ = e.session.Close()
			delete(c.idle, k)
		}
	}
	for len(c.idle) > c.Max {
		var oldest string
		var at time.Time
		for k, e := range c.idle {
			if oldest == "" || e.at.Before(at) {
				oldest, at = k, e.at
			}
		}
		_ = c.idle[oldest].session.Close()
		delete(c.idle, oldest)
	}
}

// Close drops every idle session.
func (c *SessionCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.idle {
		_ = e.session.Close()
		delete(c.idle, k)
	}
}

func (c *SessionCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}
