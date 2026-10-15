package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

// SessionCache keeps one opened adapter session per key (tenant+team+connection+
// source revision) shared by all concurrent requests. Concurrent first requests
// open once; the others wait. A session that did not accept KeepOpen is
// single-use: it is handed to the opener only and never cached.
type SessionCache struct {
	TTL time.Duration // idle lifetime, e.g. 30m
	Max int           // max cached sessions; oldest evicted

	mu      sync.Mutex
	entries map[string]*cacheEntry
	now     func() time.Time
}

type cacheEntry struct {
	ready   chan struct{} // closed once open finished
	session adapter.Session
	err     error
	at      time.Time
}

// Get returns the shared session for key, opening it if needed. For a
// non-reusable session the caller owns it and must Close it.
func (c *SessionCache) Get(ctx context.Context, key string, open func(context.Context) (adapter.Session, error)) (adapter.Session, error) {
	for {
		c.mu.Lock()
		if c.entries == nil {
			c.entries = map[string]*cacheEntry{}
		}
		e := c.entries[key]
		if e != nil && e.isReady() && c.clock().Sub(e.at) >= c.TTL {
			delete(c.entries, key)
			_ = e.session.Close()
			e = nil
		}
		if e == nil {
			break // still locked: we are the opener
		}
		e.at = c.clock()
		c.mu.Unlock()
		select {
		case <-e.ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if e.err != nil {
			return nil, e.err
		}
		if e.session != nil {
			return e.session, nil
		}
		// opener's session was single-use: open our own
	}
	e := &cacheEntry{ready: make(chan struct{}), at: c.clock()}
	c.entries[key] = e
	c.evictLocked(key)
	c.mu.Unlock()

	s, err := open(ctx)
	_, reusable := s.(reusableSession)
	c.mu.Lock()
	if err != nil || !reusable {
		delete(c.entries, key)
		e.err = err
	} else {
		e.session = s
	}
	c.mu.Unlock()
	close(e.ready)
	return s, err
}

func (e *cacheEntry) isReady() bool {
	select {
	case <-e.ready:
		return true
	default:
		return false
	}
}

// evictLocked drops the oldest ready entries beyond Max (never keep).
func (c *SessionCache) evictLocked(keep string) {
	for len(c.entries) > c.Max {
		var oldest string
		var at time.Time
		for k, e := range c.entries {
			if k != keep && e.isReady() && (oldest == "" || e.at.Before(at)) {
				oldest, at = k, e.at
			}
		}
		if oldest == "" {
			return
		}
		_ = c.entries[oldest].session.Close()
		delete(c.entries, oldest)
	}
}

// Close drops every ready session.
func (c *SessionCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.entries {
		if e.isReady() && e.session != nil {
			_ = e.session.Close()
		}
		delete(c.entries, k)
	}
}

func (c *SessionCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}
