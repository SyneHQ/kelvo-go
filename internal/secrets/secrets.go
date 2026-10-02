// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package secrets resolves explicitly configured file secrets in the trusted
// parent process. Values never contain a provider credential or enter catalogs.
package secrets

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"time"
)

const (
	MaxKeys       = 128
	MaxValueBytes = 16 << 10
	MaxCacheBytes = MaxKeys * MaxValueBytes
	MaxTTL        = 5 * time.Minute
)

var (
	ErrInvalid     = errors.New("invalid file secret provider configuration")
	ErrUnavailable = errors.New("configured secret is unavailable")
	ErrClosed      = errors.New("secret provider is closed")
	ErrUnsupported = errors.New("file secret provider is unsupported on this platform")
)

type Config struct {
	Files map[string]string `yaml:"files"`
	TTL   time.Duration     `yaml:"ttl,omitempty"`
}
type cacheEntry struct {
	owner   *lookup
	value   []byte
	expires time.Time
}
type lookup struct {
	done      chan struct{}
	waiters   int
	complete  bool
	delivered bool
	value     []byte
	err       error
}
type Provider struct {
	mu     sync.Mutex
	files  map[string]string
	ttl    time.Duration
	cache  map[string]cacheEntry
	calls  map[string]*lookup
	closed bool
	ctx    context.Context
	cancel context.CancelFunc
	read   func(context.Context, string) ([]byte, error)
}

// New validates configuration without reading any secret. The application must
// separately validate environment-reference keys against its credential policy.
// Paths are absolute and symlink-free; reads require private regular files owned
// by the current effective user. TTL zero coalesces overlapping reads but never
// retains a completed value after all of its waiting callers return.
func New(config Config) (*Provider, error) {
	if err := supported(); err != nil {
		return nil, err
	}
	if len(config.Files) == 0 || len(config.Files) > MaxKeys || config.TTL < 0 || config.TTL > MaxTTL {
		return nil, ErrInvalid
	}
	files := make(map[string]string, len(config.Files))
	for key, path := range config.Files {
		if key == "" || len(key) > 256 || path == "" || len(path) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
			return nil, ErrInvalid
		}
		files[key] = path
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Provider{files: files, ttl: config.TTL, cache: map[string]cacheEntry{}, calls: map[string]*lookup{}, ctx: ctx, cancel: cancel, read: readPrivateFile}, nil
}
func wipe(value []byte) { clear(value) }

// Resolve returns configured=true even when reading that configured key fails:
// callers must never fall back to an old environment value on provider failure.
// Unknown keys require no filesystem I/O. Cancellation releases only that
// waiter, so one caller cannot cancel another caller's coalesced lookup.
func (p *Provider) Resolve(ctx context.Context, key string) (string, bool, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return "", false, ErrClosed
	}
	path, known := p.files[key]
	if !known {
		p.mu.Unlock()
		return "", false, nil
	}
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return "", true, err
	}
	if cached, ok := p.cache[key]; ok {
		if time.Now().Before(cached.expires) {
			cached.owner.delivered = true
			value := string(cached.value)
			p.mu.Unlock()
			return value, true, nil
		}
		wipe(cached.value)
		delete(p.cache, key)
	}
	flight := p.calls[key]
	if flight != nil && flight.complete {
		delete(p.calls, key)
		flight = nil
	}
	if flight == nil {
		flight = &lookup{done: make(chan struct{})}
		p.calls[key] = flight
		flight.waiters++
		go p.load(key, path, flight)
	} else {
		flight.waiters++
	}
	p.mu.Unlock()
	select {
	case <-flight.done:
	case <-ctx.Done():
	case <-p.ctx.Done():
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	defer func() {
		flight.waiters--
		if flight.complete && flight.waiters == 0 {
			if !flight.delivered {
				if entry, ok := p.cache[key]; ok && entry.owner == flight {
					wipe(entry.value)
					delete(p.cache, key)
				}
			}
			wipe(flight.value)
			flight.value = nil
			if p.calls[key] == flight {
				delete(p.calls, key)
			}
		}
	}()
	if err := ctx.Err(); err != nil {
		return "", true, err
	}
	if p.closed {
		return "", true, ErrClosed
	}
	if !flight.complete {
		return "", true, ErrUnavailable
	}
	if flight.err != nil {
		return "", true, flight.err
	}
	flight.delivered = true
	return string(flight.value), true, nil
}
func (p *Provider) load(key, path string, flight *lookup) {
	value, err := p.read(p.ctx, path)
	if err != nil {
		wipe(value)
		value = nil
		err = ErrUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		wipe(value)
		value = nil
		err = ErrClosed
	}
	flight.value, flight.err, flight.complete = value, err, true
	if flight.waiters == 0 {
		wipe(value)
		flight.value = nil
		if p.calls[key] == flight {
			delete(p.calls, key)
		}
	} else if err == nil && p.ttl > 0 {
		if prior, ok := p.cache[key]; ok {
			wipe(prior.value)
		}
		p.cache[key] = cacheEntry{owner: flight, value: append([]byte(nil), value...), expires: time.Now().Add(p.ttl)}
	}
	close(flight.done)
}

// Close promptly cancels waiters and prevents late loads from repopulating the
// cache. Owned byte buffers are cleared best effort; returned Go strings and
// values copied into a child environment cannot be reliably erased by Go.
func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	p.cancel()
	for key, entry := range p.cache {
		wipe(entry.value)
		delete(p.cache, key)
	}
	for _, flight := range p.calls {
		if flight.complete {
			wipe(flight.value)
			flight.value = nil
		}
	}
	return nil
}
