// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package secrets resolves explicitly mapped source credentials in the trusted
// parent process. Provider credentials never enter catalogs or query children.
package secrets

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	MaxKeys              = 128
	MaxValueBytes        = 16 << 10
	MaxCacheBytes        = MaxKeys * MaxValueBytes
	MaxTTL               = 5 * time.Minute
	DefaultTimeout       = 5 * time.Second
	MaxTimeout           = 30 * time.Second
	MaxConcurrentLookups = 16
)

var (
	ErrInvalid     = errors.New("invalid secret provider configuration")
	ErrUnavailable = errors.New("configured secret is unavailable")
	ErrClosed      = errors.New("secret provider is closed")
	ErrUnsupported = errors.New("file secret provider is unsupported on this platform")
)

type Config struct {
	Files      map[string]string        `yaml:"files"`
	Providers  map[string]CloudProvider `yaml:"providers,omitempty"`
	References map[string]Reference     `yaml:"references,omitempty"`
	TTL        time.Duration            `yaml:"ttl,omitempty"`
	Timeout    time.Duration            `yaml:"timeout,omitempty"`
}

// Keys returns configured source-environment references only, never provider
// credential fields. The application must enforce its catalog key policy.
func (c Config) Keys() []string {
	keys := make([]string, 0, len(c.Files)+len(c.References))
	for key := range c.Files {
		keys = append(keys, key)
	}
	for key := range c.References {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type cacheEntry struct {
	owner   *lookup
	value   []byte
	expires time.Time
}
type lookup struct {
	done       chan struct{}
	waiters    int
	complete   bool
	delivered  bool
	value      []byte
	err        error
	ctx        context.Context
	cancel     context.CancelFunc
	generation uint64
	expires    time.Time
}
type Provider struct {
	mu          sync.Mutex
	files       map[string]string
	ttl         time.Duration
	cache       map[string]cacheEntry
	calls       map[string]*lookup
	closed      bool
	ctx         context.Context
	cancel      context.CancelFunc
	read        func(context.Context, string) ([]byte, error)
	references  map[string]Reference
	providers   map[string]backend
	timeout     time.Duration
	gate        chan struct{}
	generations map[string]uint64
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
	if len(config.Files)+len(config.References) == 0 || len(config.Files)+len(config.References) > MaxKeys || config.TTL < 0 || config.TTL > MaxTTL || config.Timeout < 0 || config.Timeout > MaxTimeout {
		return nil, ErrInvalid
	}
	files := make(map[string]string, len(config.Files))
	for key, path := range config.Files {
		if key == "" || len(key) > 256 || path == "" || len(path) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
			return nil, ErrInvalid
		}
		files[key] = path
	}
	providers, references, err := configuredBackends(config)
	if err != nil {
		return nil, err
	}
	timeout := config.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Provider{files: files, ttl: config.TTL, cache: map[string]cacheEntry{}, calls: map[string]*lookup{}, ctx: ctx, cancel: cancel, read: readPrivateFile, providers: providers, references: references, timeout: timeout, gate: make(chan struct{}, MaxConcurrentLookups), generations: map[string]uint64{}}, nil
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
	_, fileKnown := p.files[key]
	_, cloudKnown := p.references[key]
	known := fileKnown || cloudKnown
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
	if flight != nil && flight.ctx.Err() != nil {
		p.mu.Unlock()
		return "", true, ErrUnavailable
	}
	if flight == nil {
		lookupCtx, cancel := context.WithTimeout(p.ctx, p.timeout)
		flight = &lookup{done: make(chan struct{}), ctx: lookupCtx, cancel: cancel, generation: p.generations[key]}
		p.calls[key] = flight
		flight.waiters++
		go p.load(key, flight)
	} else {
		flight.waiters++
	}
	p.mu.Unlock()
	select {
	case <-flight.done:
	case <-ctx.Done():
	case <-p.ctx.Done():
	case <-flight.ctx.Done():
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
	if flight.generation != p.generations[key] || (!flight.expires.IsZero() && !time.Now().Before(flight.expires)) {
		return "", true, ErrUnavailable
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
func (p *Provider) load(key string, flight *lookup) {
	defer flight.cancel()
	var result secretValue
	var err error
	select {
	case p.gate <- struct{}{}:
		if path, ok := p.files[key]; ok {
			result.data, err = p.read(flight.ctx, path)
		} else {
			ref := p.references[key]
			result, err = p.providers[ref.Provider].Fetch(flight.ctx, ref)
		}
		<-p.gate
	case <-flight.ctx.Done():
		err = ErrUnavailable
	}
	value := result.data
	if err == nil && (flight.ctx.Err() != nil || len(value) > MaxValueBytes || bytes.IndexByte(value, 0) >= 0 || (!result.expires.IsZero() && !time.Now().Before(result.expires))) {
		err = ErrUnavailable
	}
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
	if flight.generation != p.generations[key] {
		wipe(value)
		value, err = nil, ErrUnavailable
	}
	flight.expires = result.expires
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
		expires := time.Now().Add(p.ttl)
		if !result.expires.IsZero() && result.expires.Before(expires) {
			expires = result.expires
		}
		p.cache[key] = cacheEntry{owner: flight, value: append([]byte(nil), value...), expires: expires}
	}
	close(flight.done)
}

// Invalidate rejects delivery of outstanding reads for key that have not passed
// the final delivery check under the lock, including completed flights. It also
// clears cached bytes. Already returned strings/child credentials are not
// revocable here. The trusted caller must separately stop affected operations.
func (p *Provider) Invalidate(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, fileKnown := p.files[key]
	_, cloudKnown := p.references[key]
	if p.closed || (!fileKnown && !cloudKnown) {
		return false
	}
	// A wrap would allow an ancient flight to match. Exhaustion disables the
	// provider rather than relaxing fencing (not reachable in practical use).
	if p.generations[key] == ^uint64(0) {
		p.closeLocked()
		return true
	} else {
		p.generations[key]++
	}
	if entry, ok := p.cache[key]; ok {
		wipe(entry.value)
		delete(p.cache, key)
	}
	if flight := p.calls[key]; flight != nil {
		flight.cancel()
		if flight.complete {
			wipe(flight.value)
			flight.value = nil
		}
	}
	return true
}

// Close promptly cancels waiters and prevents late loads from repopulating the
// cache. Owned byte buffers are cleared best effort; returned Go strings and
// values copied into a child environment cannot be reliably erased by Go.
func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeLocked()
	return nil
}

func (p *Provider) closeLocked() {
	if p.closed {
		return
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
	for _, provider := range p.providers {
		provider.Close()
	}
}
