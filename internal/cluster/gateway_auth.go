// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/secrets"
)

type gatewayAuthKey struct {
	tenant string
	ctx    context.Context
	cancel context.CancelFunc
}

type gatewayAuthRead struct {
	set     gatewayKeySet
	started time.Time
	err     error
}

type gatewayKeyReader func(context.Context, string, int) ([]byte, error)

// Only hashes and key-specific cancellation contexts survive a successful read.
// The single loader goroutine is deliberately not replaced while a filesystem
// read is stuck, even after timeout. Late results never renew expired authority.
type gatewayAuthenticator struct {
	mu         sync.Mutex
	config     GatewayAuthenticationConfig
	tenants    map[string]bool
	keys       map[[32]byte]*gatewayAuthKey
	bindings   map[[32]byte]string // last accepted ownership survives fail-closed invalidation
	validUntil time.Time
	revision   uint64
	digest     [32]byte
	closed     bool
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	read       gatewayKeyReader
}

func newGatewayAuthenticator(config GatewayAuthenticationConfig, tenants map[string]bool, reader gatewayKeyReader) (*gatewayAuthenticator, error) {
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	if reader == nil {
		reader = secrets.ReadPrivateDocument
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &gatewayAuthenticator{config: normalized, tenants: tenants, ctx: ctx, cancel: cancel, done: make(chan struct{}), read: reader}
	initial := a.beginRead()
	timer := time.NewTimer(gatewayKeyReadTimeout)
	defer timer.Stop()
	select {
	case result := <-initial:
		if result.err != nil || time.Since(result.started) >= gatewayKeyReadTimeout || !a.apply(result.set, result.started) {
			cancel()
			return nil, errGatewayAuthUnavailable
		}
	case <-timer.C:
		cancel()
		return nil, errGatewayAuthUnavailable
	}
	go a.run()
	return a, nil
}

func (a *gatewayAuthenticator) beginRead() <-chan gatewayAuthRead {
	output := make(chan gatewayAuthRead, 1)
	started := time.Now()
	ctx, cancel := context.WithTimeout(a.ctx, gatewayKeyReadTimeout)
	go func() {
		defer cancel()
		raw, err := a.read(ctx, a.config.KeysFile, gatewayKeyFileLimit)
		var set gatewayKeySet
		if err == nil {
			set, err = parseGatewayKeys(raw, a.tenants, a.config.MinRevision)
		}
		clear(raw)
		output <- gatewayAuthRead{set: set, started: started, err: err}
	}()
	return output
}

func cancelGatewayKeys(keys map[[32]byte]*gatewayAuthKey) {
	for _, key := range keys {
		key.cancel()
	}
}

func (a *gatewayAuthenticator) invalidate() {
	a.mu.Lock()
	prior := a.keys
	a.keys, a.validUntil = nil, time.Time{}
	a.mu.Unlock()
	cancelGatewayKeys(prior)
}

func (a *gatewayAuthenticator) apply(set gatewayKeySet, started time.Time) bool {
	until := started.Add(a.config.ReloadInterval + gatewayKeyReadTimeout)
	a.mu.Lock()
	valid := !a.closed && time.Now().Before(until) && set.revision >= a.config.MinRevision && set.revision >= a.revision && (set.revision != a.revision || set.digest == a.digest)
	// A currently configured token must never move to another tenant in place.
	for hash, tenant := range set.keys {
		if owner, exists := a.bindings[hash]; exists && owner != tenant {
			valid = false
		}
	}
	prior := a.keys
	priorFresh := time.Now().Before(a.validUntil)
	if !valid {
		a.keys, a.validUntil = nil, time.Time{}
		a.mu.Unlock()
		cancelGatewayKeys(prior)
		return false
	}
	next := make(map[[32]byte]*gatewayAuthKey, len(set.keys))
	for hash, tenant := range set.keys {
		if old := prior[hash]; old != nil && priorFresh && old.ctx.Err() == nil {
			next[hash] = old
			delete(prior, hash)
		} else {
			ctx, cancel := context.WithCancel(a.ctx)
			next[hash] = &gatewayAuthKey{tenant: tenant, ctx: ctx, cancel: cancel}
		}
	}
	a.bindings = make(map[[32]byte]string, len(set.keys))
	for hash, tenant := range set.keys {
		a.bindings[hash] = tenant
	}
	a.keys, a.validUntil, a.revision, a.digest = next, until, set.revision, set.digest
	a.mu.Unlock()
	cancelGatewayKeys(prior)
	return true
}

// Request-time expiry remains authoritative even if the reloader is delayed.
// Each returned context belongs to the key, not just its tenant: registering an
// AfterFunc after revocation still immediately observes the canceled context.
func (a *gatewayAuthenticator) lookup(token string) (string, context.Context, bool) {
	a.mu.Lock()
	if a.closed || !time.Now().Before(a.validUntil) {
		prior := a.keys
		a.keys, a.validUntil = nil, time.Time{}
		a.mu.Unlock()
		cancelGatewayKeys(prior)
		return "", nil, false
	}
	if len(token) < 32 || len(token) > 256 {
		a.mu.Unlock()
		return "", nil, false
	}
	key := a.keys[sha256.Sum256([]byte(token))]
	a.mu.Unlock()
	if key == nil || key.ctx.Err() != nil {
		return "", nil, false
	}
	return key.tenant, key.ctx, true
}

func (a *gatewayAuthenticator) ready() bool {
	a.mu.Lock()
	healthy := !a.closed && time.Now().Before(a.validUntil)
	var prior map[[32]byte]*gatewayAuthKey
	if !healthy {
		prior, a.keys, a.validUntil = a.keys, nil, time.Time{}
	}
	a.mu.Unlock()
	cancelGatewayKeys(prior)
	return healthy
}

func (a *gatewayAuthenticator) expiry() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.validUntil
}

func (a *gatewayAuthenticator) run() {
	defer close(a.done)
	tick := time.NewTicker(a.config.ReloadInterval)
	defer tick.Stop()
	expiry := time.NewTimer(time.Until(a.expiry()))
	defer expiry.Stop()
	var pending <-chan gatewayAuthRead
	var timeout <-chan time.Time
	var deadline *time.Timer
	var timedOut bool
	defer func() {
		if deadline != nil {
			deadline.Stop()
		}
	}()
	for {
		select {
		case <-a.ctx.Done():
			a.invalidate()
			return
		case <-expiry.C:
			a.ready() // also cancels active keys without waiting for a request
		case <-tick.C:
			a.ready()
			if pending == nil {
				pending = a.beginRead()
				deadline = time.NewTimer(gatewayKeyReadTimeout)
				timeout, timedOut = deadline.C, false
			}
		case result := <-pending:
			deadline.Stop()
			pending, timeout = nil, nil
			if timedOut || result.err != nil || time.Since(result.started) >= gatewayKeyReadTimeout {
				a.invalidate()
				continue
			}
			if a.apply(result.set, result.started) {
				if !expiry.Stop() {
					select {
					case <-expiry.C:
					default:
					}
				}
				expiry.Reset(max(time.Duration(0), time.Until(a.expiry())))
			}
		case <-timeout:
			timeout, timedOut = nil, true
			a.invalidate()
			// Keep pending until this same reader completes; never launch a
			// replacement goroutine for a blocked filesystem operation.
		}
	}
}

func (a *gatewayAuthenticator) close() {
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
	a.cancel()
	a.invalidate()
	<-a.done
}
