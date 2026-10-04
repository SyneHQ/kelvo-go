// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/authstate"
	"github.com/SYNEHQ/kelvo-go/internal/secrets"
)

// Retired bindings stay fenced for this process lifetime. Exhaustion refuses
// new identities; it never evicts an older ownership decision.
const gatewayMaxBindings = authstate.MaxOwners

type keyFingerprint struct{}

type gatewayAuthKey struct {
	tenant    string
	principal string
	ctx       context.Context
	cancel    context.CancelFunc
}

type gatewayAuthRead struct {
	set      gatewayKeySet
	started  time.Time
	err      error
	admitted bool
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
	bindings   map[[32]byte]string // bounded lifetime ownership survives removal and invalidation
	validUntil time.Time
	revision   uint64
	digest     [32]byte
	closed     bool
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	read       gatewayKeyReader
	state      gatewayAuthenticationStore
	openState  gatewayAuthStateOpener
	admitting  bool
	poisoned   bool
	running    bool
	loaders    sync.WaitGroup
	closeOnce  sync.Once
	closeDone  chan struct{}
	closeErr   error // published by closeDone
}

func newGatewayAuthenticator(config GatewayAuthenticationConfig, tenants map[string]bool, reader gatewayKeyReader) (*gatewayAuthenticator, error) {
	return newGatewayAuthenticatorWithState(config, tenants, reader, openGatewayAuthState)
}

func newGatewayAuthenticatorWithState(config GatewayAuthenticationConfig, tenants map[string]bool, reader gatewayKeyReader, opener gatewayAuthStateOpener) (*gatewayAuthenticator, error) {
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	identities := make(map[string]bool, len(tenants))
	for tenant, enabled := range tenants {
		if !enabled || !clusterID.MatchString(tenant) {
			return nil, errGatewayAuthConfig
		}
		identities[tenant] = true
	}
	if len(identities) == 0 || len(identities) > 256 || opener == nil {
		return nil, errGatewayAuthConfig
	}
	if reader == nil {
		reader = secrets.ReadPrivateDocument
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &gatewayAuthenticator{config: normalized, tenants: identities, ctx: ctx, cancel: cancel, done: make(chan struct{}), read: reader, openState: opener}
	startup, stopStartup := context.WithTimeout(context.Background(), gatewayKeyReadTimeout)
	defer stopStartup()
	initial := a.beginRead()
	fail := func() (*gatewayAuthenticator, error) {
		a.startClose()
		if normalized.State != nil {
			if err := a.awaitClose(startup); err != nil {
				return nil, errors.Join(errGatewayAuthUnavailable, err)
			}
		}
		return nil, errGatewayAuthUnavailable
	}
	select {
	case result := <-initial:
		if startup.Err() != nil || !a.applyRead(result) {
			return fail()
		}
	case <-startup.Done():
		return fail()
	}
	go a.run()
	return a, nil
}

func (a *gatewayAuthenticator) beginRead() <-chan gatewayAuthRead {
	output := make(chan gatewayAuthRead, 1)
	started := time.Now()
	a.mu.Lock()
	if a.closed || a.poisoned {
		a.mu.Unlock()
		output <- gatewayAuthRead{started: started, err: errGatewayAuthUnavailable}
		return output
	}
	// Admission to this wait group is fenced by the same mutex as close.
	a.loaders.Add(1)
	a.mu.Unlock()
	ctx, cancel := context.WithDeadline(a.ctx, started.Add(gatewayKeyReadTimeout))
	go func() {
		defer a.loaders.Done()
		defer cancel()
		var err error
		if a.config.State != nil {
			a.mu.Lock()
			store := a.state
			a.mu.Unlock()
			if store == nil {
				store, err = a.openState(ctx, a.config.State.Directory, gatewayAuthScope(a.config.State, a.tenants))
				a.mu.Lock()
				a.state = store
				if errors.Is(err, authstate.ErrUncertain) {
					a.poisoned = true
				}
				a.mu.Unlock()
				if err == nil && store == nil {
					err = authstate.ErrUnavailable
				}
			}
		}
		var raw []byte
		var set gatewayKeySet
		if err == nil && gatewayAuthAttemptFresh(ctx) {
			raw, err = a.read(ctx, a.config.KeysFile, gatewayKeyFileLimit)
		}
		if err == nil {
			set, err = parseGatewayKeys(raw, a.tenants, a.config.MinRevision)
		}
		clear(raw)
		admitted := false
		if err == nil && gatewayAuthAttemptFresh(ctx) && a.config.State != nil {
			err = a.admitState(ctx, set)
			admitted = err == nil
		}
		if err == nil {
			err = ctx.Err()
			if err == nil && !gatewayAuthAttemptFresh(ctx) {
				err = context.DeadlineExceeded
			}
		}
		output <- gatewayAuthRead{set: set, started: started, err: err, admitted: admitted}
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

func (a *gatewayAuthenticator) validCandidateLocked(set gatewayKeySet) bool {
	valid := !a.closed && !a.poisoned && set.revision >= a.config.MinRevision && set.revision >= a.revision && (set.revision != a.revision || set.digest == a.digest)
	// A previously accepted token cannot move to another tenant or principal.
	newBindings := 0
	for hash, tenant := range set.keys {
		if owner, exists := a.bindings[hash]; exists {
			if owner != tenant+"\x00"+set.principals[hash] {
				valid = false
			}
		} else {
			newBindings++
		}
	}
	if len(a.bindings)+newBindings > gatewayMaxBindings {
		valid = false
	}
	return valid
}

func (a *gatewayAuthenticator) applyRead(result gatewayAuthRead) bool {
	if result.err != nil || time.Since(result.started) >= gatewayKeyReadTimeout {
		a.invalidate()
		return false
	}
	return a.publish(result.set, result.started, result.admitted)
}

// The direct helper is retained for memory-only authority and its contract
// tests; configured durable authority cannot bypass admission through it.
func (a *gatewayAuthenticator) apply(set gatewayKeySet, started time.Time) bool {
	return a.publish(set, started, false)
}

func (a *gatewayAuthenticator) publish(set gatewayKeySet, started time.Time, admitted bool) bool {
	until := started.Add(a.config.ReloadInterval + gatewayKeyReadTimeout)
	a.mu.Lock()
	// Recheck after acquiring the mutex: a result can be fresh at applyRead
	// and expire while waiting to publish. Durability never extends that budget.
	now := time.Now()
	valid := a.validCandidateLocked(set) && now.Before(until) &&
		(a.config.State == nil || (admitted && now.Before(started.Add(gatewayKeyReadTimeout))))
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
			ctx = context.WithValue(ctx, keyFingerprint{}, hash)
			ctx = context.WithValue(ctx, keyPrincipalID{}, set.principals[hash])
			next[hash] = &gatewayAuthKey{tenant: tenant, principal: set.principals[hash], ctx: ctx, cancel: cancel}
		}
	}
	if a.bindings == nil {
		a.bindings = make(map[[32]byte]string, len(set.keys))
	}
	for hash, tenant := range set.keys {
		a.bindings[hash] = tenant + "\x00" + set.principals[hash]
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
	if a.closed || a.poisoned || !time.Now().Before(a.validUntil) {
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
	healthy := !a.closed && !a.poisoned && time.Now().Before(a.validUntil)
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
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.running = true
	a.mu.Unlock()
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
			a.mu.Lock()
			canRead := !a.closed && !a.poisoned
			a.mu.Unlock()
			if pending == nil && canRead {
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
			if a.applyRead(result) {
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
			a.mu.Lock()
			if a.admitting {
				a.poisoned = true
			}
			prior := a.keys
			a.keys, a.validUntil = nil, time.Time{}
			a.mu.Unlock()
			cancelGatewayKeys(prior)
			// Keep pending until this same reader completes; never launch a
			// replacement goroutine for a blocked filesystem operation.
		}
	}
}

func (a *gatewayAuthenticator) startClose() {
	a.closeOnce.Do(func() {
		a.closeDone = make(chan struct{})
		a.mu.Lock()
		a.closed = true
		running := a.running
		a.mu.Unlock()
		a.cancel()
		a.invalidate()
		go func() {
			defer close(a.closeDone)
			if running {
				<-a.done
			}
			// Memory-only authentication owns no durable writer. Preserve its
			// existing shutdown behavior when a private-file read is stuck.
			if a.config.State == nil {
				return
			}
			a.loaders.Wait()
			// A late initial Open is still owned by its original loader. Only
			// after it quiesces may this cleanup acquire and close its store.
			a.mu.Lock()
			store, poisoned := a.state, a.poisoned
			a.mu.Unlock()
			if store != nil && store.Close(context.Background()) != nil {
				a.closeErr = authstate.ErrUncertain
			}
			if poisoned {
				a.closeErr = authstate.ErrUncertain
			}
		}()
	})
}

func (a *gatewayAuthenticator) awaitClose(ctx context.Context) error {
	select {
	case <-a.closeDone:
		return a.closeErr
	case <-ctx.Done():
		return authstate.ErrUncertain
	}
}

func (a *gatewayAuthenticator) close() error {
	a.startClose()
	ctx, cancel := context.WithTimeout(context.Background(), gatewayKeyReadTimeout)
	defer cancel()
	return a.awaitClose(ctx)
}
