// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// errNeedsCredentials means a read found no worker holding credentials of its
// source's current authorization. Nothing was sent; the caller authorizes the
// source again and sends the read with credentials.
var errNeedsCredentials = errors.New("warm_pool: read needs credentials")

// warmKey identifies the workers that may serve a read. Workers are shared by
// source revision: principals whose authorizations resolve to the same
// definition and credentials share them.
func warmKey(tenant, team, connection, database, schema, revision string) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s", tenant, team, connection, database, schema, revision)
}

// processSecretless reports a request that relies on a worker's credentials.
func processSecretless(input adapter.ProcessRequest) bool {
	s := input.Source
	return s.DSN == "" && s.URL == "" && s.Username == "" && s.Password == "" && s.Token == ""
}

// readAuthorization is one principal's last authorization of one source for
// read workers. It holds no secret values: the workers hold the credentials.
type readAuthorization struct {
	source     *Executor      // the authorized executor without secrets: catalog and source quota
	def        catalog.Source // the authorized source definition
	revision   string
	database   string
	workerKey  string
	at         time.Time
	validUntil time.Time // the resolver's credential window; zero for static sources
}

// fresh reports whether a reads may still rely on a without authorizing
// again: younger than reauthorize_every and inside the resolver's window.
func (a *readAuthorization) fresh(now time.Time, every time.Duration) bool {
	return now.Sub(a.at) < every && (a.validUntil.IsZero() || now.Before(a.validUntil))
}

// credentialsValidUntil is the window a worker request carries.
func (a *readAuthorization) credentialsValidUntil(now time.Time, every time.Duration) int64 {
	return min(a.credentialBound(every), now.Add(credentialWindowLimit()).Unix())
}

// credentialBound is when this authorization's credentials stop being usable:
// its freshness or the resolver's ValidUntil, whichever is first.
func (a *readAuthorization) credentialBound(every time.Duration) int64 {
	until := a.at.Add(every)
	if !a.validUntil.IsZero() && a.validUntil.Before(until) {
		until = a.validUntil
	}
	// Rounded up: the window is in whole seconds and must stay after now.
	return (until.UnixNano() + int64(time.Second) - 1) / int64(time.Second)
}

type readAuthKey struct{}

// withReadAuthorization names the authorization a read relies on, so a worker
// is never started from credentials of a revision it no longer holds.
func withReadAuthorization(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, readAuthKey{}, key)
}

// staleRevision reports a read whose revision is not its authorization's
// current one: it must not start a worker. Called with m.mu held.
func (m *WarmPoolManager) staleRevision(ctx context.Context, revision string) bool {
	key, ok := ctx.Value(readAuthKey{}).(string)
	if !ok {
		return false
	}
	m.authMu.Lock()
	defer m.authMu.Unlock()
	a := m.auths[key]
	return a == nil || a.revision != revision
}

// freshAuthorization returns key's authorization while it is fresh.
func (m *WarmPoolManager) freshAuthorization(key string) *readAuthorization {
	m.authMu.Lock()
	defer m.authMu.Unlock()
	if a := m.auths[key]; a != nil && a.fresh(time.Now(), m.cfg.reauthorizeEvery()) {
		return a
	}
	return nil
}

// currentAuthorization returns key's authorization, fresh or not.
func (m *WarmPoolManager) currentAuthorization(key string) *readAuthorization {
	m.authMu.Lock()
	defer m.authMu.Unlock()
	return m.auths[key]
}

// storeAuthorization records a successful authorization. A new revision for
// the same source identity is a credential rotation, not a revocation: the
// old revision's workers drain (no new streams, in-flight ones finish) unless
// another principal's authorization still uses them.
func (m *WarmPoolManager) storeAuthorization(key string, a *readAuthorization) {
	m.storeAuthorizationWithin(context.Background(), key, a)
}

// storeAuthorizationWithin is storeAuthorization waiting at most ctx for
// drained idle workers to end.
func (m *WarmPoolManager) storeAuthorizationWithin(ctx context.Context, key string, a *readAuthorization) {
	m.authMu.Lock()
	if m.auths == nil {
		m.auths = map[string]*readAuthorization{}
	}
	old := m.auths[key]
	m.auths[key] = a
	retire := old != nil && old.workerKey != a.workerKey && !m.referencedLocked(old.workerKey)
	m.authMu.Unlock()
	m.metrics.Authorizations.Add(1)
	if retire {
		m.drainKey(ctx, old.workerKey)
	}
}

// retireAuthorization ends observed after its source stopped qualifying for
// read workers; its workers drain like a rotation.
func (m *WarmPoolManager) retireAuthorization(key string, observed *readAuthorization) {
	if observed == nil {
		return
	}
	m.authMu.Lock()
	if m.auths[key] != observed {
		m.authMu.Unlock()
		return
	}
	delete(m.auths, key)
	retire := !m.referencedLocked(observed.workerKey)
	m.authMu.Unlock()
	if retire {
		drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		m.drainKey(drainCtx, observed.workerKey)
		cancel()
	}
}

// revokeAuthorization ends observed, and only observed, after a definitive
// denial, killing its workers unless another principal's authorization still
// uses them. It waits for the kills up to ctx.
func (m *WarmPoolManager) revokeAuthorization(ctx context.Context, key string, observed *readAuthorization) error {
	if observed == nil {
		return nil
	}
	m.authMu.Lock()
	if m.auths[key] != observed {
		m.authMu.Unlock() // another caller already re-authorized or revoked it
		return nil
	}
	delete(m.auths, key)
	kill := !m.referencedLocked(observed.workerKey)
	m.authMu.Unlock()
	if kill {
		return m.killKey(ctx, observed.workerKey)
	}
	return nil
}

func (m *WarmPoolManager) referencedLocked(workerKey string) bool {
	for _, a := range m.auths {
		if a.workerKey == workerKey {
			return true
		}
	}
	return false
}

// pruneAuthorizations drops expired authorizations whose workers are gone.
// One whose workers still live is kept: a later definitive denial must still
// find, and kill, the workers it authorized.
func (m *WarmPoolManager) pruneAuthorizations() {
	live := map[string]bool{}
	m.mu.Lock()
	for _, w := range m.workers {
		live[w.key] = true
	}
	m.mu.Unlock()
	now, every := time.Now(), m.cfg.reauthorizeEvery()
	m.authMu.Lock()
	defer m.authMu.Unlock()
	for key, a := range m.auths {
		if !a.fresh(now, every) && !live[a.workerKey] {
			delete(m.auths, key)
		}
	}
}

// drainKey stops new streams on workerKey's workers; each ends after its last
// in-flight stream. Idle ones end now, and drainKey waits up to ctx for them
// so their reservations are free for the replacement.
func (m *WarmPoolManager) drainKey(ctx context.Context, workerKey string) {
	var idle []*warmWorkerEntry
	m.mu.Lock()
	for _, w := range m.workers {
		if w.key == workerKey && !w.draining {
			w.draining = true
			if w.inFlight == 0 {
				m.evictLocked(w, &m.metrics.Rotations)
				idle = append(idle, w)
			} else {
				// A draining worker takes no new streams: its place is a read
				// waiting for this key's replacement.
				m.wakeLocked(workerKey)
			}
		}
	}
	m.mu.Unlock()
	_ = m.waitDead(ctx, idle)
}

// waitDead waits up to ctx for evicted workers to end. Every worker closes
// dead when it ends, a failed launch included.
func (m *WarmPoolManager) waitDead(ctx context.Context, evicted []*warmWorkerEntry) error {
	for _, w := range evicted {
		select {
		case <-w.dead:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// killKey kills workerKey's workers, launching ones included, and waits up to
// ctx for them to end so their reservations are free for a replacement.
func (m *WarmPoolManager) killKey(ctx context.Context, workerKey string) error {
	var evicted []*warmWorkerEntry
	m.mu.Lock()
	for _, w := range m.workers {
		if w.key == workerKey {
			m.evictLocked(w, &m.metrics.EvictionsAuth)
			evicted = append(evicted, w)
		}
	}
	m.mu.Unlock()
	return m.waitDead(ctx, evicted)
}

// authFlight is one in-progress authorization that concurrent reads of the
// same principal share.
type authFlight struct {
	done chan struct{}
	a    *readAuthorization
	dsn  string
	err  error
}

// authorizeOnce runs authorize for key once among concurrent callers: each
// principal authorizes once per window. If the leading caller's own context
// ends, waiters run their own instead of taking its cancellation.
func (m *WarmPoolManager) authorizeOnce(ctx context.Context, key string, authorize func() (*readAuthorization, string, error)) (*readAuthorization, string, error) {
	for {
		m.authMu.Lock()
		if m.authFlights == nil {
			m.authFlights = map[string]*authFlight{}
		}
		if f := m.authFlights[key]; f != nil {
			m.authMu.Unlock()
			select {
			case <-f.done:
			case <-ctx.Done():
				return nil, "", ctx.Err()
			}
			if errors.Is(f.err, context.Canceled) || errors.Is(f.err, context.DeadlineExceeded) {
				continue // the leader's caller left; authorize ourselves
			}
			return f.a, f.dsn, f.err
		}
		f := &authFlight{done: make(chan struct{})}
		m.authFlights[key] = f
		m.authMu.Unlock()
		func() {
			defer func() {
				m.authMu.Lock()
				delete(m.authFlights, key)
				m.authMu.Unlock()
				close(f.done)
			}()
			f.a, f.dsn, f.err = authorize()
		}()
		return f.a, f.dsn, f.err
	}
}

// definitiveDenial reports a source authorization that was refused, as
// opposed to one that could not be checked (cancelled, timed out, resolver
// unavailable). Only a definitive denial revokes.
func definitiveDenial(err error) bool {
	var public *query.Error
	return errors.As(err, &public) && (public.Code == "PERMISSION_DENIED" || public.Code == "NOT_FOUND")
}
