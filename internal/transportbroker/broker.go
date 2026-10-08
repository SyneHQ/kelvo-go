// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package transportbroker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"time"
)

type Limits struct {
	MaxSessions, MaxDataConnections, MaxDataPerSession int
}

const maxPoolConnections = 1 << 16

// Snapshot counts reservations, including opens and closes that have not joined.
// Every session reserves MaxDataPerSession cancellation slots outside data capacity.
type Snapshot struct {
	Sessions, DataConnections, CancellationConnections, Opening int
	Draining, CleanupFailed                                     bool
}

type Broker struct {
	self     *Broker
	mu       sync.Mutex
	limits   Limits
	opener   Opener
	sessions map[*Session]struct{}
	attempts map[sourceAttempt]*Session
	state    Snapshot
	done     chan struct{}
	joined   bool
}

// New creates a dormant broker. No existing runtime constructs one.
func New(limits Limits, opener Opener) (*Broker, error) {
	if opener == nil || limits.MaxSessions < 1 || limits.MaxSessions > maxPoolConnections || limits.MaxDataConnections < 1 || limits.MaxDataConnections > maxPoolConnections ||
		limits.MaxDataPerSession < 1 || limits.MaxDataPerSession > limits.MaxDataConnections ||
		limits.MaxSessions > maxPoolConnections/limits.MaxDataPerSession {
		return nil, ErrInvalid
	}
	b := &Broker{limits: limits, opener: opener, sessions: make(map[*Session]struct{}), attempts: make(map[sourceAttempt]*Session), done: make(chan struct{})}
	b.self = b
	return b, nil
}

// Session belongs to the trusted parent, not to a driver. Its context must cover
// execution cleanup, including source cancellation; a query's cancelled request
// context must not be substituted for this custody context.
type Session struct {
	self                             *Session
	broker                           *Broker
	binding                          Binding
	ctx                              context.Context
	cancel                           context.CancelFunc
	done                             chan struct{}
	connections                      map[*connection]struct{}
	data, cancelConnections, pending int
	closing, joined                  bool
}

type sourceAttempt struct {
	clusterTenant, principal, tenant, source string
	kind, id, worker, owner, claim           string
}

func (b Binding) attempt() sourceAttempt {
	e := b.Execution
	return sourceAttempt{b.ClusterTenant, b.ServicePrincipal, b.Tenant, b.Source, e.Kind, e.ID, e.Worker, e.Owner, e.Claim}
}

func (b *Broker) Admit(ctx context.Context, binding Binding) (*Session, error) {
	if b == nil || b.self != b || ctx == nil || !validBinding(binding, time.Now()) {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.state.Draining {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	if b.attempts[binding.attempt()] != nil {
		b.mu.Unlock()
		return nil, ErrScope
	}
	if len(b.sessions) >= b.limits.MaxSessions {
		b.mu.Unlock()
		return nil, ErrCapacity
	}
	lifetime, cancel := context.WithDeadline(ctx, binding.ExpiresAt)
	s := &Session{broker: b, binding: binding, ctx: lifetime, cancel: cancel, done: make(chan struct{}), connections: make(map[*connection]struct{})}
	s.self = s
	b.sessions[s] = struct{}{}
	b.attempts[binding.attempt()] = s
	b.state.Sessions++
	b.mu.Unlock()
	context.AfterFunc(lifetime, func() { _ = s.beginClose() })
	return s, nil
}

func (b *Broker) Snapshot() Snapshot {
	if b == nil || b.self != b {
		return Snapshot{Draining: true, CleanupFailed: true}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Broker) Close(ctx context.Context) error {
	if b == nil || b.self != b || ctx == nil {
		return ErrInvalid
	}
	b.mu.Lock()
	b.state.Draining = true
	sessions := make([]*Session, 0, len(b.sessions))
	for s := range b.sessions {
		sessions = append(sessions, s)
	}
	b.finishLocked()
	b.mu.Unlock()
	for _, s := range sessions {
		_ = s.beginClose()
	}
	return wait(ctx, b.done)
}

func (b *Broker) finishLocked() {
	if b.state.Draining && len(b.sessions) == 0 && !b.joined {
		b.joined = true
		close(b.done)
	}
}

// DataDialer and CancellationDialer have the same source/attempt authority.
// Cancellation has one reserved slot per permitted data connection, not an
// unbounded priority lane. Division in New bounds the aggregate without overflow.
// A connector must explicitly select it; inspecting SQL or guessing dial order
// cannot reliably distinguish a cancellation connection from a data connection.
func (s *Session) DataDialer() Dialer         { return dialer{s: s, purpose: Data} }
func (s *Session) CancellationDialer() Dialer { return dialer{s: s, purpose: Cancellation} }

type dialer struct {
	s       *Session
	purpose Purpose
}

func (d dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if d.s == nil || d.s.self != d.s || d.s.broker == nil || d.s.broker.self != d.s.broker || ctx == nil {
		return nil, ErrInvalid
	}
	s, b := d.s, d.s.broker
	if network != "tcp" || address != s.binding.Authority {
		return nil, ErrScope
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	if s.closing || s.ctx.Err() != nil || (b.state.Draining && d.purpose != Cancellation) {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	if _, exists := b.sessions[s]; !exists || b.attempts[s.binding.attempt()] != s {
		b.mu.Unlock()
		return nil, ErrInvalid
	}
	if d.purpose == Data {
		if s.data >= b.limits.MaxDataPerSession || b.state.DataConnections >= b.limits.MaxDataConnections {
			b.mu.Unlock()
			return nil, ErrCapacity
		}
		s.data++
		b.state.DataConnections++
	} else if d.purpose == Cancellation {
		if s.cancelConnections >= b.limits.MaxDataPerSession {
			b.mu.Unlock()
			return nil, ErrCapacity
		}
		s.cancelConnections++
		b.state.CancellationConnections++
	} else {
		b.mu.Unlock()
		return nil, ErrInvalid
	}
	s.pending++
	b.state.Opening++
	b.mu.Unlock()

	openCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() { stop(); cancel() }()
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		s.openFailed(d.purpose)
		return nil, ErrOpen
	}
	request := OpenRequest{Binding: s.binding, ID: hex.EncodeToString(nonce[:]), Purpose: d.purpose}
	raw, err, panicked := callOpener(b.opener, openCtx, request)
	if panicked {
		// A provider panic cannot prove that no socket was opened. Keep the
		// reservation and stop new data admission until operator recovery.
		b.mu.Lock()
		b.state.CleanupFailed, b.state.Draining = true, true
		b.mu.Unlock()
		return nil, ErrCleanup
	}
	if raw == nil {
		s.openFailed(d.purpose)
		if openCtx.Err() != nil {
			return nil, openCtx.Err()
		}
		return nil, ErrOpen
	}
	c := &connection{conn: raw, session: s, purpose: d.purpose, closed: make(chan struct{})}
	c.self = c
	b.mu.Lock()
	s.pending--
	b.state.Opening--
	s.connections[c] = struct{}{}
	reject := err != nil || openCtx.Err() != nil || s.ctx.Err() != nil || s.closing || (b.state.Draining && d.purpose != Cancellation)
	b.mu.Unlock()
	if reject {
		// Even a provider that returns a connection with an error transfers
		// cleanup custody. Do not make its reservation available prematurely.
		go c.Close()
		if openCtx.Err() != nil {
			return nil, openCtx.Err()
		}
		return nil, ErrOpen
	}
	return c, nil
}

func callOpener(opener Opener, ctx context.Context, request OpenRequest) (conn net.Conn, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	conn, err = opener.Open(ctx, request)
	return conn, err, false
}

func (s *Session) openFailed(purpose Purpose) {
	b := s.broker
	b.mu.Lock()
	s.pending--
	b.state.Opening--
	s.releaseLocked(purpose)
	b.mu.Unlock()
}

func (s *Session) releaseLocked(purpose Purpose) {
	if purpose == Data {
		s.data--
		s.broker.state.DataConnections--
	} else {
		s.cancelConnections--
		s.broker.state.CancellationConnections--
	}
	s.finishLocked()
}

func (s *Session) finishLocked() {
	if s.closing && s.data == 0 && s.cancelConnections == 0 && s.pending == 0 && !s.joined {
		s.joined = true
		delete(s.broker.sessions, s)
		delete(s.broker.attempts, s.binding.attempt())
		s.broker.state.Sessions--
		close(s.done)
		s.broker.finishLocked()
	}
}

func (s *Session) beginClose() error {
	if s == nil || s.self != s || s.broker == nil || s.broker.self != s.broker {
		return ErrInvalid
	}
	b := s.broker
	b.mu.Lock()
	if s.closing {
		b.mu.Unlock()
		return nil
	}
	if _, exists := b.sessions[s]; !exists || b.attempts[s.binding.attempt()] != s {
		b.mu.Unlock()
		return ErrInvalid
	}
	s.closing = true
	connections := make([]*connection, 0, len(s.connections))
	for c := range s.connections {
		connections = append(connections, c)
	}
	s.finishLocked()
	b.mu.Unlock()
	s.cancel()
	for _, c := range connections {
		go c.Close()
	}
	return nil
}

// Close prevents new opens and waits for pending opens and socket closes. A
// timeout does not release capacity; uncertain cleanup requires operator action.
func (s *Session) Close(ctx context.Context) error {
	if s == nil || ctx == nil {
		return ErrInvalid
	}
	if err := s.beginClose(); err != nil {
		return err
	}
	return wait(ctx, s.done)
}

func wait(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errors.Join(ErrCleanup, ctx.Err())
	}
}
