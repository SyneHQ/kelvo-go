// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"sync"

	"github.com/SYNEHQ/kelvo-go/adapter"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/rabbitconnect"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/sourceproof"
)

// The runtime is shared by executions. One broker enforces total source capacity.
// The operator must configure private operations before startup constructs it.
type privateOperationRuntime struct {
	mu             sync.Mutex
	policy         *privateOperationPolicy
	broker         *transportbroker.Broker
	limits         transportbroker.Limits
	life           context.Context
	cancel         context.CancelFunc
	issuer         *rabbitconnect.HTTPIssuer
	opener         func(sourceproof.Scope, func(context.Context) (sourceproof.Envelope, error)) (transportbroker.Opener, error)
	channel        func(context.Context, adapter.ProcessRequest, *transportbroker.Session, transportbroker.Binding, int, func()) (*privateOperationChannel, error)
	entries        map[transportbroker.Binding]*privateOperationEntry
	pending        int
	closed, joined bool
	cleanupEnabled bool
	done           chan struct{}
}
type privateOperationEntry struct {
	opener  transportbroker.Opener
	cleanup *privatePostgresCleanup
}

func newPrivateOperationRuntime(parent context.Context, p *privateOperationPolicy, proxy rabbitconnect.Config, issuer rabbitconnect.HTTPIssuerConfig, limits transportbroker.Limits) (*privateOperationRuntime, error) {
	if parent == nil || parent.Err() != nil || p == nil || limits.MaxDataPerSession > 32 || proxy.Issuer != p.trust.Issuer || proxy.Audience != p.trust.Audience || proxy.ClusterTenant != p.trust.ClusterTenant || proxy.ServicePrincipal != p.trust.ServicePrincipal || proxy.WorkerIdentity != p.identity || issuer.WorkerIdentity != p.identity {
		return nil, transportbroker.ErrInvalid
	}
	for _, pairConfig := range []struct{ cert, key []byte }{{proxy.ClientCertificatePEM, proxy.ClientKeyPEM}, {issuer.ClientCertificatePEM, issuer.ClientKeyPEM}} {
		pair, err := tls.X509KeyPair(pairConfig.cert, pairConfig.key)
		if err != nil || len(pair.Certificate) == 0 || !bytes.Equal(pair.Certificate[0], p.certificate) {
			return nil, transportbroker.ErrInvalid
		}
	}
	issue, err := rabbitconnect.NewHTTPIssuer(issuer)
	if err != nil {
		return nil, err
	}
	proxy.RootCAPEM = bytes.Clone(proxy.RootCAPEM)
	proxy.ClientCertificatePEM = bytes.Clone(proxy.ClientCertificatePEM)
	proxy.ClientKeyPEM = bytes.Clone(proxy.ClientKeyPEM)
	proxy.IssuerPublicKey = bytes.Clone(proxy.IssuerPublicKey)
	// Validate the fixed proxy before accepting an execution.
	if _, err := rabbitconnect.New(proxy, issue); err != nil {
		issue.Shutdown(context.Background())
		return nil, err
	}
	life, cancel := context.WithCancel(parent)
	r := &privateOperationRuntime{policy: p, limits: limits, life: life, cancel: cancel, issuer: issue, entries: make(map[transportbroker.Binding]*privateOperationEntry), done: make(chan struct{}), channel: newPrivateOperationChannel, cleanupEnabled: proxy.AcceptedOpenTrust != nil}
	r.opener = func(scope sourceproof.Scope, refresh func(context.Context) (sourceproof.Envelope, error)) (transportbroker.Opener, error) {
		return rabbitconnect.NewWithSourceProof(proxy, issue, rabbitconnect.SourceProofConfig{PublicKey: p.proofKey, Scope: scope, Refresh: refresh})
	}
	r.broker, err = transportbroker.New(limits, r)
	if err != nil {
		cancel()
		issue.Shutdown(context.Background())
		return nil, err
	}
	return r, nil
}

func (r *privateOperationRuntime) Open(ctx context.Context, request transportbroker.OpenRequest) (net.Conn, error) {
	if r == nil || ctx == nil {
		return nil, transportbroker.ErrInvalid
	}
	r.mu.Lock()
	entry := r.entries[request.Binding]
	closed := r.closed
	r.mu.Unlock()
	if entry == nil || closed {
		return nil, transportbroker.ErrClosed
	}
	conn, err := entry.opener.Open(ctx, request)
	if err == nil && entry.cleanup != nil {
		accepted, ok := conn.(*rabbitconnect.AcceptedConn)
		if !ok {
			err = transportbroker.ErrScope
		} else {
			err = entry.cleanup.accept(ctx, accepted)
		}
		if err != nil && conn != nil {
			if closeErr := conn.Close(); closeErr != nil {
				return conn, transportbroker.ErrCleanup
			}
			return nil, err
		}
	}
	return conn, err
}
func (r *privateOperationRuntime) finishLocked() {
	if r.closed && !r.joined && r.pending == 0 && len(r.entries) == 0 {
		r.joined = true
		close(r.done)
	}
}
func (r *privateOperationRuntime) close(ctx context.Context) error {
	if r == nil || ctx == nil {
		return transportbroker.ErrInvalid
	}
	r.mu.Lock()
	r.closed = true
	r.finishLocked()
	r.mu.Unlock()
	r.cancel()
	if err := r.broker.Close(ctx); err != nil {
		return err
	}
	select {
	case <-r.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return r.issuer.Shutdown(ctx)
}
func privateSourceDigest(input adapter.ProcessRequest) ([32]byte, error) {
	raw, err := json.Marshal(input.Source)
	defer clear(raw)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(raw), nil
}

// prepare runs only inside executeResolvedOperation's resource admission. Its
// retained request is immutable and is released after physical transport cleanup.
func (r *privateOperationRuntime) prepare(ctx context.Context, e *Executor, record operationstore.Record, request operations.Request, payload []byte) (adapter.ProcessRequest, *privateOperationChannel, error) {
	return r.prepareMode(ctx, e, record, request, payload, false, 0)
}

func (r *privateOperationRuntime) prepareMode(ctx context.Context, e *Executor, record operationstore.Record, request operations.Request, payload []byte, allowPublic bool, maxBytes int64) (adapter.ProcessRequest, *privateOperationChannel, error) {
	return r.prepareModeWithCleanup(ctx, e, record, request, payload, allowPublic, maxBytes, nil)
}
func (r *privateOperationRuntime) prepareModeWithCleanup(ctx context.Context, e *Executor, record operationstore.Record, request operations.Request, payload []byte, allowPublic bool, maxBytes int64, store *operationstore.Store) (adapter.ProcessRequest, *privateOperationChannel, error) {
	bad := func(err error) (adapter.ProcessRequest, *privateOperationChannel, error) {
		return adapter.ProcessRequest{}, nil, err
	}
	if r == nil || ctx == nil || r.policy == nil || r.broker == nil || request.Validate() != nil || len(payload) > operations.MaxSealedInputBytes {
		return bad(transportbroker.ErrInvalid)
	}
	r.mu.Lock()
	if r.closed || r.life.Err() != nil {
		r.mu.Unlock()
		return bad(transportbroker.ErrClosed)
	}
	if len(r.entries)+r.pending >= r.limits.MaxSessions {
		r.mu.Unlock()
		return bad(transportbroker.ErrCapacity)
	}
	r.pending++
	r.mu.Unlock()
	pending := true
	defer func() {
		if pending {
			r.mu.Lock()
			r.pending--
			r.finishLocked()
			r.mu.Unlock()
		}
	}()
	work, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.life, cancel)
	defer stop()
	defer cancel()
	raw, err := json.Marshal(request)
	defer clear(raw)
	var ownedRequest operations.Request
	if err != nil || operations.DecodeStrict(raw, &ownedRequest, operations.MaxRequestBytes) != nil {
		return bad(transportbroker.ErrInvalid)
	}
	request = ownedRequest
	ownedPayload := bytes.Clone(payload)
	retained := false
	defer func() {
		if !retained {
			clear(ownedPayload)
		}
	}()
	selection, err := r.policy.resolveMode(work, e, record, request, ownedPayload, allowPublic)
	if err != nil {
		return bad(err)
	}
	if maxBytes > 0 {
		selection.input.Limits.MaxBytes = min(selection.input.Limits.MaxBytes, maxBytes)
	}
	if selection.binding == (transportbroker.Binding{}) {
		// The admitted caller retains the public payload until process completion.
		selection.input.Input = payload
		return selection.input, nil, nil
	}
	sourceDigest, err := privateSourceDigest(selection.input)
	if err != nil {
		return bad(err)
	}
	scope, binding := selection.scope, selection.binding
	refresh := func(openContext context.Context) (sourceproof.Envelope, error) {
		current, err := r.policy.resolve(openContext, e, record, request, ownedPayload)
		if err != nil {
			return sourceproof.Envelope{}, err
		}
		digest, err := privateSourceDigest(current.input)
		if err != nil || digest != sourceDigest || current.scope != scope || current.binding != binding {
			return sourceproof.Envelope{}, transportbroker.ErrScope
		}
		return current.proof, nil
	}
	opener, err := r.opener(scope, refresh)
	if err != nil {
		return bad(err)
	}
	entry := &privateOperationEntry{opener: opener}
	if r.cleanupEnabled {
		entry.cleanup, err = newPrivatePostgresCleanup(store, record, selection.input)
		if err != nil {
			return bad(err)
		}
	}
	r.mu.Lock()
	if r.closed || r.entries[selection.binding] != nil {
		r.mu.Unlock()
		return bad(transportbroker.ErrScope)
	}
	r.entries[selection.binding] = entry
	r.pending--
	pending = false
	r.mu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			r.mu.Lock()
			if r.entries[selection.binding] == entry {
				delete(r.entries, selection.binding)
			}
			r.finishLocked()
			r.mu.Unlock()
			clear(ownedPayload)
		})
	}
	session, err := r.broker.Admit(r.life, selection.binding)
	if err != nil {
		release()
		return bad(err)
	}
	retained = true
	var channel *privateOperationChannel
	// Copy only the diagnostic value. Operation cancellation must not shorten
	// the runtime context that owns physical transport cleanup.
	channelContext := privateOpenDiagnosticLifetime(r.life, ctx)
	if entry.cleanup != nil {
		channel, err = newPrivatePostgresChannel(channelContext, selection.input, session, selection.binding, entry.cleanup, release)
	} else {
		channel, err = r.channel(channelContext, selection.input, session, selection.binding, 2*r.limits.MaxDataPerSession, release)
	}
	if err != nil {
		// Return cleanup custody even when IPC construction fails. The operation
		// finalizer must join this session before it releases its resource hold.
		return adapter.ProcessRequest{}, &privateOperationChannel{server: privateUnstartedChannel{}, session: session, binding: selection.binding, release: release}, err
	}
	return selection.input, channel, nil
}

type privateUnstartedChannel struct{}

func (privateUnstartedChannel) Serve(int) error             { return transportbroker.ErrInvalid }
func (privateUnstartedChannel) Close(context.Context) error { return nil }

func (e *Executor) executePrivateOperation(ctx context.Context, r *privateOperationRuntime, cfg OperationProcessConfig, record operationstore.Record, request operations.Request, payload []byte, sink query.Sink) (operations.Receipt, error) {
	if r == nil {
		return operations.Receipt{}, errors.New("private operation runtime is unavailable")
	}
	return e.executeResolvedOperation(ctx, cfg, record.ID, record.RequestSHA256, func(ctx context.Context) (adapter.ProcessRequest, *privateOperationChannel, error) {
		return r.prepare(ctx, e, record, request, payload)
	}, sink)
}
