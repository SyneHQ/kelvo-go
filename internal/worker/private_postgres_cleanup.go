// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"net"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/rabbitconnect"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

// One instance belongs to one pinned child and one accepted DATA open. The
// source TLS policy comes only from the original resolved parent-owned input.
type privateAcceptedSource interface {
	Accepted() transportissuer.AcceptedOpenClaims
	AbortPostgres(context.Context, rabbitconnect.PostgresTarget) error
}
type privatePostgresCleanup struct {
	mu                                   sync.Mutex
	store                                *ledger.Store
	record                               ledger.Record
	tls                                  *tls.Config
	accepted                             privateAcceptedSource
	target                               rabbitconnect.PostgresTarget
	handle                               string
	aborted, observed, stopped, finished bool
}

func newPrivatePostgresCleanup(store *ledger.Store, record ledger.Record, input adapter.ProcessRequest) (*privatePostgresCleanup, error) {
	if store == nil || input.Source.Engine != "postgresql" || input.Request.Kind.Mutating() {
		return nil, adapter.ErrUnsupported
	}
	authority, err := privateSourceAuthority(input.Source)
	if err != nil {
		return nil, err
	}
	host, _, err := net.SplitHostPort(authority)
	if err != nil {
		return nil, adapter.ErrInvalid
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}
	if name := input.Source.Options["tls_server_name"]; name != "" {
		config.ServerName = name
	}
	if pem := input.Source.Options["tls_ca_pem"]; pem != "" {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(pem)) {
			return nil, adapter.ErrInvalid
		}
		config.RootCAs = roots
	}
	return &privatePostgresCleanup{store: store, record: record, tls: config}, nil
}
func (p *privatePostgresCleanup) accept(ctx context.Context, conn privateAcceptedSource) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn == nil || p.accepted != nil || p.finished {
		return transportbroker.ErrScope
	}
	a := conn.Accepted()
	until := time.Unix(a.ExpiresAt, 0)
	if p.record.ExecuteBefore.Before(until) {
		until = p.record.ExecuteBefore
	}
	_, err := p.store.RegisterAcceptedCleanup(ctx, p.record.Scope, p.record.ID, p.record.Binding, a.DataTicketSHA256, a.AcceptanceID, until)
	if err != nil {
		return err
	}
	p.accepted = conn
	return nil
}
func (p *privatePostgresCleanup) Register(ctx context.Context, pid uint32, secret []byte) (string, error) {
	if pid == 0 || len(secret) < 4 || len(secret) > rabbitconnect.MaxPostgresCancelKeyBytes {
		return "", adapter.ErrInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.accepted == nil || p.handle != "" || p.finished {
		return "", transportbroker.ErrScope
	}
	if _, err := p.store.Current(ctx, p.record.Scope, p.record.ID, p.record.Binding); err != nil {
		return "", err
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", transportbroker.ErrOpen
	}
	p.handle = hex.EncodeToString(nonce[:])
	p.target = rabbitconnect.PostgresTarget{PID: pid, Secret: bytes.Clone(secret), TLS: p.tls.Clone()}
	return p.handle, nil
}
func (p *privatePostgresCleanup) Abort(ctx context.Context, handle string) error {
	p.mu.Lock()
	if p.accepted == nil || p.handle == "" || handle != p.handle || p.aborted || p.finished {
		p.mu.Unlock()
		return transportbroker.ErrScope
	}
	p.aborted = true
	accepted, target := p.accepted, p.target
	target.Secret = bytes.Clone(target.Secret)
	p.mu.Unlock()
	defer clear(target.Secret)
	a := accepted.Accepted()
	current, err := p.store.CurrentCleanup(ctx, p.record.Scope, p.record.ID, p.record.Binding, a.DataTicketSHA256, a.AcceptanceID)
	if err != nil {
		return err
	}
	bounded, cancel := context.WithDeadline(ctx, current.Record.AcceptedCleanup.CleanupUntil)
	defer cancel()
	return accepted.AbortPostgres(bounded, target)
}
func (p *privatePostgresCleanup) Observed(ctx context.Context, handle string, stopped bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ctx == nil || ctx.Err() != nil || p.accepted == nil || p.handle == "" || handle != p.handle || !p.aborted || p.observed || p.finished {
		return transportbroker.ErrScope
	}
	a := p.accepted.Accepted()
	if _, err := p.store.CurrentCleanup(ctx, p.record.Scope, p.record.ID, p.record.Binding, a.DataTicketSHA256, a.AcceptanceID); err != nil {
		return err
	}
	p.observed, p.stopped = true, stopped
	return nil
}

// finish runs only after the executor joins the child, transport and scratch.
// An observed protocol stop cannot complete the ledger before physical cleanup.
func (p *privatePostgresCleanup) finish(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return nil
	}
	p.finished = true
	defer clear(p.target.Secret)
	if p.accepted == nil {
		return nil
	}
	current, err := p.store.Get(ctx, p.record.Scope, p.record.ID)
	if err != nil {
		return err
	}
	if current.Record.Terminal() || current.Record.State != ledger.Cancelling {
		return nil
	}
	a := p.accepted.Accepted()
	_, err = p.store.CompleteCleanup(ctx, p.record.Scope, p.record.ID, p.record.Binding, a.DataTicketSHA256, a.AcceptanceID, p.observed && p.stopped)
	return err
}
