// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/rabbitconnect"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

type pgCleanupBackend struct {
	mu    sync.Mutex
	entry ledger.Entry
}

func (b *pgCleanupBackend) Get(context.Context, string) (ledger.Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.entry.Revision == 0 {
		return ledger.Entry{}, ledger.ErrMissing
	}
	return ledger.Entry{Value: bytes.Clone(b.entry.Value), Revision: b.entry.Revision}, nil
}
func (b *pgCleanupBackend) Create(ctx context.Context, key string, raw []byte) (uint64, error) {
	return b.Update(ctx, key, raw, 0)
}
func (b *pgCleanupBackend) Update(_ context.Context, _ string, raw []byte, revision uint64) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.entry.Revision != revision {
		return 0, ledger.ErrRevision
	}
	b.entry = ledger.Entry{Value: bytes.Clone(raw), Revision: revision + 1}
	return b.entry.Revision, nil
}

type pgCleanupAccepted struct {
	claims transportissuer.AcceptedOpenClaims
	target rabbitconnect.PostgresTarget
	calls  int
}

func (a *pgCleanupAccepted) Accepted() transportissuer.AcceptedOpenClaims { return a.claims }
func (a *pgCleanupAccepted) AbortPostgres(_ context.Context, target rabbitconnect.PostgresTarget) error {
	a.calls++
	a.target = target
	a.target.Secret = bytes.Clone(target.Secret)
	return nil
}

func newPGCleanupFixture(t *testing.T) (*privatePostgresCleanup, *pgCleanupAccepted) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	store, err := ledger.New(&pgCleanupBackend{}, ledger.Policy{Namespace: "fixture", TenantID: "tenant", Shards: 1, SlotsPerShard: 2, Retention: time.Hour, ExecutionTimeout: time.Minute, LeaseDuration: 10 * time.Second, StorageTimeout: time.Second, MaxCASAttempts: 4})
	if err != nil {
		t.Fatal(err)
	}
	request := operations.Request{Version: 1, Kind: operations.QueryRead, Connection: operations.ConnectionRef{ID: "saved", Database: "data"}, Spec: operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT 1"}}}
	ref, _, _ := operations.SealRequest(request, "input")
	scope := ledger.Scope{Issuer: "issuer", ClusterTenant: "tenant", ServicePrincipal: "api", AppTeam: "customer", SubjectKind: "api_key", SubjectID: "key", ConnectionID: "saved"}
	snapshot, _, err := store.Submit(ctx, ledger.Submission{Scope: scope, Request: request, RequestRef: ref, AuthorityToken: "fixture.token.signature", AuthoritySHA256: operations.GrantDigest("fixture.token.signature"), AuthorityUntil: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	binding := ledger.Binding{WorkerID: "worker", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}
	if _, err := store.Claim(ctx, scope, snapshot.Record.ID, binding); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Start(ctx, scope, snapshot.Record.ID, binding)
	if err != nil {
		t.Fatal(err)
	}
	input := adapter.ProcessRequest{Request: request, Source: adapter.ConnectionSpec{Engine: "postgresql", DSN: "postgresql://fixture:fixture@source.test:5432/data?sslmode=verify-full", Options: map[string]string{"tls_server_name": "database.test"}}}
	cleanup, err := newPrivatePostgresCleanup(store, snapshot.Record, input)
	if err != nil {
		t.Fatal(err)
	}
	accepted := &pgCleanupAccepted{claims: transportissuer.AcceptedOpenClaims{Version: 1, DataTicketSHA256: strings.Repeat("c", 64), AcceptanceID: strings.Repeat("d", 32), AcceptedAt: now.Unix(), ExpiresAt: now.Add(30 * time.Second).Unix()}}
	if err := cleanup.accept(ctx, accepted); err != nil {
		t.Fatal(err)
	}
	return cleanup, accepted
}
func TestPrivatePostgresCleanupRequiresTypedImmutableTargetAndJoinedCompletion(t *testing.T) {
	ctx := context.Background()
	p, a := newPGCleanupFixture(t)
	secret := []byte{1, 2, 3, 4}
	handle, err := p.Register(ctx, 42, secret)
	if err != nil {
		t.Fatal(err)
	}
	secret[0] = 9
	if _, err := p.Register(ctx, 99, []byte{1, 2, 3, 4}); err == nil {
		t.Fatal("backend target replaced")
	}
	if err := p.Abort(ctx, "other-handle"); err == nil || a.calls != 0 {
		t.Fatal("foreign handle reached source")
	}
	if _, err := p.store.Cancel(ctx, p.record.Scope, p.record.ID); err != nil {
		t.Fatal(err)
	}
	if err := p.Abort(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if a.target.PID != 42 || !bytes.Equal(a.target.Secret, []byte{1, 2, 3, 4}) || a.target.TLS.ServerName != "database.test" || a.target.TLS.InsecureSkipVerify {
		t.Fatal("parent target or original TLS changed")
	}
	if err := p.Observed(ctx, handle, true); err != nil {
		t.Fatal(err)
	}
	before, _ := p.store.Get(ctx, p.record.Scope, p.record.ID)
	if before.Record.Terminal() {
		t.Fatal("observation bypassed physical cleanup join")
	}
	if err := p.finish(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ := p.store.Get(ctx, p.record.Scope, p.record.ID)
	if after.Record.Receipt == nil || after.Record.Receipt.ErrorCode != "CANCELLED" {
		t.Fatal("confirmed cleanup missing")
	}
	if err := p.Abort(ctx, handle); err == nil || a.calls != 1 {
		t.Fatal("typed cancellation replayed")
	}
}
func TestPrivatePostgresCleanupDoesNotConfuseAbortDeliveryWithStop(t *testing.T) {
	ctx := context.Background()
	p, _ := newPGCleanupFixture(t)
	handle, err := p.Register(ctx, 42, []byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.store.Cancel(ctx, p.record.Scope, p.record.ID); err != nil {
		t.Fatal(err)
	}
	if err := p.Abort(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if err := p.finish(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ := p.store.Get(ctx, p.record.Scope, p.record.ID)
	if after.Record.State != string(operations.CleanupUnknown) {
		t.Fatal("abort delivery was treated as stopped source")
	}
}
func TestPrivatePostgresCleanupRejectsRegistrationAfterCancellation(t *testing.T) {
	ctx := context.Background()
	p, _ := newPGCleanupFixture(t)
	if _, err := p.store.Cancel(ctx, p.record.Scope, p.record.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Register(ctx, 42, []byte{1, 2, 3, 4}); err == nil {
		t.Fatal("cancelled source authorized adapter SQL")
	}
}
