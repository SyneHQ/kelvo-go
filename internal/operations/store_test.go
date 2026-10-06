// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/SYNEHQ/kelvo-go/operations"
)

type memoryBackend struct {
	mu             sync.Mutex
	entries        map[string]Entry
	revision       uint64
	loseNextAck    bool
	conflictAlways bool
	updates        int
}

func (b *memoryBackend) Get(ctx context.Context, key string) (Entry, error) {
	if ctx.Err() != nil {
		return Entry{}, ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.entries[key]
	if !ok {
		return Entry{}, ErrMissing
	}
	v.Value = append([]byte(nil), v.Value...)
	return v, nil
}
func (b *memoryBackend) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	return b.write(ctx, key, value, 0)
}
func (b *memoryBackend) Update(ctx context.Context, key string, value []byte, rev uint64) (uint64, error) {
	return b.write(ctx, key, value, rev)
}
func (b *memoryBackend) write(ctx context.Context, key string, value []byte, rev uint64) (uint64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.updates++
	if b.conflictAlways {
		return 0, ErrRevision
	}
	current, exists := b.entries[key]
	if (rev == 0 && exists) || (rev != 0 && (!exists || current.Revision != rev)) {
		return 0, ErrRevision
	}
	b.revision++
	b.entries[key] = Entry{Value: append([]byte(nil), value...), Revision: b.revision}
	if b.loseNextAck {
		b.loseNextAck = false
		return 0, errors.New("private transport diagnostic")
	}
	return b.revision, nil
}

type fixture struct {
	store   *Store
	backend *memoryBackend
	now     time.Time
	scope   Scope
	binding Binding
	input   Submission
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	backend := &memoryBackend{entries: map[string]Entry{}}
	policy := Policy{Namespace: "shared", TenantID: "shared", Shards: 1, SlotsPerShard: 8, Retention: time.Hour, ExecutionTimeout: time.Minute, LeaseDuration: 10 * time.Second, StorageTimeout: time.Second, MaxCASAttempts: 32}
	store, err := New(backend, policy)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{store: store, backend: backend, now: time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC), scope: Scope{Issuer: "app", ClusterTenant: "shared", ServicePrincipal: "gateway", AppTeam: "team-a", SubjectKind: "api_key", SubjectID: "key-a", ConnectionID: "saved-a"}, binding: Binding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}}
	store.now = func() time.Time { return f.now }
	r := api.Request{Version: api.Version, Kind: api.StatementExecute, Connection: api.ConnectionRef{ID: "saved-a", Database: "app"}, IdempotencyKey: "change-1", Spec: api.Spec{Statement: &api.StatementSpec{SQL: "UPDATE accounts SET n = n + 1", Transaction: api.TransactionRequired}}}
	ref, _, err := api.SealRequest(r, "request-a")
	if err != nil {
		t.Fatal(err)
	}
	f.input = Submission{Scope: f.scope, Request: r, RequestRef: ref, AuthorityToken: "store.fixture.token", AuthoritySHA256: api.GrantDigest("store.fixture.token"), AuthorityUntil: f.now.Add(time.Minute)}
	return f
}
func (f *fixture) submit(t *testing.T) Snapshot {
	t.Helper()
	s, duplicate, err := f.store.Submit(context.Background(), f.input)
	if err != nil || duplicate {
		t.Fatal("submit", duplicate, err)
	}
	return s
}
func (f *fixture) running(t *testing.T) Snapshot {
	t.Helper()
	s := f.submit(t)
	var err error
	s, err = f.store.Claim(context.Background(), f.scope, s.Record.ID, f.binding)
	if err != nil {
		t.Fatal(err)
	}
	s, err = f.store.Start(context.Background(), f.scope, s.Record.ID, f.binding)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func completed(s Snapshot) api.Receipt {
	return api.Receipt{Version: api.Version, OperationID: s.Record.ID, RequestSHA256: s.Record.RequestSHA256, Outcome: api.Completed, Effect: api.EffectCommitted}
}

func TestConcurrentDuplicateSubmissionHasOneIdentity(t *testing.T) {
	f := newFixture(t)
	const n = 32
	results := make(chan Snapshot, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, _, err := f.store.Submit(context.Background(), f.input)
			results <- s
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	id := ""
	for s := range results {
		if id == "" {
			id = s.Record.ID
		}
		if s.Record.ID != id {
			t.Fatal("duplicate acquired distinct operation")
		}
	}
	entry, err := f.backend.Get(context.Background(), f.store.key(0))
	if err != nil {
		t.Fatal(err)
	}
	var d document
	if json.Unmarshal(entry.Value, &d) != nil || len(d.Records) != 1 {
		t.Fatal("duplicate admission used extra retention")
	}
	changed := f.input
	changed.Request, _ = api.Clone(changed.Request)
	changed.Request.Spec.Statement.SQL = "DELETE FROM accounts"
	changed.RequestRef, _, _ = api.SealRequest(changed.Request, "request-b")
	if _, _, err := f.store.Submit(context.Background(), changed); !errors.Is(err, ErrConflict) {
		t.Fatal("changed retry did not conflict", err)
	}
}

func TestConcurrentStartAllowsOnlyOneSourceDispatch(t *testing.T) {
	f := newFixture(t)
	s := f.submit(t)
	if _, err := f.store.Claim(context.Background(), f.scope, s.Record.ID, f.binding); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.store.Start(context.Background(), f.scope, s.Record.ID, f.binding); err == nil {
				calls.Add(1)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("source dispatch count", calls.Load())
	}
}

func TestLostStartAcknowledgementNeverAuthorizesReplay(t *testing.T) {
	f := newFixture(t)
	s := f.submit(t)
	if _, err := f.store.Claim(context.Background(), f.scope, s.Record.ID, f.binding); err != nil {
		t.Fatal(err)
	}
	f.backend.loseNextAck = true
	if _, err := f.store.Start(context.Background(), f.scope, s.Record.ID, f.binding); !errors.Is(err, ErrUnavailable) {
		t.Fatal("lost ACK authorized execution", err)
	}
	if _, err := f.store.Start(context.Background(), f.scope, s.Record.ID, f.binding); !errors.Is(err, ErrConflict) {
		t.Fatal("running mutation was replayed", err)
	}
	f.now = f.now.Add(11 * time.Second)
	got, err := f.store.Get(context.Background(), f.scope, s.Record.ID)
	if err != nil || got.Record.State != string(api.OutcomeUnknown) || got.Record.Receipt.Effect != api.EffectUnknown {
		t.Fatal("lost custody not uncertain", got.Record.State, err)
	}
	if _, err := f.store.Complete(context.Background(), f.scope, s.Record.ID, f.binding, completed(s)); !errors.Is(err, ErrConflict) {
		t.Fatal("late outcome silently replaced uncertainty", err)
	}
}

func TestLostSubmitAndCompletionAcknowledgementsRetainEvidence(t *testing.T) {
	f := newFixture(t)
	f.backend.loseNextAck = true
	if _, _, err := f.store.Submit(context.Background(), f.input); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	s, duplicate, err := f.store.Submit(context.Background(), f.input)
	if err != nil || !duplicate {
		t.Fatal("lost submit duplicated work", duplicate, err)
	}
	if _, err = f.store.Claim(context.Background(), f.scope, s.Record.ID, f.binding); err != nil {
		t.Fatal(err)
	}
	s, err = f.store.Start(context.Background(), f.scope, s.Record.ID, f.binding)
	if err != nil {
		t.Fatal(err)
	}
	receipt := completed(s)
	f.backend.loseNextAck = true
	if _, err = f.store.Complete(context.Background(), f.scope, s.Record.ID, f.binding, receipt); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	got, err := f.store.Complete(context.Background(), f.scope, s.Record.ID, f.binding, receipt)
	if err != nil || got.Record.State != string(api.Completed) {
		t.Fatal("durable receipt lost", err)
	}
	receipt.Effect = api.EffectNone
	if _, err = f.store.Complete(context.Background(), f.scope, s.Record.ID, f.binding, receipt); !errors.Is(err, ErrConflict) {
		t.Fatal("conflicting receipt replaced result", err)
	}
}

func TestTenantAndCurrentCustodyAreBound(t *testing.T) {
	f := newFixture(t)
	s := f.running(t)
	for _, mutate := range []func(*Scope){func(s *Scope) { s.AppTeam = "team-b" }, func(s *Scope) { s.SubjectID = "key-b" }, func(s *Scope) { s.ConnectionID = "saved-b" }, func(s *Scope) { s.Issuer = "other-app" }, func(s *Scope) { s.ClusterTenant = "other-cluster" }} {
		other := f.scope
		mutate(&other)
		if _, err := f.store.Get(context.Background(), other, s.Record.ID); !errors.Is(err, ErrNotFound) {
			t.Fatal("foreign authority read record", err)
		}
	}
	wrong := f.binding
	wrong.Owner = strings.Repeat("d", 32)
	if _, err := f.store.Current(context.Background(), f.scope, s.Record.ID, wrong); !errors.Is(err, ErrConflict) {
		t.Fatal("wrong process owner admitted", err)
	}
	f.now = f.now.Add(10 * time.Second)
	if _, err := f.store.Renew(context.Background(), f.scope, s.Record.ID, f.binding); !errors.Is(err, ErrConflict) {
		t.Fatal("expired lease resurrected", err)
	}
}

func TestSharedServiceSameKeyDoesNotCollapseApplicationTeams(t *testing.T) {
	f := newFixture(t)
	first := f.submit(t)
	other := f.input
	other.Scope.AppTeam = "team-b"
	second, duplicate, err := f.store.Submit(context.Background(), other)
	if err != nil || duplicate || second.Record.ID == first.Record.ID {
		t.Fatal("team identity collapsed", duplicate, err)
	}
}

func TestJobRunsHaveDistinctDeduplicationAndHandleScope(t *testing.T) {
	f := newFixture(t)
	f.scope.SubjectKind = "job"
	f.scope.SubjectID = "scheduler-user"
	f.scope.SubjectJobID = "run-a"
	f.input.Scope = f.scope
	first := f.submit(t)
	other := f.input
	other.Scope.SubjectJobID = "run-b"
	second, duplicate, err := f.store.Submit(context.Background(), other)
	if err != nil || duplicate || second.Record.ID == first.Record.ID {
		t.Fatal("job identities collapsed", duplicate, err)
	}
	if _, err := f.store.Get(context.Background(), other.Scope, first.Record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("different run accessed handle", err)
	}
	other.Scope.SubjectJobID = ""
	if _, _, err := f.store.Submit(context.Background(), other); !errors.Is(err, ErrInvalid) {
		t.Fatal("job without run identity accepted", err)
	}
}

func TestRetentionCoversGrantValidityAndQueueWakeRecovery(t *testing.T) {
	f := newFixture(t)
	f.store.policy.Retention = 61 * time.Second
	raw, _ := json.Marshal(f.store.policy)
	f.store.policySHA256 = digestBytes(raw)
	f.input.AuthorityUntil = f.now.Add(5 * time.Minute)
	s := f.submit(t)
	if s.Record.RetainUntil.Before(f.input.AuthorityUntil.Add(30 * time.Second)) {
		t.Fatal("retention shorter than usable authority")
	}
	queued, err := f.store.QueuedShard(context.Background(), 0)
	if err != nil || len(queued) != 1 || queued[0].ID != s.Record.ID {
		t.Fatal("accepted job not discoverable after lost wake", err)
	}
	if _, err := f.store.Claim(context.Background(), f.scope, s.Record.ID, f.binding); err != nil {
		t.Fatal(err)
	}
	queued, err = f.store.QueuedShard(context.Background(), 0)
	if err != nil || len(queued) != 0 {
		t.Fatal("assigned job queued again", err)
	}
	f.now = f.now.Add(11 * time.Second)
	if err := f.store.RecoverShard(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	queued, err = f.store.QueuedShard(context.Background(), 0)
	if err != nil || len(queued) != 0 {
		t.Fatal("expired assignment silently replayed", err)
	}
	got, err := f.store.Get(context.Background(), f.scope, s.Record.ID)
	if err != nil || got.Record.Receipt.Outcome != api.Rejected || got.Record.Receipt.Effect != api.EffectNone {
		t.Fatal("prestart expiry was not effect-free", err)
	}
	if got.Record.RetainUntil.Before(f.input.AuthorityUntil.Add(30 * time.Second)) {
		t.Fatal("terminal receipt shortened grant retention")
	}
}

func TestCancellationBeforeAndAfterStartHaveDifferentEffects(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[started], func(t *testing.T) {
			f := newFixture(t)
			var s Snapshot
			if started {
				s = f.running(t)
			} else {
				s = f.submit(t)
			}
			got, err := f.store.Cancel(context.Background(), f.scope, s.Record.ID)
			if err != nil {
				t.Fatal(err)
			}
			if started {
				if got.Record.Receipt.Outcome != api.OutcomeUnknown || got.Record.Receipt.Effect != api.EffectUnknown {
					t.Fatal("cancellation claimed rollback")
				}
			} else {
				if got.Record.Receipt.Outcome != api.CancelledBeforeStart || got.Record.Receipt.Effect != api.EffectNone {
					t.Fatal("prestart cancellation lost certainty")
				}
			}
			if _, err := f.store.Claim(context.Background(), f.scope, s.Record.ID, f.binding); !errors.Is(err, ErrConflict) {
				t.Fatal("cancelled operation reassigned", err)
			}
		})
	}
}

func TestExpiredRunningRecordGetsFreshRetainedTombstone(t *testing.T) {
	f := newFixture(t)
	f.store.policy.SlotsPerShard = 1
	raw, _ := json.Marshal(f.store.policy)
	sum := digestBytes(raw)
	f.store.policySHA256 = sum
	s := f.running(t)
	f.now = f.now.Add(2 * time.Hour)
	other := f.input
	other.Request, _ = api.Clone(other.Request)
	other.Request.IdempotencyKey = "other-operation"
	other.RequestRef, _, _ = api.SealRequest(other.Request, "request-b")
	other.AuthorityUntil = f.now.Add(time.Minute)
	if _, _, err := f.store.Submit(context.Background(), other); !errors.Is(err, ErrCapacity) {
		t.Fatal("active expired record was evicted", err)
	}
	got, err := f.store.Get(context.Background(), f.scope, s.Record.ID)
	if err != nil || got.Record.State != string(api.OutcomeUnknown) || !got.Record.RetainUntil.Equal(f.now.Add(time.Hour)) {
		t.Fatal("uncertain receipt not retained", err)
	}
	f.now = f.now.Add(time.Hour + time.Second)
	other.AuthorityUntil = f.now.Add(time.Minute)
	if _, _, err := f.store.Submit(context.Background(), other); err != nil {
		t.Fatal("terminal retention was never reclaimable", err)
	}
}

func digestBytes(raw []byte) string { /* matches the immutable policy fingerprint */
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func TestSealedRequestReferenceAndSnapshotOwnership(t *testing.T) {
	f := newFixture(t)
	bad := f.input
	bad.RequestRef.SHA256 = strings.Repeat("0", 64)
	if _, _, err := f.store.Submit(context.Background(), bad); !errors.Is(err, ErrInvalid) {
		t.Fatal("incorrect sealed digest admitted", err)
	}
	s := f.running(t)
	receipt := completed(s)
	count := int64(5)
	receipt.AffectedRows = &count
	got, err := f.store.Complete(context.Background(), f.scope, s.Record.ID, f.binding, receipt)
	if err != nil {
		t.Fatal(err)
	}
	*receipt.AffectedRows = 999
	*got.Record.Receipt.AffectedRows = 888
	again, err := f.store.Get(context.Background(), f.scope, s.Record.ID)
	if err != nil || *again.Record.Receipt.AffectedRows != 5 {
		t.Fatal("caller mutated retained receipt", err)
	}
	entry, err := f.backend.Get(context.Background(), f.store.key(0))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(entry.Value), "UPDATE accounts") || strings.Contains(string(entry.Value), "parameters") {
		t.Fatal("request body persisted in queue metadata")
	}
}

func TestCASRetryIsBoundedAndCorruptionFailsClosed(t *testing.T) {
	f := newFixture(t)
	f.backend.conflictAlways = true
	if _, _, err := f.store.Submit(context.Background(), f.input); !errors.Is(err, ErrUnavailable) || f.backend.updates != f.store.policy.MaxCASAttempts {
		t.Fatal("unbounded retries", f.backend.updates, err)
	}
	f = newFixture(t)
	s := f.submit(t)
	f.backend.mu.Lock()
	entry := f.backend.entries[f.store.key(0)]
	entry.Value = []byte(`{"version":1,"version":1}`)
	f.backend.entries[f.store.key(0)] = entry
	f.backend.mu.Unlock()
	if _, err := f.store.Get(context.Background(), f.scope, s.Record.ID); !errors.Is(err, ErrUnavailable) {
		t.Fatal("corrupt record accepted", err)
	}
}

func TestAuthorityIntegrityAndAssignedRejection(t *testing.T) {
	f := newFixture(t)
	bad := f.input
	bad.AuthorityToken += "changed"
	if _, _, err := f.store.Submit(context.Background(), bad); !errors.Is(err, ErrInvalid) {
		t.Fatal("changed grant admitted", err)
	}
	bad = f.input
	bad.AuthorityToken = strings.Repeat("a", api.MaxGrantBytes+1)
	bad.AuthoritySHA256 = api.GrantDigest(bad.AuthorityToken)
	if _, _, err := f.store.Submit(context.Background(), bad); !errors.Is(err, ErrInvalid) {
		t.Fatal("oversized grant admitted", err)
	}
	s := f.submit(t)
	if s.Record.AuthorityToken != f.input.AuthorityToken {
		t.Fatal("restart authority missing")
	}
	if _, err := f.store.RejectBeforeStart(context.Background(), f.scope, s.Record.ID, f.binding, "PERMISSION_DENIED"); !errors.Is(err, ErrConflict) {
		t.Fatal("unowned rejection accepted", err)
	}
	if _, err := f.store.Claim(context.Background(), f.scope, s.Record.ID, f.binding); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.RejectBeforeStart(context.Background(), f.scope, s.Record.ID, f.binding, "private source error"); !errors.Is(err, ErrInvalid) {
		t.Fatal("private diagnostic retained", err)
	}
	got, err := f.store.RejectBeforeStart(context.Background(), f.scope, s.Record.ID, f.binding, "PERMISSION_DENIED")
	if err != nil || got.Record.State != string(api.Rejected) || got.Record.Receipt.Effect != api.EffectNone {
		t.Fatal("rejection lost certainty", err)
	}
	if _, err := f.store.Start(context.Background(), f.scope, s.Record.ID, f.binding); !errors.Is(err, ErrConflict) {
		t.Fatal("rejected operation started", err)
	}
}
