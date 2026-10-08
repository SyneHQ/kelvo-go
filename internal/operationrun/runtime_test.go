// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operationrun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	api "github.com/SYNEHQ/kelvo-go/operations"
)

type backend struct {
	mu                sync.Mutex
	entries           map[string]ledger.Entry
	revision          uint64
	loseState         string
	failState         string
	loseUntilDeadline bool
	lost              bool
}

func (b *backend) Get(ctx context.Context, key string) (ledger.Entry, error) {
	if ctx.Err() != nil {
		return ledger.Entry{}, ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.entries[key]
	if !ok {
		return ledger.Entry{}, ledger.ErrMissing
	}
	v.Value = append([]byte(nil), v.Value...)
	return v, nil
}
func (b *backend) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	return b.write(ctx, key, value, 0)
}
func (b *backend) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	return b.write(ctx, key, value, revision)
}
func (b *backend) write(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.entries[key]
	if (revision == 0 && ok) || (revision != 0 && (!ok || v.Revision != revision)) {
		return 0, ledger.ErrRevision
	}
	var proposed struct {
		Records []ledger.Record `json:"records"`
	}
	_ = json.Unmarshal(value, &proposed)
	for _, record := range proposed.Records {
		if b.failState != "" && record.State == b.failState {
			return 0, errors.New("private fixture storage failure")
		}
	}
	b.revision++
	b.entries[key] = ledger.Entry{Value: append([]byte(nil), value...), Revision: b.revision}
	var document struct {
		Records []ledger.Record `json:"records"`
	}
	_ = json.Unmarshal(value, &document)
	for _, record := range document.Records {
		if !b.lost && b.loseState != "" && record.State == b.loseState {
			b.lost = true
			if b.loseUntilDeadline {
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return 0, errors.New("private transport error after persisted write")
		}
	}
	return b.revision, nil
}

type prepared struct {
	execute func(context.Context) (api.Receipt, error)
	close   func() error
}

func (p *prepared) Execute(ctx context.Context) (api.Receipt, error) { return p.execute(ctx) }
func (p *prepared) Close() error {
	if p.close != nil {
		return p.close()
	}
	return nil
}

type fixture struct {
	t                    *testing.T
	store                *ledger.Store
	backend              *backend
	scope                ledger.Scope
	request              api.Request
	input                ledger.Submission
	calls, audits        atomic.Int32
	execute              func(context.Context, ledger.Record, ledger.Binding) (api.Receipt, error)
	expectedRuntimeError error
}

func newFixture(t *testing.T, timeout time.Duration) *fixture {
	t.Helper()
	b := &backend{entries: map[string]ledger.Entry{}}
	p := ledger.Policy{Namespace: "runtime", TenantID: "shared", Shards: 1, SlotsPerShard: 8, Retention: time.Hour, ExecutionTimeout: timeout, LeaseDuration: time.Second, StorageTimeout: time.Second, MaxCASAttempts: 32}
	s, err := ledger.New(b, p)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, store: s, backend: b, scope: ledger.Scope{Issuer: "application", ClusterTenant: "shared", ServicePrincipal: "gateway", AppTeam: "team-a", SubjectKind: "api_key", SubjectID: "key-a", ConnectionID: "saved-a"}}
	f.request = api.Request{Version: api.Version, Kind: api.StatementExecute, Connection: api.ConnectionRef{ID: "saved-a"}, IdempotencyKey: "change-a", Spec: api.Spec{Statement: &api.StatementSpec{SQL: "UPDATE counter SET n = n + 1", Transaction: api.TransactionRequired}}}
	ref, _, err := api.SealRequest(f.request, "request-a")
	if err != nil {
		t.Fatal(err)
	}
	f.input = ledger.Submission{Scope: f.scope, Request: f.request, RequestRef: ref, AuthorityToken: "store.fixture.token", AuthoritySHA256: api.GrantDigest("store.fixture.token"), AuthorityUntil: time.Now().Add(time.Minute)}
	f.execute = func(ctx context.Context, record ledger.Record, binding ledger.Binding) (api.Receipt, error) {
		if _, err := s.Current(ctx, record.Scope, record.ID, binding); err != nil {
			return api.Receipt{}, err
		}
		return api.Receipt{Outcome: api.Completed, Effect: api.EffectCommitted}, nil
	}
	return f
}
func (f *fixture) submit() ledger.Snapshot {
	f.t.Helper()
	s, dup, err := f.store.Submit(context.Background(), f.input)
	if err != nil || dup {
		f.t.Fatal("submit", dup, err)
	}
	return s
}
func (f *fixture) hooks() Hooks {
	return Hooks{
		Load: func(context.Context, ledger.Record) (api.Request, error) { return api.Clone(f.request) },
		Authorize: func(ctx context.Context, record ledger.Record, request api.Request, binding ledger.Binding) error {
			s, err := f.store.Get(ctx, record.Scope, record.ID)
			if err != nil {
				return err
			}
			if s.Record.Binding != binding || (s.Record.State != ledger.Assigned && s.Record.State != ledger.Running) {
				return ledger.ErrConflict
			}
			return nil
		},
		Prepare: func(_ context.Context, record ledger.Record, _ api.Request, binding ledger.Binding) (Prepared, error) {
			return &prepared{execute: func(ctx context.Context) (api.Receipt, error) { f.calls.Add(1); return f.execute(ctx, record, binding) }}, nil
		},
		AuditAdmission: func(context.Context, ledger.Record, api.Request) error { f.audits.Add(1); return nil },
	}
}
func (f *fixture) runtime(worker string, concurrency int, hooks Hooks) *Runtime {
	f.t.Helper()
	r, err := New(f.store, Config{WorkerID: worker, Owner: strings.Repeat("a", 32), Concurrency: concurrency, PollInterval: 10 * time.Millisecond}, hooks)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := r.Close(ctx); !errors.Is(err, f.expectedRuntimeError) {
			f.t.Error("close", err)
		}
	})
	return r
}
func start(t *testing.T, r *Runtime) {
	t.Helper()
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func waitRecord(t *testing.T, f *fixture, id string, test func(ledger.Record) bool) ledger.Record {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		s, err := f.store.Get(context.Background(), f.scope, id)
		if err == nil && test(s.Record) {
			return s.Record
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("record did not reach expected state")
	return ledger.Record{}
}
func terminal(t *testing.T, f *fixture, id string) ledger.Record {
	return waitRecord(t, f, id, func(r ledger.Record) bool { return r.Terminal() })
}

func TestAutonomousSharedWorkersDispatchOnce(t *testing.T) {
	f := newFixture(t, 5*time.Second)
	s := f.submit()
	for range 5 {
		got, duplicate, err := f.store.Submit(context.Background(), f.input)
		if err != nil || !duplicate || got.Record.ID != s.Record.ID {
			t.Fatal("duplicate", duplicate, err)
		}
	}
	// Status reads cannot execute an operation without a runtime.
	for range 5 {
		if _, err := f.store.Get(context.Background(), f.scope, s.Record.ID); err != nil {
			t.Fatal(err)
		}
	}
	if f.calls.Load() != 0 {
		t.Fatal("GET executed source")
	}
	a, b := f.runtime("worker-a", 2, f.hooks()), f.runtime("worker-b", 2, f.hooks())
	start(t, a)
	start(t, b)
	got := terminal(t, f, s.Record.ID)
	if got.Receipt.Outcome != api.Completed || f.calls.Load() != 1 || f.audits.Load() != 1 {
		t.Fatal("duplicate dispatch or missing audit", got.State, f.calls.Load(), f.audits.Load())
	}
	for range 10 {
		a.Wake()
		b.Wake()
	}
	if err := a.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.calls.Load() != 1 {
		t.Fatal("replayed after completion")
	}
}

func TestUnknownClaimOrStartAcknowledgementNeverDispatches(t *testing.T) {
	for _, state := range []string{ledger.Assigned, ledger.Running} {
		t.Run(state, func(t *testing.T) {
			f := newFixture(t, 3*time.Second)
			f.backend.loseState = state
			s := f.submit()
			r := f.runtime("worker-a", 1, f.hooks())
			start(t, r)
			got := terminal(t, f, s.Record.ID)
			if f.calls.Load() != 0 {
				t.Fatal("unknown write acknowledgement dispatched")
			}
			want := api.Rejected
			if state == ledger.Running {
				want = api.OutcomeUnknown
			}
			if got.Receipt.Outcome != want {
				t.Fatal("lost custody outcome", got.State)
			}
		})
	}
}

func TestCompletionSurvivesLostAckAndDeliveryError(t *testing.T) {
	f := newFixture(t, 5*time.Second)
	f.expectedRuntimeError = ErrDelivery
	f.backend.loseState = string(api.Completed)
	f.execute = func(context.Context, ledger.Record, ledger.Binding) (api.Receipt, error) {
		return api.Receipt{Outcome: api.Completed, Effect: api.EffectCommitted}, errors.New("private response delivery failed")
	}
	s := f.submit()
	r := f.runtime("worker-a", 1, f.hooks())
	start(t, r)
	got := terminal(t, f, s.Record.ID)
	if got.Receipt.Outcome != api.Completed || got.Receipt.ErrorCode != "" || f.calls.Load() != 1 {
		t.Fatal("confirmed outcome discarded", got.State)
	}
	if err := r.Drain(context.Background()); !errors.Is(err, ErrDelivery) {
		t.Fatal(err)
	}
	if _, duplicate, err := f.store.Submit(context.Background(), f.input); err != nil || !duplicate {
		t.Fatal("completed mutation was replayable", err)
	}
}

func TestRejectionsNeverReachSource(t *testing.T) {
	for _, stage := range []string{"load", "digest", "authorize", "prepare", "audit"} {
		t.Run(stage, func(t *testing.T) {
			f := newFixture(t, 5*time.Second)
			s := f.submit()
			hooks := f.hooks()
			switch stage {
			case "load":
				hooks.Load = func(context.Context, ledger.Record) (api.Request, error) {
					return api.Request{}, errors.New("private load detail")
				}
			case "digest":
				hooks.Load = func(context.Context, ledger.Record) (api.Request, error) {
					request, _ := api.Clone(f.request)
					request.Spec.Statement.SQL = "DELETE FROM counter"
					return request, nil
				}
			case "authorize":
				hooks.Authorize = func(context.Context, ledger.Record, api.Request, ledger.Binding) error { return errors.New("revoked") }
			case "prepare":
				hooks.Prepare = func(context.Context, ledger.Record, api.Request, ledger.Binding) (Prepared, error) {
					return nil, api.ErrUnsupported
				}
			case "audit":
				hooks.AuditAdmission = func(context.Context, ledger.Record, api.Request) error { return errors.New("audit capacity full") }
			}
			r := f.runtime("worker-a", 1, hooks)
			start(t, r)
			got := terminal(t, f, s.Record.ID)
			if got.Receipt.Outcome != api.Rejected || got.Receipt.Effect != api.EffectNone || f.calls.Load() != 0 {
				t.Fatal("prestart failure dispatched", got.State)
			}
		})
	}
}

func TestCancellationBeforeStartDoesNotDispatch(t *testing.T) {
	f := newFixture(t, 5*time.Second)
	s := f.submit()
	if _, err := f.store.Cancel(context.Background(), f.scope, s.Record.ID); err != nil {
		t.Fatal(err)
	}
	r := f.runtime("worker-a", 1, f.hooks())
	start(t, r)
	if err := r.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := terminal(t, f, s.Record.ID)
	if got.Receipt.Outcome != api.CancelledBeforeStart || f.calls.Load() != 0 {
		t.Fatal("cancelled work dispatched")
	}
}

func TestRunningCancellationStopsWorkerAndPreservesUnknown(t *testing.T) {
	f := newFixture(t, 5*time.Second)
	entered, exited := make(chan struct{}), make(chan struct{})
	f.execute = func(ctx context.Context, _ ledger.Record, _ ledger.Binding) (api.Receipt, error) {
		close(entered)
		<-ctx.Done()
		close(exited)
		return api.Receipt{Outcome: api.Completed, Effect: api.EffectCommitted}, nil
	}
	s := f.submit()
	r := f.runtime("worker-a", 1, f.hooks())
	start(t, r)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("no dispatch")
	}
	if _, err := f.store.Cancel(context.Background(), f.scope, s.Record.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("custody loss did not cancel source")
	}
	if err := r.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := terminal(t, f, s.Record.ID)
	if got.Receipt.Outcome != api.OutcomeUnknown || f.calls.Load() != 1 {
		t.Fatal("late completion replaced uncertainty", got.State)
	}
}

func TestDeadlineStopsMutationWithoutReplay(t *testing.T) {
	f := newFixture(t, time.Second)
	f.execute = func(ctx context.Context, _ ledger.Record, _ ledger.Binding) (api.Receipt, error) {
		<-ctx.Done()
		return api.Receipt{}, ctx.Err()
	}
	s := f.submit()
	r := f.runtime("worker-a", 1, f.hooks())
	start(t, r)
	got := terminal(t, f, s.Record.ID)
	if err := r.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got.Receipt.Outcome != api.OutcomeUnknown || got.Receipt.Effect != api.EffectUnknown || f.calls.Load() != 1 {
		t.Fatal("deadline claimed rollback", got.State)
	}
}

func TestHeartbeatRechecksCurrentAuthorization(t *testing.T) {
	f := newFixture(t, 5*time.Second)
	var revoked atomic.Bool
	f.execute = func(ctx context.Context, _ ledger.Record, _ ledger.Binding) (api.Receipt, error) {
		revoked.Store(true)
		<-ctx.Done()
		return api.Receipt{}, ctx.Err()
	}
	hooks := f.hooks()
	original := hooks.Authorize
	hooks.Authorize = func(ctx context.Context, record ledger.Record, request api.Request, binding ledger.Binding) error {
		if revoked.Load() {
			return errors.New("authority removed")
		}
		return original(ctx, record, request, binding)
	}
	s := f.submit()
	r := f.runtime("worker-a", 1, hooks)
	start(t, r)
	got := terminal(t, f, s.Record.ID)
	if got.Receipt.Outcome != api.OutcomeUnknown || f.calls.Load() != 1 {
		t.Fatal("revoked authority continued or replayed", got.State)
	}
}

func TestCloseCancelsAndDrainWaits(t *testing.T) {
	for _, closing := range []bool{false, true} {
		t.Run(fmt.Sprint(closing), func(t *testing.T) {
			f := newFixture(t, 5*time.Second)
			entered, release := make(chan struct{}), make(chan struct{})
			f.execute = func(ctx context.Context, _ ledger.Record, _ ledger.Binding) (api.Receipt, error) {
				close(entered)
				select {
				case <-ctx.Done():
					return api.Receipt{}, ctx.Err()
				case <-release:
					return api.Receipt{Outcome: api.Completed, Effect: api.EffectCommitted}, nil
				}
			}
			s := f.submit()
			r := f.runtime("worker-a", 1, f.hooks())
			start(t, r)
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("no dispatch")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if closing {
				if err := r.Close(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				r.BeginDrain()
				short, done := context.WithTimeout(context.Background(), 25*time.Millisecond)
				if err := r.Drain(short); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("drain abandoned in-flight work", err)
				}
				done()
				close(release)
				if err := r.Drain(ctx); err != nil {
					t.Fatal(err)
				}
			}
			got := terminal(t, f, s.Record.ID)
			want := api.Completed
			if closing {
				want = api.OutcomeUnknown
			}
			if got.Receipt.Outcome != want {
				t.Fatal("shutdown outcome", got.State)
			}
			if !errors.Is(r.Start(context.Background()), ErrClosed) {
				t.Fatal("drained runtime restarted")
			}
		})
	}
}

func TestConcurrencyRemainsBounded(t *testing.T) {
	f := newFixture(t, 5*time.Second)
	requests := map[string]api.Request{}
	ids := []string{}
	for i := range 5 {
		input := f.input
		input.Request, _ = api.Clone(f.request)
		input.Request.IdempotencyKey = fmt.Sprint("work-", i)
		input.RequestRef, _, _ = api.SealRequest(input.Request, fmt.Sprint("input-", i))
		s, _, err := f.store.Submit(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, s.Record.ID)
		requests[s.Record.ID] = input.Request
	}
	var current, peak atomic.Int32
	entered := make(chan struct{}, 5)
	release := make(chan struct{})
	f.execute = func(ctx context.Context, _ ledger.Record, _ ledger.Binding) (api.Receipt, error) {
		n := current.Add(1)
		defer current.Add(-1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		entered <- struct{}{}
		select {
		case <-release:
			return api.Receipt{Outcome: api.Completed, Effect: api.EffectCommitted}, nil
		case <-ctx.Done():
			return api.Receipt{}, ctx.Err()
		}
	}
	hooks := f.hooks()
	hooks.Load = func(_ context.Context, record ledger.Record) (api.Request, error) {
		return api.Clone(requests[record.ID])
	}
	r := f.runtime("worker-a", 2, hooks)
	start(t, r)
	for range 2 {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("slots not dispatched")
		}
	}
	if peak.Load() != 2 {
		t.Fatal("capacity not used")
	}
	close(release)
	for _, id := range ids {
		if got := terminal(t, f, id); got.Receipt.Outcome != api.Completed {
			t.Fatal(got.State)
		}
	}
	if peak.Load() > 2 || f.calls.Load() != 5 {
		t.Fatal("concurrency exceeded", peak.Load(), f.calls.Load())
	}
}
