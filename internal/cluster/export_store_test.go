// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/nats-io/nats.go/jetstream"
)

type exportMemoryKV struct {
	jetstream.KeyValue
	mu       sync.Mutex
	entries  map[string]statusEntry
	revision uint64
	fail     bool
}

func newExportMemoryKV() *exportMemoryKV { return &exportMemoryKV{entries: map[string]statusEntry{}} }
func (k *exportMemoryKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.fail {
		return nil, errors.New("PRIVATE_BACKEND_DIAGNOSTIC")
	}
	e, ok := k.entries[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return e, nil
}
func (k *exportMemoryKV) Create(ctx context.Context, key string, raw []byte, _ ...jetstream.KVCreateOpt) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.fail {
		return 0, errors.New("PRIVATE_BACKEND_DIAGNOSTIC")
	}
	if _, ok := k.entries[key]; ok {
		return 0, jetstream.ErrKeyExists
	}
	k.revision++
	k.entries[key] = statusEntry{raw: append([]byte(nil), raw...), revision: k.revision}
	return k.revision, nil
}
func (k *exportMemoryKV) Update(ctx context.Context, key string, raw []byte, rev uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.fail {
		return 0, errors.New("PRIVATE_BACKEND_DIAGNOSTIC")
	}
	e, ok := k.entries[key]
	if !ok || e.revision != rev {
		return 0, jetstream.ErrKeyRevisionMismatch
	}
	k.revision++
	k.entries[key] = statusEntry{raw: append([]byte(nil), raw...), revision: k.revision}
	return k.revision, nil
}

type exportStoreFixture struct {
	store         *natsExportStore
	jobs, workers *exportMemoryKV
	now           time.Time
	ctx           context.Context
}

const exportTestOwner = "0123456789abcdef0123456789abcdef"
const exportTestClaim = "1123456789abcdef0123456789abcdef"

func newExportStoreFixture(t *testing.T) *exportStoreFixture {
	t.Helper()
	p := runtimeExportConfigFixture(t).Policy
	p.Exports.MaxJobs = 2
	f := &exportStoreFixture{jobs: newExportMemoryKV(), workers: newExportMemoryKV(), now: time.Now().UTC()}
	a, _ := authorityForPrincipal(p, "reports")
	f.ctx = context.WithValue(context.Background(), jobAuthorityKey{}, a)
	f.store = &natsExportStore{policy: p, kv: f.jobs, base: &NATSStore{policy: p, kv: f.workers}, now: func() time.Time { return f.now }}
	f.lease(t, exportTestOwner)
	return f
}
func (f *exportStoreFixture) lease(t *testing.T, owner string) {
	t.Helper()
	raw, _ := json.Marshal(workerLease{Owner: owner, HeartbeatAt: f.now})
	f.workers.mu.Lock()
	f.workers.revision++
	f.workers.entries["worker.a1"] = statusEntry{raw: raw, revision: f.workers.revision}
	f.workers.mu.Unlock()
}
func (f *exportStoreFixture) input() ExportSubmission {
	return ExportSubmission{Request: query.Request{Mode: "federated", SQL: "SELECT 1"}, SupervisorOwner: exportTestOwner, AuthorityUntil: f.now.Add(f.store.policy.LeaseDuration)}
}
func (f *exportStoreFixture) submit(t *testing.T) ExportSnapshot {
	t.Helper()
	s, err := f.store.SubmitExport(f.ctx, f.input())
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func (f *exportStoreFixture) step(t *testing.T, s ExportSnapshot, state string) ExportSnapshot {
	t.Helper()
	next := s.Job
	next.State = state
	switch state {
	case ExportAssigned:
		next.WorkerID = "a1"
		next.WorkerOwner = exportTestOwner
	case ExportClaimed:
		next.Claim = exportTestClaim
	case ExportCancelled, ExportFailed:
		next.Error = query.PublicError(query.NewError("CANCELLED", "Export cancelled"))
	}
	out, err := f.store.CompareAndSwapExport(f.ctx, s, next)
	if err != nil {
		t.Fatalf("%s -> %s: %v", s.Job.State, state, err)
	}
	return out
}
func (f *exportStoreFixture) running(t *testing.T) ExportSnapshot {
	t.Helper()
	s := f.submit(t)
	s = f.step(t, s, ExportAssigned)
	s = f.step(t, s, ExportClaimed)
	return f.step(t, s, ExportRunning)
}
func (f *exportStoreFixture) stored(t *testing.T) ExportSnapshot {
	t.Helper()
	s := f.running(t)
	next := s.Job
	next.Local = &ExportLocator{StorageID: exportTestOwner, ExportID: exportTestClaim, Fence: strings.Repeat("a", 32)}
	s, err := f.store.CompareAndSwapExport(f.ctx, s, next)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := exportIdentity(f.store.policy, s.Job.Authority)
	if err != nil {
		t.Fatal(err)
	}
	m := exports.Manifest{Version: 1, ID: s.Job.Local.ExportID, Fence: s.Job.Local.Fence, Tenant: s.Job.TenantID, Identity: identity, CreatedAt: f.now, ExpiresAt: s.Job.ExpiresAt, SchemaSHA256: strings.Repeat("b", 64), Rows: 1, EncodedBytes: 128, DecodedBytes: 8, Parts: []exports.PartInfo{{Index: 0, Rows: 1, Batches: 1, EncodedBytes: 128, DecodedBytes: 8, SHA256: strings.Repeat("c", 64)}}}
	receipt, err := sealExportReceipt(*s.Job.Local, m)
	if err != nil {
		t.Fatal(err)
	}
	next = s.Job
	next.State = ExportStored
	next.Receipt = &receipt
	next.Stats = query.Stats{Rows: 1, Batches: 1, Bytes: 8}
	s, err = f.store.CompareAndSwapExport(f.ctx, s, next)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestExportStoreRetainsSlotsAndDetachesSnapshots(t *testing.T) {
	f := newExportStoreFixture(t)
	first := f.submit(t)
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := f.store.SubmitExport(f.ctx, f.input()); results <- err }()
	}
	wg.Wait()
	close(results)
	admitted, busy := 0, 0
	for err := range results {
		if err == nil {
			admitted++
		} else if errors.Is(err, ErrExportCapacity) {
			busy++
		} else {
			t.Fatal(err)
		}
	}
	if admitted != 1 || busy != 7 {
		t.Fatal(admitted, busy)
	}
	cancelled := f.step(t, first, ExportCancelled)
	if _, err := f.store.SubmitExport(f.ctx, f.input()); !errors.Is(err, ErrExportCapacity) {
		t.Fatal("cancelled slot released before expiry", err)
	}
	cancelled.Job.Request.SQL = "SELECT private"
	policy := f.store.Policy()
	policy.Exports.MaxJobs = 400
	policy.Access.Principals["reports"] = PrincipalGrant{}
	got, err := f.store.GetExport(f.ctx, first.Job.ID)
	if err != nil || got.Job.Request.SQL != "SELECT 1" || f.store.policy.Exports.MaxJobs != 2 {
		t.Fatal("mutable return escaped", err)
	}
	f.now = first.Job.ExpiresAt.Add(time.Nanosecond)
	reused := f.submit(t)
	if reused.Job.ID == first.Job.ID {
		t.Fatal("reused handle identity")
	}
	if _, err = f.store.GetExport(f.ctx, first.Job.ID); !errors.Is(err, ErrExportNotFound) {
		t.Fatal("old handle survived replacement", err)
	}
}

func TestExportStoreRejectsForgedAndStaleTransitions(t *testing.T) {
	changes := map[string]func(*ExportJob){
		"request": func(j *ExportJob) { j.Request.SQL = "SELECT private" }, "tenant": func(j *ExportJob) { j.TenantID = "foreign" },
		"authority": func(j *ExportJob) { j.Authority.AuthorizationVersion = "v2" }, "supervisor": func(j *ExportJob) { j.SupervisorOwner = strings.Repeat("f", 32) },
		"expiry": func(j *ExportJob) { j.ExpiresAt = j.ExpiresAt.Add(time.Hour) }, "query_budget": func(j *ExportJob) { j.Spec.QueryLimits.MaxRows++ },
		"timestamp": func(j *ExportJob) { j.HeartbeatAt = j.HeartbeatAt.Add(time.Second) }, "worker": func(j *ExportJob) { j.WorkerOwner = strings.Repeat("f", 32) },
		"claim": func(j *ExportJob) { j.Claim = strings.Repeat("f", 32) }, "stats": func(j *ExportJob) { j.Stats.Rows = 99 },
		"requeue": func(j *ExportJob) { j.State = ExportQueued }, "skip_commit": func(j *ExportJob) { j.State = ExportReady },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			f := newExportStoreFixture(t)
			s := f.running(t)
			next := s.Job
			change(&next)
			if _, err := f.store.CompareAndSwapExport(f.ctx, s, next); !errors.Is(err, ErrExportConflict) {
				t.Fatal(err)
			}
		})
	}
	f := newExportStoreFixture(t)
	s := f.running(t)
	cancelled := f.step(t, s, ExportCancelled)
	if _, err := f.store.CompareAndSwapExport(f.ctx, s, s.Job); !errors.Is(err, ErrExportConflict) {
		t.Fatal("stale snapshot accepted", err)
	}
	fake := cancelled
	fake.Job = s.Job
	if _, err := f.store.CompareAndSwapExport(f.ctx, fake, s.Job); !errors.Is(err, ErrExportConflict) {
		t.Fatal("passed snapshot overrode current state", err)
	}
}

func TestExportStoreSeparatesSupervisorAndWorkerLiveness(t *testing.T) {
	f := newExportStoreFixture(t)
	s := f.running(t)
	heartbeat := s.Job.HeartbeatAt
	f.now = f.now.Add(time.Second)
	f.lease(t, exportTestOwner)
	next := s.Job
	next.AuthorityUntil = f.now.Add(f.store.policy.LeaseDuration)
	renewed, err := f.store.CompareAndSwapExport(f.ctx, s, next)
	if err != nil || !renewed.Job.HeartbeatAt.Equal(heartbeat) {
		t.Fatal("supervisor renewed worker heartbeat", err)
	}
	f.now = f.now.Add(time.Second)
	f.lease(t, exportTestOwner)
	beat, err := f.store.CompareAndSwapExport(context.Background(), renewed, renewed.Job)
	if err != nil || !beat.Job.HeartbeatAt.Equal(f.now) || !beat.Job.AuthorityUntil.Equal(renewed.Job.AuthorityUntil) {
		t.Fatal("worker changed authority", err)
	}
	next = beat.Job
	next.AuthorityUntil = f.now.Add(f.store.policy.LeaseDuration + time.Second)
	if _, err = f.store.CompareAndSwapExport(f.ctx, beat, next); !errors.Is(err, ErrExportConflict) {
		t.Fatal("future lease accepted", err)
	}
	next = beat.Job
	next.AuthorityUntil = f.now.Add(f.store.policy.LeaseDuration)
	if _, err = f.store.CompareAndSwapExport(context.Background(), beat, next); !errors.Is(err, ErrExportConflict) {
		t.Fatal("supervisor without principal accepted", err)
	}
	f.lease(t, strings.Repeat("f", 32))
	if _, err = f.store.CompareAndSwapExport(f.ctx, beat, beat.Job); !errors.Is(err, ErrExportConflict) {
		t.Fatal("superseded worker renewed export", err)
	}
	next = beat.Job
	next.State = ExportCancelled
	next.Error = query.PublicError(context.Canceled)
	if _, err = f.store.CompareAndSwapExport(context.Background(), beat, next); err != nil {
		t.Fatal("trusted stop required expired worker or key", err)
	}
}

func TestExportStoreStoredReadyAndWithdrawal(t *testing.T) {
	f := newExportStoreFixture(t)
	stored := f.stored(t)
	before := stored.Job.Receipt.Manifest.Parts[0].SHA256
	copy, err := f.store.GetExport(f.ctx, stored.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	copy.Job.Receipt.Manifest.Parts[0].SHA256 = strings.Repeat("d", 64)
	got, err := f.store.GetExport(f.ctx, stored.Job.ID)
	if err != nil || got.Job.Receipt.Manifest.Parts[0].SHA256 != before {
		t.Fatal("manifest alias", err)
	}
	ready := f.step(t, stored, ExportReady)
	f.now = f.now.Add(time.Minute)
	if err = f.store.ReconcileExports(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err = f.store.GetExport(f.ctx, ready.Job.ID)
	if err != nil || got.Job.State != ExportReady {
		t.Fatal("completed export required expired fill authority", err)
	}
	next := got.Job
	next.State = ExportCancelled
	next.Error = query.PublicError(context.Canceled)
	withdrawn, err := f.store.CompareAndSwapExport(context.Background(), got, next)
	if err != nil || !sameExportReceipt(withdrawn.Job.Receipt, ready.Job.Receipt) {
		t.Fatal("withdrawal changed immutable result", err)
	}
	if _, err = f.store.CompareAndSwapExport(f.ctx, withdrawn, ready.Job); !errors.Is(err, ErrExportConflict) {
		t.Fatal("withdrawn export resurrected", err)
	}
}

func TestExportStoreReconcilesLossWithoutReplayingSQL(t *testing.T) {
	for _, state := range []string{ExportQueued, ExportAssigned, ExportClaimed, ExportRunning, ExportStored} {
		t.Run(state, func(t *testing.T) {
			f := newExportStoreFixture(t)
			var s ExportSnapshot
			if state == ExportStored {
				s = f.stored(t)
			} else {
				s = f.submit(t)
				for _, step := range []string{ExportAssigned, ExportClaimed, ExportRunning} {
					if s.Job.State == state {
						break
					}
					s = f.step(t, s, step)
				}
			}
			f.now = s.Job.AuthorityUntil.Add(time.Nanosecond)
			if err := f.store.ReconcileExports(context.Background()); err != nil {
				t.Fatal(err)
			}
			failed, err := f.store.GetExport(f.ctx, s.Job.ID)
			if err != nil || failed.Job.State != ExportFailed {
				t.Fatal(err, failed.Job.State)
			}
			next := failed.Job
			next.State = ExportQueued
			if _, err = f.store.CompareAndSwapExport(f.ctx, failed, next); !errors.Is(err, ErrExportConflict) {
				t.Fatal("replayed lost SQL", err)
			}
		})
	}
}

func TestExportStoreQuarantinesPublicationUncertainty(t *testing.T) {
	f := newExportStoreFixture(t)
	s := f.running(t)
	next := s.Job
	next.Local = &ExportLocator{StorageID: exportTestOwner, ExportID: exportTestClaim, Fence: strings.Repeat("a", 32)}
	s, err := f.store.CompareAndSwapExport(f.ctx, s, next)
	if err != nil {
		t.Fatal(err)
	}
	next = s.Job
	next.State = ExportPublicationUncertain
	next.Error = query.PublicError(query.NewError("PUBLICATION_UNCERTAIN", "Export storage publication uncertain"))
	uncertain, err := f.store.CompareAndSwapExport(context.Background(), s, next)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{ExportQueued, ExportRunning, ExportStored, ExportReady, ExportCancelled} {
		next = uncertain.Job
		next.State = state
		if _, err = f.store.CompareAndSwapExport(f.ctx, uncertain, next); !errors.Is(err, ErrExportConflict) {
			t.Fatal("uncertainty adopted", state, err)
		}
	}
}

func TestExportStoreSanitizesBackendErrors(t *testing.T) {
	f := newExportStoreFixture(t)
	s := f.submit(t)
	f.jobs.fail = true
	for _, call := range []func() error{func() error { _, e := f.store.GetExport(f.ctx, s.Job.ID); return e }, func() error { _, e := f.store.SubmitExport(f.ctx, f.input()); return e }, func() error { return f.store.ReconcileExports(f.ctx) }} {
		err := call()
		if err == nil || strings.Contains(err.Error(), "PRIVATE_BACKEND_DIAGNOSTIC") {
			t.Fatal(err)
		}
	}
}
