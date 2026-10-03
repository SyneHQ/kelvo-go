//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type exportRuntimeStore struct {
	mu     sync.Mutex
	p      Policy
	jobs   map[string]ExportSnapshot
	queue  chan Delivery
	acks   atomic.Int32
	before func(ExportJob, ExportJob) error
	after  func(ExportJob, ExportJob) error
}
type exportRuntimeDelivery struct {
	id    string
	store *exportRuntimeStore
}

func (d exportRuntimeDelivery) ID() string                  { return d.id }
func (d exportRuntimeDelivery) Ack(context.Context) error   { d.store.acks.Add(1); return nil }
func (d exportRuntimeDelivery) Retry(context.Context) error { return nil }
func (s *exportRuntimeStore) Policy() Policy                { return s.p }
func cloneRuntimeExport(s ExportSnapshot) ExportSnapshot {
	raw, _ := json.Marshal(s)
	var copy ExportSnapshot
	_ = json.Unmarshal(raw, &copy)
	return copy
}
func (s *exportRuntimeStore) GetExport(ctx context.Context, id string) (ExportSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ExportSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.jobs[id]
	if !ok {
		return ExportSnapshot{}, ErrExportNotFound
	}
	return cloneRuntimeExport(v), nil
}
func (s *exportRuntimeStore) SubmitExport(context.Context, ExportSubmission) (ExportSnapshot, error) {
	return ExportSnapshot{}, ErrExportCapacity
}
func (s *exportRuntimeStore) CompareAndSwapExport(ctx context.Context, old ExportSnapshot, next ExportJob) (ExportSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ExportSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.jobs[old.Job.ID]
	if !ok || current.Revision != old.Revision {
		return ExportSnapshot{}, ErrExportConflict
	}
	if s.before != nil {
		if err := s.before(current.Job, next); err != nil {
			return ExportSnapshot{}, err
		}
	}
	var err error
	next, _, err = exportTransition(s.p, current.Job, next, time.Now().UTC())
	if err != nil {
		return ExportSnapshot{}, err
	}
	if err := validateExportJob(s.p, next); err != nil {
		return ExportSnapshot{}, err
	}
	result := cloneRuntimeExport(ExportSnapshot{Job: next, Revision: old.Revision + 1})
	s.jobs[next.ID] = result
	if s.after != nil {
		if err := s.after(current.Job, next); err != nil {
			return ExportSnapshot{}, err
		}
	}
	return cloneRuntimeExport(result), nil
}
func (s *exportRuntimeStore) EnqueueExport(ctx context.Context, id string) error {
	select {
	case s.queue <- exportRuntimeDelivery{id, s}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *exportRuntimeStore) NextExport(ctx context.Context) (Delivery, error) {
	select {
	case d := <-s.queue:
		return d, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (s *exportRuntimeStore) ReconcileExports(context.Context) error { return nil }

type runtimeExportExecutor func(context.Context, query.Request, query.Sink) (query.Stats, error)

func (f runtimeExportExecutor) Execute(ctx context.Context, q query.Request, s query.Sink) (query.Stats, error) {
	return f(ctx, q, s)
}

func runtimeExportRows(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues([]int64{7, 0, 9223372036854775807}, []bool{true, false, true})
	a := b.NewArray()
	defer a.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{a}, 3)
	defer record.Release()
	if err := sink.Write(record); err != nil {
		return query.Stats{}, err
	}
	return query.Stats{Rows: 3, Batches: 1, Bytes: 24, Backend: "fixture"}, ctx.Err()
}

func openRuntimeExportFixture(t *testing.T, executor query.Executor) (*ExportRuntime, *exportRuntimeStore, NodeConfig) {
	t.Helper()
	cfg := runtimeExportConfigFixture(t)
	cfg.WorkerID = "a1"
	s := &exportRuntimeStore{p: cfg.Policy, jobs: make(map[string]ExportSnapshot), queue: make(chan Delivery, 16)}
	pool, err := cfg.Resources.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	r, err := newExportRuntime(cfg, s, executor, pool, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r, s, cfg
}

func addRuntimeExport(t *testing.T, s *exportRuntimeStore) string {
	return addRuntimeExportSQL(t, s, "SELECT 7")
}

func addRuntimeExportSQL(t *testing.T, s *exportRuntimeStore, sql string) string {
	t.Helper()
	a, _ := authorityForPrincipal(s.p, "reports")
	ctx := context.WithValue(context.Background(), jobAuthorityKey{}, a)
	now := time.Now().UTC()
	j, err := normalizeExportSubmission(ctx, s.p, ExportSubmission{Request: query.Request{Mode: "federated", SQL: sql}, SupervisorOwner: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", AuthorityUntil: now.Add(s.p.LeaseDuration)}, now)
	if err != nil {
		t.Fatal(err)
	}
	j.ID = "e0-0123456789abcdef0123456789abcdef"
	s.mu.Lock()
	s.jobs[j.ID] = ExportSnapshot{Job: j, Revision: 1}
	s.mu.Unlock()
	if err = s.EnqueueExport(ctx, j.ID); err != nil {
		t.Fatal(err)
	}
	return j.ID
}
func claimRuntimeExport(t *testing.T, s *exportRuntimeStore, id string) ExportSnapshot {
	t.Helper()
	waitFor(t, func() bool { v, _ := s.GetExport(context.Background(), id); return v.Job.State == ExportAssigned })
	v, err := s.GetExport(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	next := v.Job
	next.State = ExportClaimed
	next.Claim = "cccccccccccccccccccccccccccccccc"
	v, err = s.CompareAndSwapExport(context.Background(), v, next)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func readyRuntimeExport(t *testing.T, s *exportRuntimeStore, id string) ExportSnapshot {
	t.Helper()
	v, err := s.GetExport(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	next := v.Job
	next.State = ExportReady
	v, err = s.CompareAndSwapExport(context.Background(), v, next)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestExportRuntimeStoresBeforeReadyAndPreservesRestartReads(t *testing.T) {
	var calls atomic.Int32
	r, s, cfg := openRuntimeExportFixture(t, runtimeExportExecutor(func(ctx context.Context, q query.Request, sink query.Sink) (query.Stats, error) {
		calls.Add(1)
		return runtimeExportRows(ctx, q, sink)
	}))
	id := addRuntimeExport(t, s)
	claimed := claimRuntimeExport(t, s, id)
	result, err := r.RunExport(context.Background(), claimed)
	if err != nil {
		t.Fatal(err)
	}
	stored, _ := s.GetExport(context.Background(), id)
	if stored.Job.State != ExportStored || stored.Job.Receipt == nil || !sameExportReceipt(stored.Job.Receipt, &result.Receipt) {
		t.Fatal("missing durable stored receipt")
	}
	if _, err = r.OpenPart(context.Background(), stored, 0); err == nil {
		t.Fatal("stored result readable before ready")
	}
	ready := readyRuntimeExport(t, s, id)
	storageID := ready.Job.Local.StorageID
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	pool, err := cfg.Resources.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := newExportRuntime(cfg, s, runtimeExportExecutor(func(context.Context, query.Request, query.Sink) (query.Stats, error) {
		calls.Add(1)
		return query.Stats{}, errors.New("must not replay")
	}), pool, "dddddddddddddddddddddddddddddddd")
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if restarted.custody.ID() != storageID {
		t.Fatal("restart changed storage identity")
	}
	for range 2 {
		part, err := restarted.OpenPart(context.Background(), ready, 0)
		if err != nil {
			t.Fatal(err)
		}
		reader, err := ipc.NewReader(part)
		if err != nil {
			t.Fatal(err)
		}
		if !reader.Next() {
			t.Fatal("missing retained batch", reader.Err())
		}
		values := reader.RecordBatch().Column(0).(*array.Int64)
		if values.Value(0) != 7 || !values.IsNull(1) || values.Value(2) != 9223372036854775807 {
			t.Fatal("retained data changed")
		}
		if reader.Next() || reader.Err() != nil {
			t.Fatal("unexpected stream tail", reader.Err())
		}
		reader.Release()
		if err = part.FinalReady(); err != nil {
			t.Fatal(err)
		}
		if err = part.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 || pool.Snapshot().Active != 0 {
		t.Fatal("restart replayed SQL or leaked admission")
	}
}

func TestExportRuntimeReservationBindingFailureNeverRunsSQL(t *testing.T) {
	var calls atomic.Int32
	r, s, _ := openRuntimeExportFixture(t, runtimeExportExecutor(func(context.Context, query.Request, query.Sink) (query.Stats, error) {
		calls.Add(1)
		return query.Stats{}, nil
	}))
	s.before = func(old, next ExportJob) error {
		if old.Local == nil && next.Local != nil {
			return errors.New("lost binding CAS")
		}
		return nil
	}
	id := addRuntimeExport(t, s)
	claimed := claimRuntimeExport(t, s, id)
	if _, err := r.RunExport(context.Background(), claimed); err == nil {
		t.Fatal("binding failure succeeded")
	}
	if calls.Load() != 0 || r.pool.Snapshot().Active != 0 {
		t.Fatal("SQL ran before binding or admission leaked")
	}
	result, err := r.storage.Cleanup(context.Background(), 8)
	if err != nil || result.Removed != 1 {
		t.Fatal("failed reservation was not cancelled", result, err)
	}
}

func TestExportRuntimeCancellationKeepsCustodyUntilExecutorCleanup(t *testing.T) {
	started, cleanup := make(chan struct{}), make(chan struct{})
	r, s, _ := openRuntimeExportFixture(t, runtimeExportExecutor(func(ctx context.Context, _ query.Request, _ query.Sink) (query.Stats, error) {
		close(started)
		<-ctx.Done()
		<-cleanup
		return query.Stats{}, ctx.Err()
	}))
	id := addRuntimeExport(t, s)
	claimed := claimRuntimeExport(t, s, id)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := r.RunExport(ctx, claimed); done <- err }()
	<-started
	cancel()
	if state := r.pool.Snapshot(); state.Active != 1 || state.Classes[admission.ClassExport].Active != 1 {
		t.Fatal("custody released before cleanup")
	}
	select {
	case <-done:
		t.Fatal("executor cleanup was bypassed")
	default:
	}
	close(cleanup)
	if err := <-done; err == nil {
		t.Fatal("cancelled export succeeded")
	}
	if r.pool.Snapshot().Active != 0 {
		t.Fatal("cleanup leaked custody")
	}
	final, _ := s.GetExport(context.Background(), id)
	if final.Job.State == ExportStored || final.Job.State == ExportReady {
		t.Fatal("cancelled export published")
	}
}

func TestExportRuntimeQuarantinedProcessRetainsAdmission(t *testing.T) {
	var release func()
	r, s, _ := openRuntimeExportFixture(t, runtimeExportExecutor(func(ctx context.Context, _ query.Request, _ query.Sink) (query.Stats, error) {
		var err error
		release, err = containment.FromContext(ctx).Hold()
		if err != nil {
			return query.Stats{}, err
		}
		return query.Stats{}, errors.New("quarantined child")
	}))
	id := addRuntimeExport(t, s)
	claimed := claimRuntimeExport(t, s, id)
	if _, err := r.RunExport(context.Background(), claimed); err == nil {
		t.Fatal("failed child succeeded")
	}
	if r.pool.Snapshot().Active != 1 {
		t.Fatal("quarantined child lost reservation")
	}
	release()
	if r.pool.Snapshot().Active != 0 {
		t.Fatal("released child leaked reservation")
	}
}

func TestExportRuntimeWithdrawsAdmittedPartAndRechecksEveryOpen(t *testing.T) {
	r, s, _ := openRuntimeExportFixture(t, runtimeExportExecutor(runtimeExportRows))
	id := addRuntimeExport(t, s)
	claimed := claimRuntimeExport(t, s, id)
	if _, err := r.RunExport(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	ready := readyRuntimeExport(t, s, id)
	part, err := r.OpenPart(context.Background(), ready, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.pool.Snapshot().Classes[admission.ClassExport].Active != 1 {
		t.Fatal("download verification lacks admission")
	}
	next := ready.Job
	next.State = ExportCancelled
	next.Error = &query.Error{Code: "CANCELLED", Message: "Export cancelled"}
	if _, err = s.CompareAndSwapExport(context.Background(), ready, next); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return part.ctx.Err() != nil })
	if _, err = part.Read(make([]byte, 1)); err == nil {
		t.Fatal("withdrawn read returned bytes")
	}
	if part.FinalReady() == nil {
		t.Fatal("withdrawn part retained EOS authority")
	}
	if _, err = r.OpenPart(context.Background(), ready, 0); err == nil {
		t.Fatal("stale ready snapshot admitted another read")
	}
	if err = part.Close(); err != nil {
		t.Fatal(err)
	}
	if r.pool.Snapshot().Active != 0 {
		t.Fatal("download leaked admission")
	}
}

func TestExportRuntimeLostStoredCASNeverPublishesOrReplays(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[applied], func(t *testing.T) {
			var calls atomic.Int32
			r, s, _ := openRuntimeExportFixture(t, runtimeExportExecutor(func(ctx context.Context, q query.Request, sink query.Sink) (query.Stats, error) {
				calls.Add(1)
				return runtimeExportRows(ctx, q, sink)
			}))
			hook := func(old, next ExportJob) error {
				if next.State == ExportStored && old.State == ExportRunning {
					return errors.New("stored acknowledgement lost")
				}
				return nil
			}
			if applied {
				s.after = hook
			} else {
				s.before = hook
			}
			id := addRuntimeExport(t, s)
			claimed := claimRuntimeExport(t, s, id)
			if _, err := r.RunExport(context.Background(), claimed); err == nil {
				t.Fatal("uncertain store acknowledgement succeeded")
			}
			final, _ := s.GetExport(context.Background(), id)
			if final.Job.State == ExportReady {
				t.Fatal("lost acknowledgement published ready")
			}
			if _, err := r.OpenPart(context.Background(), final, 0); err == nil {
				t.Fatal("unpublished result readable")
			}
			if _, err := r.RunExport(context.Background(), claimed); err == nil {
				t.Fatal("old claim replayed")
			}
			if calls.Load() != 1 {
				t.Fatal("SQL replayed")
			}
		})
	}
}

func TestExportRuntimeDownloadRejectsChangedReceiptAndRoot(t *testing.T) {
	r, s, _ := openRuntimeExportFixture(t, runtimeExportExecutor(runtimeExportRows))
	id := addRuntimeExport(t, s)
	claimed := claimRuntimeExport(t, s, id)
	if _, err := r.RunExport(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	ready := readyRuntimeExport(t, s, id)
	for _, change := range []func(*ExportSnapshot){
		func(s *ExportSnapshot) { s.Job.Receipt.Manifest.Parts[0].Rows++ },
		func(s *ExportSnapshot) {
			s.Job.Local.StorageID = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
			s.Job.Receipt.Locator = *s.Job.Local
		},
		func(s *ExportSnapshot) { s.Job.Authority.Principal.PrincipalID = "analyst" },
	} {
		forged := cloneRuntimeExport(ready)
		change(&forged)
		if reflect.DeepEqual(forged, ready) {
			t.Fatal("fixture did not change")
		}
		// A caller snapshot only nominates a receipt; the trusted store remains
		// authoritative for principal and root binding.
		if sameExportReceipt(forged.Job.Receipt, ready.Job.Receipt) {
			continue
		}
		if part, err := r.OpenPart(context.Background(), forged, 0); err == nil {
			part.Close()
			t.Fatal("forged receipt accepted")
		}
	}
	part, err := r.OpenPart(context.Background(), ready, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(io.Discard, part); err != nil {
		t.Fatal(err)
	}
	part.Close()
}
