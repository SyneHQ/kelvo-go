//go:build linux && duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// These tests use the built application, its actual IPC boundary and Landlock.
// Queue state is an in-memory CAS fixture; this is not a live NATS test.
func workerFailureBinaries(t *testing.T) (string, string) {
	t.Helper()
	binary, sandbox := os.Getenv("KELVO_TEST_WORKER_BINARY"), os.Getenv("KELVO_TEST_SANDBOX_BINARY")
	if binary == "" && sandbox == "" {
		t.Skip("set both KELVO_TEST_WORKER_BINARY and KELVO_TEST_SANDBOX_BINARY for sandboxed resource acceptance")
	}
	for _, path := range []string{binary, sandbox} {
		info, err := os.Stat(path)
		if err != nil || !filepath.IsAbs(path) || !info.Mode().IsRegular() || info.Mode()&0111 == 0 || info.Mode()&0022 != 0 {
			t.Fatal("resource acceptance requires absolute, private executable paths")
		}
	}
	return binary, sandbox
}

type failureQuota struct {
	cancel   context.CancelCauseFunc
	active   atomic.Int64
	calls    atomic.Int64
	releases atomic.Int64
}

func (q *failureQuota) Acquire(ctx context.Context, ids []string) (context.Context, func(), error) {
	if len(ids) != 1 || ids[0] != "events" {
		return nil, nil, errors.New("unexpected fixture source selection")
	}
	owned, cancel := context.WithCancelCause(ctx)
	q.cancel = cancel
	q.active.Add(1)
	q.calls.Add(1)
	return owned, func() { q.active.Add(-1); q.releases.Add(1); cancel(context.Canceled) }, nil
}

type failureSink struct {
	query.Sink
	afterWrite func()
}

func (s failureSink) Write(batch arrow.RecordBatch) error {
	if err := s.Sink.Write(batch); err != nil {
		return err
	}
	s.afterWrite()
	return nil
}

type failureExecutor struct {
	inner      query.Executor
	afterWrite func()
}

func (e failureExecutor) Execute(ctx context.Context, req query.Request, sink query.Sink) (query.Stats, error) {
	if e.afterWrite != nil {
		sink = failureSink{sink, e.afterWrite}
	}
	return e.inner.Execute(ctx, req, sink)
}

type failureScalarSink struct {
	rows  int64
	total int64
}

func (*failureScalarSink) Schema(*arrow.Schema) error { return nil }
func (s *failureScalarSink) Write(batch arrow.RecordBatch) error {
	if batch.NumCols() != 1 {
		return errors.New("unexpected scalar schema")
	}
	column, ok := batch.Column(0).(*array.Int64)
	if !ok {
		return errors.New("unexpected scalar type")
	}
	for i := 0; i < column.Len(); i++ {
		if column.IsNull(i) {
			return errors.New("unexpected scalar null")
		}
		s.rows++
		s.total += column.Value(i)
	}
	return nil
}

func TestSandboxedWorkerFailurePreservesSnapshotAndAdmission(t *testing.T) {
	binary, sandbox := workerFailureBinaries(t)
	for _, multipart := range []bool{false, true} {
		name := "single"
		if multipart {
			name = "multipart"
		}
		t.Run(name, func(t *testing.T) {
			for _, failure := range []string{"native_memory", "source_ownership"} {
				t.Run(failure, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
					defer cancel()
					dir := t.TempDir()
					work := filepath.Join(dir, "workers")
					if err := os.Mkdir(work, 0700); err != nil {
						t.Fatal(err)
					}
					t.Setenv("TMPDIR", work)
					path := filepath.Join(dir, "events.parquet")
					writeInput := func(n int64) {
						t.Helper()
						file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
						if err != nil {
							t.Fatal(err)
						}
						defer file.Close()
						schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
						builder := array.NewInt64Builder(memory.DefaultAllocator)
						builder.Append(n)
						column := builder.NewArray()
						builder.Release()
						batch := array.NewRecordBatch(schema, []arrow.Array{column}, 1)
						column.Release()
						defer batch.Release()
						sink := acceleration.NewParquetSink(file, query.DefaultLimits())
						defer sink.Abort()
						if err := sink.Schema(schema); err != nil {
							t.Fatal(err)
						}
						if err := sink.Write(batch); err != nil {
							t.Fatal(err)
						}
						if err := sink.Finish(); err != nil {
							t.Fatal(err)
						}
					}
					writeInput(3)
					limits := query.Limits{MaxRows: 10, MaxBytes: 1 << 20, Timeout: 20 * time.Second, MemoryMB: 64, Threads: 1, MaxTempMB: 16}
					request := query.Request{Mode: "federated", Sources: []string{"events"}, SQL: "SELECT length(list(i))::BIGINT AS total FROM events, LATERAL range(events.n) AS input(i)"}
					dataset := catalog.Dataset{ID: "protected_snapshot", Query: request, MaxAge: time.Hour, AuthorizationVersion: "readers-v1", Limits: limits}
					if multipart {
						dataset.Multipart = &catalog.MultipartConfig{MaxParts: 2, MaxPartBytes: 1 << 20}
					}
					cfg := catalog.Config{Sources: []catalog.Source{{ID: "events", Type: "parquet", Path: path}}, Acceleration: &catalog.AccelerationConfig{Directory: filepath.Join(dir, "snapshots"), TenantID: "worker-failure", Datasets: []catalog.Dataset{dataset}}}
					pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 80 << 20, ScratchBytes: 16 << 20})
					if err != nil {
						t.Fatal(err)
					}
					quota := &failureQuota{}
					var loseDuringWrite atomic.Bool
					factory := func(c catalog.Config, l query.Limits) (query.Executor, error) {
						e := &worker.Executor{Config: c, Limits: l, Binary: binary, SandboxPath: sandbox, ResourcePool: pool, ResourceOverheadBytes: 16 << 20, SourceAdmission: quota}
						return failureExecutor{inner: e, afterWrite: func() {
							if loseDuringWrite.Load() {
								quota.cancel(errors.New("private coordination credential"))
							}
						}}, nil
					}
					manager, err := acceleration.NewManager(cfg, factory)
					if err != nil {
						t.Fatal(err)
					}
					defer manager.Close()
					initial, err := manager.Refresh(ctx, dataset.ID, false)
					if err != nil || initial.Rows != 1 {
						t.Fatalf("initial snapshot: %v", err)
					}
					assertReleased := func() {
						t.Helper()
						state := pool.Snapshot()
						if state.Active != 0 || state.Waiting != 0 || state.Used != (admission.Request{}) || quota.active.Load() != 0 || quota.calls.Load() != quota.releases.Load() {
							t.Fatal("worker failure leaked admission")
						}
						entries, err := os.ReadDir(work)
						if err != nil || len(entries) != 0 {
							t.Fatal("worker scratch directory not removed")
						}
					}
					assertReleased()
					if failure == "native_memory" {
						writeInput(20000000)
					} else {
						loseDuringWrite.Store(true)
					}
					fingerprint, err := cfg.DatasetFingerprint(dataset.ID)
					if err != nil {
						t.Fatal(err)
					}
					job := RefreshJob{Dataset: dataset.ID, Fingerprint: fingerprint}
					statusStore := newStatusFixture()
					queue := &RefreshQueue{status: statusStore, latestSequence: func(context.Context) (uint64, error) { return 50, nil }}
					msg := &refreshTestMessage{streamSequence: 51}
					var observedErr error
					err = queue.processRefresh(ctx, msg, job, func(ctx context.Context, _ RefreshJob) error {
						_, observedErr = manager.Refresh(ctx, dataset.ID, false)
						return observedErr
					}, time.Second)
					if err == nil || observedErr == nil {
						t.Fatal("failed worker reported successful refresh")
					}
					if strings.Contains(err.Error(), "private") || strings.Contains(observedErr.Error(), "private") {
						t.Fatal("private failure escaped")
					}
					status, err := queue.Status(ctx, job)
					if err != nil {
						t.Fatal(err)
					}
					if failure == "native_memory" {
						public := query.PublicError(observedErr)
						if public.Code != "RESOURCE_EXHAUSTED" || public.Message != "Query exceeded DuckDB memory limit" {
							t.Fatalf("native error lost through worker IPC: %+v", public)
						}
						if status.State != "permanent" || status.Category != "resource" || msg.terms.Load() != 1 || msg.naks.Load() != 0 || msg.acks.Load() != 0 || !status.NextRetryAt.IsZero() {
							t.Fatalf("resource failure not terminal: %+v", status)
						}
						queue = &RefreshQueue{status: statusStore, latestSequence: queue.latestSequence}
						stopped := &refreshTestMessage{streamSequence: 52}
						_ = queue.processRefresh(ctx, stopped, job, func(context.Context, RefreshJob) error {
							t.Error("permanent refresh repeated after queue restart")
							return nil
						}, time.Second)
						if stopped.terms.Load() != 1 {
							t.Fatal("resource stop lost across queue restart")
						}
					} else if status.State != "retrying" || msg.naks.Load() != 1 || msg.acks.Load() != 0 {
						t.Fatal("lost source ownership did not remain retryable")
					}
					current, err := manager.Verify(ctx, dataset.ID)
					if err != nil || current.Generation != initial.Generation || current.SHA256 != initial.SHA256 || current.SchemaHash != initial.SchemaHash || current.Fingerprint != initial.Fingerprint || current.Rows != initial.Rows || !current.RefreshedAt.Equal(initial.RefreshedAt) {
						t.Fatalf("failed refresh changed prior generation: %v", err)
					}
					assertReleased()
					writeInput(5)
					loseDuringWrite.Store(false)
					if err := queue.Reset(ctx, job); err != nil {
						t.Fatal(err)
					}
					repaired := &refreshTestMessage{streamSequence: 53}
					err = queue.processRefresh(ctx, repaired, job, func(ctx context.Context, _ RefreshJob) error {
						_, err := manager.Refresh(ctx, dataset.ID, false)
						return err
					}, time.Second)
					if err != nil || repaired.acks.Load() != 1 {
						t.Fatalf("repaired refresh did not resume: %v", err)
					}
					current, err = manager.Verify(ctx, dataset.ID)
					if err != nil || current.Generation == initial.Generation || current.Fingerprint != initial.Fingerprint {
						t.Fatal("repaired refresh did not publish new generation")
					}
					reader := &worker.Executor{Config: cfg, Limits: limits, Binary: binary, SandboxPath: sandbox, ResourcePool: pool, ResourceOverheadBytes: 16 << 20}
					sink := &failureScalarSink{}
					_, err = reader.Execute(ctx, query.Request{Mode: "federated", Sources: []string{dataset.ID}, SQL: "SELECT total FROM protected_snapshot"}, sink)
					if err != nil || sink.rows != 1 || sink.total != 5 {
						t.Fatalf("recovered snapshot values wrong: %v", err)
					}
					assertReleased()
				})
			}
		})
	}
}
