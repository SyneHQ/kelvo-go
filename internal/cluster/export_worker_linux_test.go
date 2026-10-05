//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func TestExportRuntimeConstructorBindsLimitsAndManagedScratch(t *testing.T) {
	for _, kind := range []string{"normalize", "missing", "other", "closed", "quota", "pool", "launcher", "missing_launcher", "missing_containment", "unexpected_containment", "unopened_containment"} {
		t.Run(kind, func(t *testing.T) {
			cfg := runtimeExportConfigFixture(t)
			cfg.WorkerID = "a1"
			if err := os.Mkdir(cfg.ScratchDirectory, 0700); err != nil {
				t.Fatal(err)
			}
			scratch, err := worker.OpenScratchRoot(cfg.ScratchDirectory)
			if err != nil {
				t.Fatal(err)
			}
			defer scratch.Close()
			pool, err := cfg.Resources.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			limits := cfg.Policy.Limits
			limits.MemoryMB *= 2
			executor := &worker.Executor{Limits: limits, ResourcePool: pool, SandboxPath: cfg.SandboxPath, ScratchRoot: scratch, ResourceOverheadBytes: 1}
			switch kind {
			case "missing":
				executor.ScratchRoot = nil
			case "other":
				cfg.ScratchDirectory = filepath.Join(filepath.Dir(cfg.ScratchDirectory), "other")
			case "closed":
				scratch.Close()
			case "quota":
				cfg.Policy.SourceQuotas = map[string]int{"sales": 1}
			case "pool":
				executor.ResourcePool, _ = admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 1024})
			case "launcher":
				executor.SandboxPath = "/private/other-launcher"
			case "missing_launcher":
				cfg.SandboxPath = ""
			case "missing_containment":
				cfg.Containment = &ContainmentConfig{}
			case "unexpected_containment":
				executor.Containment = &containment.Manager{}
			case "unopened_containment":
				cfg.Containment = &ContainmentConfig{Config: containment.Config{Root: "/sys/fs/cgroup/owned/jobs", StateDirectory: filepath.Join(t.TempDir(), "state")},
					Budget: containment.Budget{NativeOverheadMB: 16, ParentOverheadMB: 16, MaxProcesses: 32}}
				cfg.Resources.OverheadMB = int64(cfg.Policy.Limits.MemoryMB) + 32
				executor.Containment = &containment.Manager{}
				executor.ContainmentBudget = cfg.Containment.Budget
				executor.ResourcePool, err = cfg.Resources.NewPool()
				if err != nil {
					t.Fatal(err)
				}
			}
			s := &exportRuntimeStore{p: cfg.Policy, jobs: make(map[string]ExportSnapshot), queue: make(chan Delivery, 16)}
			r, err := NewExportRuntime(cfg, s, executor, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
			if kind != "normalize" {
				if err == nil {
					r.Close()
					t.Fatal("unsafe constructor input accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			private := r.executor.(*worker.Executor)
			if private.Limits != cfg.Policy.Limits || private.ResourcePool != nil || private.ResourceOverheadBytes != cfg.Resources.OverheadMB<<20 {
				t.Fatal("private executor exceeded admitted policy")
			}
			if executor.Limits != limits || executor.ResourcePool != pool || executor.ResourceOverheadBytes != 1 {
				t.Fatal("constructor changed the shared executor")
			}
		})
	}
}

type exportWorkerQuota struct {
	active  atomic.Int32
	calls   atomic.Int32
	started chan struct{}
}

func (q *exportWorkerQuota) Acquire(ctx context.Context, _ []string) (context.Context, func(), error) {
	q.calls.Add(1)
	q.active.Add(1)
	if q.started != nil {
		close(q.started)
	}
	return ctx, func() { q.active.Add(-1) }, nil
}

func exportActualWorker(t *testing.T) (*ExportRuntime, *exportRuntimeStore, *worker.ScratchRoot, *exportWorkerQuota) {
	t.Helper()
	return exportConfiguredActualWorker(t, nil)
}

func exportConfiguredActualWorker(t *testing.T, configure func(*NodeConfig, *catalog.Config)) (*ExportRuntime, *exportRuntimeStore, *worker.ScratchRoot, *exportWorkerQuota) {
	t.Helper()
	binary, sandbox := os.Getenv("KELVO_TEST_EXPORT_BINARY"), os.Getenv("KELVO_TEST_EXPORT_SANDBOX")
	if binary == "" || sandbox == "" {
		t.Skip("set KELVO_TEST_EXPORT_BINARY and KELVO_TEST_EXPORT_SANDBOX for actual sandboxed child acceptance")
	}
	cfg := runtimeExportConfigFixture(t)
	cfg.WorkerID = "a1"
	cfg.SandboxPath = sandbox
	catalogue := catalog.Config{}
	if configure != nil {
		configure(&cfg, &catalogue)
	}
	if err := os.MkdirAll(cfg.ScratchDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	scratch, err := worker.OpenScratchRoot(cfg.ScratchDirectory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scratch.Close(); err != nil {
			t.Error(err)
		}
	})
	pool, err := cfg.Resources.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	quota := &exportWorkerQuota{}
	executor, err := worker.New(catalogue, cfg.Policy.Limits)
	if err != nil {
		t.Fatal(err)
	}
	executor.Binary, executor.SandboxPath, executor.ScratchRoot = binary, sandbox, scratch
	executor.ResourcePool = pool
	executor.ResourceOverheadBytes = cfg.Resources.OverheadMB << 20
	executor.SourceAdmission = quota
	s := &exportRuntimeStore{p: cfg.Policy, jobs: make(map[string]ExportSnapshot), queue: make(chan Delivery, 16)}
	r, err := NewExportRuntime(cfg, s, executor, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r, s, scratch, quota
}

func TestExportWorkerUsesSandboxAndExternalCustody(t *testing.T) {
	r, s, scratch, quota := exportActualWorker(t)
	var sawStored atomic.Bool
	s.before = func(old, next ExportJob) error {
		if next.State == ExportStored {
			if quota.active.Load() != 0 {
				t.Error("source quota retained after source/child cleanup")
			}
			if r.pool.Snapshot().Classes[admission.ClassExport].Active != 1 {
				t.Error("export publication lost external custody")
			}
			sawStored.Store(true)
		}
		return nil
	}
	id := addRuntimeExport(t, s)
	claimed := claimRuntimeExport(t, s, id)
	result, err := r.RunExport(context.Background(), claimed)
	if err != nil {
		t.Fatal(err)
	}
	if !sawStored.Load() || result.Stats.Rows != 1 || quota.calls.Load() != 1 || quota.active.Load() != 0 {
		t.Fatal("actual worker completion/accounting mismatch", result.Stats)
	}
	ready := readyRuntimeExport(t, s, id)
	part, err := r.OpenPart(context.Background(), ready, 0)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := ipc.NewReader(part)
	if err != nil {
		part.Close()
		t.Fatal(err)
	}
	if !stream.Next() || stream.RecordBatch().NumRows() != 1 {
		t.Fatal("missing actual worker data", stream.Err())
	}
	values, ok := stream.RecordBatch().Column(0).(*array.Int32)
	if !ok || values.Value(0) != 7 {
		t.Fatal("actual worker data changed")
	}
	if stream.Next() || stream.Err() != nil {
		t.Fatal("incomplete actual worker stream", stream.Err())
	}
	stream.Release()
	part.Close()
	if reclaimed, err := scratch.Reclaim(); err != nil || reclaimed != 0 {
		t.Fatal("actual worker left scratch ownership", reclaimed, err)
	}
	if r.pool.Snapshot().Active != 0 {
		t.Fatal("actual worker leaked reservations")
	}
}

func TestExportWorkerCancellationReapsSandboxAndReservations(t *testing.T) {
	r, s, scratch, quota := exportActualWorker(t)
	quota.started = make(chan struct{})
	id := addRuntimeExportSQL(t, s, "SELECT sum(sin(i::DOUBLE)) FROM range(1000000000) t(i)")
	claimed := claimRuntimeExport(t, s, id)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := r.RunExport(ctx, claimed); done <- err }()
	<-quota.started
	time.AfterFunc(300*time.Millisecond, cancel)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled child export succeeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("actual child did not stop")
	}
	if quota.active.Load() != 0 || r.pool.Snapshot().Active != 0 {
		t.Fatal("cancelled actual worker retained quota or admission")
	}
	if reclaimed, err := scratch.Reclaim(); err != nil || reclaimed != 0 {
		t.Fatal("cancelled actual worker left scratch ownership", reclaimed, err)
	}
	final, _ := s.GetExport(context.Background(), id)
	if final.Job.State == ExportReady || final.Job.State == ExportStored {
		t.Fatal("cancelled actual worker published")
	}
}
