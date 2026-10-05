//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func TestExportRuntimeStorageExhaustionPrecedesSQL(t *testing.T) {
	var calls atomic.Int32
	r, s, _ := openRuntimeExportFixture(t, runtimeExportExecutor(func(context.Context, query.Request, query.Sink) (query.Stats, error) {
		calls.Add(1)
		return query.Stats{}, nil
	}))
	a, _ := authorityForPrincipal(s.p, "reports")
	identity, err := exportIdentity(s.p, ExportAuthority{Principal: a, AuthorizationVersion: s.p.Exports.AuthorizationVersion})
	if err != nil {
		t.Fatal(err)
	}
	var held []*exports.Writer
	defer func() {
		for _, w := range held {
			if err := w.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	for range r.cfg.Exports.MaxEntries + 1 {
		w, err := r.storage.Reserve(context.Background(), exports.Request{Identity: identity, ExpiresAt: time.Now().Add(time.Minute), Limits: s.p.Exports.Limits})
		if errors.Is(err, exports.ErrLimit) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, w)
	}
	if len(held) == 0 {
		t.Fatal("storage was not exercised")
	}
	id := addRuntimeExport(t, s)
	claimed := claimRuntimeExport(t, s, id)
	if _, err := r.RunExport(context.Background(), claimed); !errors.Is(err, exports.ErrLimit) {
		t.Fatal("storage exhaustion was not preserved", err)
	}
	if calls.Load() != 0 || r.pool.Snapshot().Active != 0 {
		t.Fatal("storage-exhausted export ran SQL or retained admission")
	}
	final, _ := s.GetExport(context.Background(), id)
	if final.Job.State != ExportFailed || final.Job.Local != nil {
		t.Fatal("storage failure created executable ownership")
	}
}

type exportFaultQuota struct {
	reject  bool
	started chan struct{}
	cancel  context.CancelFunc
	ids     []string
	active  atomic.Int32
}

func (q *exportFaultQuota) Acquire(ctx context.Context, ids []string) (context.Context, func(), error) {
	q.ids = append([]string(nil), ids...)
	if q.reject {
		return nil, nil, ErrSourceQuotaUnavailable
	}
	ctx, q.cancel = context.WithCancel(ctx)
	q.active.Add(1)
	close(q.started)
	return ctx, func() { q.cancel(); q.active.Add(-1) }, nil
}

func exportWorkerCSVSource(t *testing.T, cfg *NodeConfig, catalogue *catalog.Config) {
	t.Helper()
	path := filepath.Join(filepath.Dir(cfg.ScratchDirectory), "source.csv")
	if err := os.WriteFile(path, []byte("value\n7\n"), 0600); err != nil {
		t.Fatal(err)
	}
	catalogue.Sources = []catalog.Source{{ID: "sales", Type: "csv", Path: path}}
	grant := cfg.Policy.Access.Principals["reports"]
	grant.FederatedSources = append(grant.FederatedSources, "sales")
	cfg.Policy.Access.Principals["reports"] = grant
}

func TestExportWorkerSourceQuotaRejectsBeforeChildStart(t *testing.T) {
	r, s, scratch, _ := exportConfiguredActualWorker(t, func(cfg *NodeConfig, catalogue *catalog.Config) {
		exportWorkerCSVSource(t, cfg, catalogue)
	})
	quota := &exportFaultQuota{reject: true}
	native := r.executor.(*worker.Executor)
	native.SourceAdmission = quota
	native.Binary = filepath.Join(t.TempDir(), "must-not-execute")
	id := addRuntimeExportRequest(t, s, query.Request{Mode: "federated", SQL: "SELECT * FROM sales", Sources: []string{"sales"}})
	claimed := claimRuntimeExport(t, s, id)
	if _, err := r.RunExport(context.Background(), claimed); !errors.Is(err, ErrSourceQuotaUnavailable) {
		t.Fatal("source rejection was bypassed", err)
	}
	if !reflect.DeepEqual(quota.ids, []string{"sales"}) || r.pool.Snapshot().Active != 0 {
		t.Fatal("source quota selection/admission mismatch")
	}
	if reclaimed, err := scratch.Reclaim(); err != nil || reclaimed != 0 {
		t.Fatal("quota rejection created child scratch", reclaimed, err)
	}
}

func TestExportWorkerSourceQuotaRevocationReapsChild(t *testing.T) {
	r, s, scratch, _ := exportConfiguredActualWorker(t, func(cfg *NodeConfig, catalogue *catalog.Config) {
		exportWorkerCSVSource(t, cfg, catalogue)
	})
	quota := &exportFaultQuota{started: make(chan struct{})}
	r.executor.(*worker.Executor).SourceAdmission = quota
	id := addRuntimeExportRequest(t, s, query.Request{Mode: "federated", SQL: "SELECT sum(sin(i::DOUBLE)) FROM range(1000000000) t(i)", Sources: []string{"sales"}})
	claimed := claimRuntimeExport(t, s, id)
	done := make(chan error, 1)
	go func() { _, err := r.RunExport(context.Background(), claimed); done <- err }()
	<-quota.started
	time.AfterFunc(300*time.Millisecond, quota.cancel)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("revoked source quota completed export")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("quota revocation did not stop child")
	}
	if !reflect.DeepEqual(quota.ids, []string{"sales"}) || quota.active.Load() != 0 || r.pool.Snapshot().Active != 0 {
		t.Fatal("revocation leaked source or export admission")
	}
	if reclaimed, err := scratch.Reclaim(); err != nil || reclaimed != 0 {
		t.Fatal("revocation left child scratch", reclaimed, err)
	}
}

// This deliberately small disk-backed test double persists production-validated
// transitions. It is not a substitute for the separate real JetStream campaign.
type exportCrashStore struct {
	*exportRuntimeStore
	mu   sync.Mutex
	path string
}

func (s *exportCrashStore) CompareAndSwapExport(ctx context.Context, old ExportSnapshot, next ExportJob) (ExportSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.exportRuntimeStore.CompareAndSwapExport(ctx, old, next)
	if err == nil {
		err = persistExportCrashSnapshot(s.path, result)
	}
	return result, err
}
func persistExportCrashSnapshot(path string, s ExportSnapshot) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path+".next", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(raw)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err = errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err = os.Rename(path+".next", path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func exportCrashConfig(t *testing.T, root string) NodeConfig {
	cfg := runtimeExportConfigFixture(t)
	cfg.WorkerID = "a1"
	cfg.Policy.LeaseDuration = 30 * time.Second
	cfg.Exports.Directory = filepath.Join(root, "exports")
	cfg.ScratchDirectory = filepath.Join(root, "scratch")
	cfg.SandboxPath = os.Getenv("KELVO_TEST_EXPORT_SANDBOX")
	return cfg
}

func runExportCrashChild(t *testing.T, stage, root string) {
	t.Helper()
	cfg := exportCrashConfig(t, root)
	if err := os.MkdirAll(cfg.ScratchDirectory, 0700); err != nil {
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
	native, err := worker.New(catalog.Config{}, cfg.Policy.Limits)
	if err != nil {
		t.Fatal(err)
	}
	native.Binary = os.Getenv("KELVO_TEST_EXPORT_BINARY")
	native.SandboxPath = cfg.SandboxPath
	native.ScratchRoot = scratch
	base := &exportRuntimeStore{p: cfg.Policy, jobs: make(map[string]ExportSnapshot), queue: make(chan Delivery, 16)}
	durable := &exportCrashStore{exportRuntimeStore: base, path: filepath.Join(root, "job.json")}
	checkpoint := func() {
		fmt.Println("commit-boundary")
		for {
			time.Sleep(time.Hour)
		}
	}
	if stage == "after" {
		base.before = func(_, next ExportJob) error {
			if next.State == ExportStored {
				checkpoint()
			}
			return nil
		}
	}
	executor := runtimeExportExecutor(func(ctx context.Context, q query.Request, sink query.Sink) (query.Stats, error) {
		stats, err := native.Execute(ctx, q, sink)
		if err == nil && stage == "before" {
			checkpoint()
		}
		return stats, err
	})
	r, err := newExportRuntime(cfg, durable, executor, pool, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	id := addRuntimeExport(t, base)
	claimed := claimRuntimeExport(t, base, id)
	if _, err = r.RunExport(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash cutpoint was not reached")
}

func TestExportWorkerCrashAcrossLocalCommit(t *testing.T) {
	if stage := os.Getenv("KELVO_TEST_EXPORT_CRASH_STAGE"); stage != "" {
		runExportCrashChild(t, stage, os.Getenv("KELVO_TEST_EXPORT_CRASH_ROOT"))
		return
	}
	if os.Getenv("KELVO_TEST_EXPORT_BINARY") == "" || os.Getenv("KELVO_TEST_EXPORT_SANDBOX") == "" {
		t.Skip("set actual export worker binary and sandbox for commit crash acceptance")
	}
	for _, stage := range []string{"before", "after"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExportWorkerCrashAcrossLocalCommit$")
			child.Env = append(os.Environ(), "KELVO_TEST_EXPORT_CRASH_STAGE="+stage, "KELVO_TEST_EXPORT_CRASH_ROOT="+root)
			output, err := child.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = child.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if child.ProcessState == nil {
					_ = child.Process.Kill()
					_ = child.Wait()
				}
			}()
			scanner := bufio.NewScanner(output)
			if !scanner.Scan() || scanner.Text() != "commit-boundary" {
				t.Fatal("worker did not reach commit cutpoint", scanner.Text())
			}
			if err = child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err = child.Wait(); err == nil {
				t.Fatal("worker did not crash")
			}
			raw, err := os.ReadFile(filepath.Join(root, "job.json"))
			if err != nil {
				t.Fatal(err)
			}
			var persisted ExportSnapshot
			if err = json.Unmarshal(raw, &persisted); err != nil {
				t.Fatal(err)
			}
			if persisted.Job.State != ExportRunning || persisted.Job.Local == nil || persisted.Job.Receipt != nil {
				t.Fatal("crash persisted publication authority")
			}
			cfg := exportCrashConfig(t, root)
			pool, err := cfg.Resources.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			base := &exportRuntimeStore{p: cfg.Policy, jobs: map[string]ExportSnapshot{persisted.Job.ID: persisted}, queue: make(chan Delivery, 16)}
			var replays atomic.Int32
			r, err := newExportRuntime(cfg, base, runtimeExportExecutor(func(context.Context, query.Request, query.Sink) (query.Stats, error) {
				replays.Add(1)
				return query.Stats{}, errors.New("unexpected replay")
			}), pool, "dddddddddddddddddddddddddddddddd")
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if r.custody.ID() != persisted.Job.Local.StorageID {
				t.Fatal("restart changed storage root identity")
			}
			identity, err := exportIdentity(cfg.Policy, persisted.Job.Authority)
			if err != nil {
				t.Fatal(err)
			}
			local, readErr := r.storage.Acquire(context.Background(), persisted.Job.Local.ExportID, identity)
			if stage == "before" && readErr == nil {
				local.Close()
				t.Fatal("uncommitted local export readable")
			}
			if stage == "after" {
				if readErr != nil {
					t.Fatal("committed local witness missing", readErr)
				}
				local.Close()
			}
			if _, err = r.OpenPart(context.Background(), persisted, 0); err == nil {
				t.Fatal("restart published abandoned export")
			}
			if _, err = r.RunExport(context.Background(), persisted); err == nil {
				t.Fatal("restart adopted abandoned execution")
			}
			if err = base.EnqueueExport(context.Background(), persisted.Job.ID); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return base.acks.Load() == 1 })
			if replays.Load() != 0 {
				t.Fatal("redelivery replayed SQL")
			}
			if result, err := r.storage.Cleanup(context.Background(), 8); err != nil || result.Removed != 0 {
				t.Fatal("restart reclaimed unexpired retained reservation", result, err)
			}
		})
	}
}
