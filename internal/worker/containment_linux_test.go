//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func containedExecutor(t *testing.T) (*Executor, *containment.Manager, *admission.Pool) {
	t.Helper()
	root, state, launcher := os.Getenv("KELVO_TEST_CGROUP_ROOT"), os.Getenv("KELVO_TEST_CGROUP_STATE"), os.Getenv("KELVO_TEST_SANDBOX")
	if root == "" || state == "" || launcher == "" {
		t.Skip("explicit disposable delegation and launcher required")
	}
	manager, err := containment.Open(containment.Config{Root: root, StateDirectory: state})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	limits := query.DefaultLimits()
	limits.MemoryMB = 64
	limits.MaxTempMB = 16
	limits.Threads = 2
	executor, err := New(catalog.Config{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	scratchDir := t.TempDir()
	os.Chmod(scratchDir, 0700)
	scratch, err := OpenScratchRoot(scratchDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scratch.Close(); err != nil {
			t.Error(err)
		}
	})
	pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 256 << 20, ScratchBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	executor.Containment = manager
	executor.ContainmentBudget = containment.Budget{NativeOverheadMB: 64, ParentOverheadMB: 32, MaxProcesses: 64}
	executor.ResourceOverheadBytes = 160 << 20
	executor.ResourcePool = pool
	executor.SandboxPath = launcher
	executor.ScratchRoot = scratch
	return executor, manager, pool
}
func TestContainedWorkerSuccessAndCancellation(t *testing.T) {
	executor, manager, pool := containedExecutor(t)
	stats, err := executor.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, &workerTestSink{})
	if err != nil || stats.Rows != 3 {
		t.Fatal("contained Go/Arrow execution", stats, err)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("success leaked resources")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := executor.Execute(ctx, query.Request{SQL: "SELECT wait"}, &workerTestSink{}); err == nil {
		t.Fatal("cancelled worker succeeded")
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("cancelled worker leaked resources")
	}
}
func TestContainedWorkerRefreshCustody(t *testing.T) {
	executor, manager, pool := containedExecutor(t)
	reservation, err := pool.Acquire(context.Background(), admission.Request{MemoryBytes: 224 << 20, ScratchBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	custody, _ := containment.NewCustody(reservation.Release)
	defer custody.Complete()
	executor.ResourcePool = nil
	ctx := containment.WithCustody(context.Background(), custody)
	if _, err := executor.Execute(ctx, query.Request{SQL: "SELECT 1"}, &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	if custody.State().Completed || custody.State().Held != 0 || manager.Status().Active != 0 || pool.Snapshot().Active != 1 {
		t.Fatal("worker completed outer refresh custody early", custody.State())
	}
	custody.Complete()
	if pool.Snapshot().Active != 0 {
		t.Fatal("publication completion leaked reservation")
	}
}
func TestContainedWorkerStartFailureCleansOwnership(t *testing.T) {
	executor, manager, pool := containedExecutor(t)
	// Replace only the local configured launcher with a nonexistent executable.
	// Preparation must be rolled back; no pipe read or uncontained retry follows.
	executor.SandboxPath = filepath.Join(t.TempDir(), "missing-launcher")
	start := time.Now()
	if _, err := executor.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, &workerTestSink{}); err == nil {
		t.Fatal("missing launcher succeeded")
	}
	if time.Since(start) > time.Second || pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("start failure blocked or leaked ownership")
	}
}

func TestContainedWorkerRealDuckDBCTE(t *testing.T) {
	binary := os.Getenv("KELVO_TEST_BINARY")
	if binary == "" {
		t.Skip("built Kelvo binary required")
	}
	executor, manager, pool := containedExecutor(t)
	executor.Binary = binary
	path := filepath.Join(t.TempDir(), "events.csv")
	if err := os.WriteFile(path, []byte("region,amount\na,10\na,20\nb,5\n"), 0600); err != nil {
		t.Fatal(err)
	}
	executor.Config = catalog.Config{Sources: []catalog.Source{{ID: "events", Type: "csv", Path: path, Options: map[string]string{"buffer_size": "1048576", "maximum_line_size": "262144"}}}}
	var sums []int64
	sink := &workerTestSink{write: func(batch arrow.RecordBatch) error {
		column, ok := batch.Column(1).(*array.Int64)
		if !ok {
			t.Fatal("result width changed")
		}
		for i := 0; i < column.Len(); i++ {
			sums = append(sums, column.Value(i))
		}
		return nil
	}}
	stats, err := executor.Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: "WITH base AS (SELECT region, CAST(amount AS BIGINT) AS amount FROM events) SELECT region, CAST(sum(amount) AS BIGINT) AS amount FROM base GROUP BY region ORDER BY region"}, sink)
	if err != nil || stats.Rows != 2 || len(sums) != 2 || sums[0] != 30 || sums[1] != 5 {
		t.Fatal("contained DuckDB CTE failed", stats, sums, err)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("DuckDB query leaked resources")
	}
}
