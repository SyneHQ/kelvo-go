//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// The external controller must run this exact gate as nonroot PID 1, validate
// every leaf, and independently prove container/cgroup cleanup. Host skips are
// not physical backend evidence. Configuration still cannot select this backend.
func TestNamespaceWorkerLive(t *testing.T) {
	if os.Getenv("KELVO_TEST_NAMESPACE_WORKER") != "1" {
		t.Skip("requires the dedicated read-only PID-1 worker fixture")
	}
	limits := query.DefaultLimits()
	limits.MemoryMB, limits.MaxTempMB, limits.Threads = 64, 16, 1
	budget := containment.Budget{NativeOverheadMB: 64, ParentOverheadMB: 32, MaxProcesses: 64}
	native, err := budget.ProcessLimits(int64(limits.MemoryMB), limits.Threads)
	if err != nil {
		t.Fatal(err)
	}
	const fullMemory = 512 << 20
	pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: fullMemory, ScratchBytes: 64 << 20,
		Classes: map[admission.Class]admission.ClassLimits{admission.ClassExport: {MaxConcurrent: 1, MemoryBytes: fullMemory, ScratchBytes: 64 << 20}}})
	if err != nil {
		t.Fatal(err)
	}
	policy := containment.NamespacePolicy{Container: containment.Limits{MemoryBytes: fullMemory, MaxProcesses: 128, CPUQuotaMicros: 100000, CPUPeriodMicros: 100000},
		Native: native, ParentMemoryBytes: 32 << 20, CancellationGrace: query.WorkerCancellationGrace, CleanupTimeout: 3 * time.Second}
	domain, err := containment.OpenNamespaceDomain(policy, pool)
	if err != nil {
		t.Fatal(err)
	}
	scratchPath := t.TempDir()
	if err := os.Chmod(scratchPath, 0700); err != nil {
		t.Fatal(err)
	}
	scratch, err := OpenScratchRoot(scratchPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := domain.Close(context.Background()); err != nil {
			t.Error("native domain did not close", err)
		}
		if err := scratch.Close(); err != nil {
			t.Error("scratch ownership did not close", err)
		}
	})
	executor, err := New(catalog.Config{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	executor.Containment, executor.ContainmentBudget = domain, budget
	executor.ResourcePool, executor.ResourceOverheadBytes = pool, fullMemory-(int64(limits.MemoryMB)<<20)
	executor.ScratchRoot, executor.SandboxPath = scratch, os.Getenv("KELVO_TEST_SANDBOX")
	if executor.SandboxPath == "" || os.Getenv("KELVO_TEST_BINARY") == "" {
		t.Fatal("fixture did not supply the exact built worker and launcher")
	}
	assertIdle := func(t *testing.T) {
		t.Helper()
		if pool.Snapshot().Active != 0 || pool.Snapshot().Used.MemoryBytes != 0 || domain.Err() != nil {
			t.Fatal("worker did not return to reusable idle custody", pool.Snapshot(), domain.Err())
		}
	}
	t.Run("optional-descriptor-slots", func(t *testing.T) {
		reservation, err := pool.Acquire(context.Background(), admission.Request{MemoryBytes: fullMemory})
		if err != nil {
			t.Fatal(err)
		}
		custody, _ := containment.NewReservationCustody(reservation)
		defer custody.Complete()
		hold, _ := custody.Hold()
		process, err := domain.PrepareProcess(custody, native, hold)
		if err != nil {
			hold()
			t.Fatal(err)
		}
		defer process.Finish(context.Background())
		null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer null.Close()
		command := exec.Command("/probe/supervisor-probe", "optional-fd")
		command.Dir = "/"
		command.Stdin, command.Stdout, command.Stderr = null, null, null
		command.ExtraFiles = []*os.File{nil, null}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := process.Start(ctx, command); err != nil {
			t.Fatal(err)
		}
		if err := process.Wait(); err != nil {
			t.Fatal("the native child observed different descriptor numbers", err)
		}
		usage, err := process.Finish(context.Background())
		if err != nil || usage.Scope != "container_cgroup_lifetime" || usage.MemoryPeakBytes == 0 {
			t.Fatal("native cleanup lost whole-container usage scope", usage, err)
		}
		custody.Complete()
		assertIdle(t)
	})
	t.Run("reused-query", func(t *testing.T) {
		for range 2 {
			stats, err := executor.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, &workerTestSink{})
			if err != nil || stats.Rows != 3 {
				t.Fatal("native query path failed", stats, err)
			}
			assertIdle(t)
		}
	})
	t.Run("duckdb-cte", func(t *testing.T) {
		copy := *executor
		copy.Binary = os.Getenv("KELVO_TEST_BINARY")
		path := filepath.Join(t.TempDir(), "events.csv")
		if err := os.WriteFile(path, []byte("region,amount\na,10\na,20\nb,5\n"), 0600); err != nil {
			t.Fatal(err)
		}
		copy.Config = catalog.Config{Sources: []catalog.Source{{ID: "events", Type: "csv", Path: path,
			Options: map[string]string{"buffer_size": "1048576", "maximum_line_size": "262144"}}}}
		var values []int64
		sink := &workerTestSink{write: func(batch arrow.RecordBatch) error {
			column, ok := batch.Column(1).(*array.Int64)
			if !ok {
				return errors.New("result changed integer width")
			}
			for i := 0; i < column.Len(); i++ {
				values = append(values, column.Value(i))
			}
			return nil
		}}
		stats, err := copy.Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: "WITH base AS (SELECT region, CAST(amount AS BIGINT) amount FROM events) SELECT region, CAST(sum(amount) AS BIGINT) FROM base GROUP BY region ORDER BY region"}, sink)
		if err != nil || stats.Rows != 2 || len(values) != 2 || values[0] != 30 || values[1] != 5 {
			t.Fatal("native DuckDB CTE failed", stats, values, err)
		}
		assertIdle(t)
	})
	for _, class := range []admission.Class{admission.ClassRefresh, admission.ClassExport} {
		t.Run(string(class)+"-custody", func(t *testing.T) {
			reservation, err := pool.Acquire(context.Background(), admission.Request{Class: class, MemoryBytes: fullMemory, ScratchBytes: 32 << 20})
			if err != nil {
				t.Fatal(err)
			}
			custody, _ := containment.NewReservationCustody(reservation)
			defer custody.Complete()
			copy := *executor
			copy.ResourcePool = nil
			if _, err := copy.Execute(containment.WithCustody(context.Background(), custody), query.Request{SQL: "SELECT 1"}, &workerTestSink{}); err != nil {
				t.Fatal(err)
			}
			if pool.Snapshot().Active != 1 || custody.State().Held != 0 || custody.State().Completed {
				t.Fatal("native exit completed the outer publication custody", custody.State())
			}
			custody.Complete()
			assertIdle(t)
		})
	}
	cfg := operationExecutable(t)
	t.Run("partial-admission-before-resolver", func(t *testing.T) {
		copy := *executor
		copy.ResourceOverheadBytes = 160 << 20
		input := operationProcessInput(t, operations.StatementExecute, "fixture")
		resolved := false
		receipt, err := copy.ExecuteResolvedOperation(context.Background(), cfg, input.OperationID, input.RequestSHA256,
			func(context.Context) (adapter.ProcessRequest, error) { resolved = true; return input, nil }, &workerTestSink{})
		if err == nil || resolved || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone {
			t.Fatal("partial reservation reached credentials or native launch", receipt, err)
		}
		assertIdle(t)
	})
	for _, test := range []struct {
		name, mode string
		kind       operations.Kind
		fail       bool
	}{
		{"operation-read", "fixture", operations.QueryRead, false},
		{"operation-committed-error", "committed_exit_error", operations.StatementExecute, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := operationProcessInput(t, test.kind, test.mode)
			receipt, err := executor.ExecuteOperation(context.Background(), cfg, input, &workerTestSink{})
			if (err != nil) != test.fail || receipt.Outcome != operations.Completed || (test.kind.Mutating() && receipt.Effect != operations.EffectCommitted) {
				t.Fatal("native operation lost its observed receipt", receipt, err)
			}
			assertIdle(t)
		})
	}
	t.Run("operation-cancel-descendants", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		input := operationProcessInput(t, operations.QueryRead, "descendant")
		if _, err := executor.ExecuteOperation(ctx, cfg, input, &workerTestSink{}); err == nil {
			t.Fatal("cancelled operation succeeded")
		}
		assertIdle(t)
	})
	t.Run("blocked-sink-custody", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		entered, resume := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(resume) })
		sink := &workerTestSink{write: func(arrow.RecordBatch) error { close(entered); <-resume; return nil }}
		done := make(chan error, 1)
		go func() { _, err := executor.Execute(ctx, query.Request{SQL: "SELECT 1"}, sink); done <- err }()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("native result did not reach the blocked sink")
		}
		<-ctx.Done()
		if pool.Snapshot().Active != 1 || pool.Snapshot().Used.MemoryBytes != fullMemory {
			t.Fatal("deadline released borrowed output capacity")
		}
		if _, err := pool.TryAcquire(admission.Request{MemoryBytes: fullMemory}); !errors.Is(err, admission.ErrBusy) {
			t.Fatal("blocked sink admitted another operation", err)
		}
		once.Do(func() { close(resume) })
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("cancelled sink returned success")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("output completion did not join native cleanup")
		}
		assertIdle(t)
	})
	if t.Failed() {
		return
	}
	t.Log("NAMESPACE_WORKER_ACCEPTED query=true duckdb_cte=true refresh=true export=true operation=true cancellation=true blocked_sink=true admission_slots=1")
}
