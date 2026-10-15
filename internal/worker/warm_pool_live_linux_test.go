//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapter/frame"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sys/unix"
)

// operationFixtureServe is the warm counterpart of operationFixtureMain. It
// answers each request frame with the process ID and a per-process counter, so
// the parent can prove that one sandboxed process served several operations.
func operationFixtureServe() int {
	served := int64(0)
	if hello, err := frame.Read(os.Stdin); err != nil || frame.CheckHello(hello) != nil || frame.WriteHello(os.Stdout) != nil {
		return 39
	}
	for {
		in, err := frame.Read(os.Stdin)
		kind, payload, stream := in.Type, in.Payload, in.Stream
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0
			}
			return 40
		}
		switch kind {
		case frame.Close:
			return 0
		case frame.Ping:
			if frame.WritePong(os.Stdout) != nil {
				return 41
			}
			continue
		case frame.Request:
		default:
			return 42
		}
		input, err := adapter.ParseProcessRequest(payload)
		clear(payload)
		if err != nil {
			return 43
		}
		for _, key := range []string{"KELVO_TOKEN", "AWS_SECRET_ACCESS_KEY", "KELVO_FIXTURE_PARENT_SECRET"} {
			if _, present := os.LookupEnv(key); present {
				return 44
			}
		}
		if input.Source.Password != "fixture-private-value" || len(os.Environ()) != 5 {
			return 45
		}
		var lease, binary unix.Stat_t
		if unix.Fstat(4, &lease) != nil || lease.Mode&unix.S_IFMT != unix.S_IFREG || unix.Fstat(5, &binary) != nil || binary.Mode&unix.S_IFMT != unix.S_IFREG {
			return 46
		}
		// The sandbox still denies paths outside its grants in serve mode.
		if _, err := os.ReadFile("/etc/passwd"); !errors.Is(err, os.ErrPermission) {
			return 47
		}
		served++
		// "SELECT slow" keeps the worker busy, for the capacity gate.
		if input.Request.Spec.Query != nil && input.Request.Spec.Query.SQL == "SELECT slow" {
			time.Sleep(1500 * time.Millisecond)
		}
		values := []int64{int64(os.Getpid()), served}
		schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		builder.AppendValues(values, nil)
		column := builder.NewArray()
		builder.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(len(values)))
		column.Release()
		var wire bytes.Buffer
		writer := ipc.NewWriter(&wire, ipc.WithSchema(schema))
		writeErr := writer.Write(record)
		record.Release()
		if writeErr != nil || writer.Close() != nil {
			return 48
		}
		if frame.WriteResult(os.Stdout, stream, wire.Bytes()) != nil {
			return 49
		}
		hash := sha256.Sum256(wire.Bytes())
		receipt := operations.Receipt{Version: operations.Version, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256, Outcome: operations.Completed, Effect: operations.EffectNone,
			Result: &operations.ResultRef{ID: input.OperationID, SHA256: hex.EncodeToString(hash[:]), Bytes: int64(wire.Len()), Rows: int64(len(values)), Format: "arrow_ipc"}}
		raw, err := json.Marshal(receipt)
		if err != nil || receipt.Validate() != nil || frame.WriteReceipt(os.Stdout, stream, raw) != nil {
			return 50
		}
	}
}

// This gate starts a warm worker through the real Landlock launcher inside a
// delegated cgroup. A fake launcher cannot show that the entrypoint is allowed,
// that the descriptor layout is right or that the worker outlives a request.
func TestContainedWarmWorkerServesRepeatedRequests(t *testing.T) {
	executor, manager, _ := containedExecutor(t)
	pool, err := admission.New(admission.Limits{MaxConcurrent: 2, MemoryBytes: 512 << 20, ScratchBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	executor.ResourcePool = pool
	cfg := operationExecutable(t)
	warm := DefaultWarmPoolConfig()
	warm.Enabled = true
	mgr, err := NewWarmPoolManager(warm, nil)
	if err != nil {
		t.Fatal(err)
	}
	executor.WarmPool = mgr
	drained := false
	defer func() {
		if !drained {
			_ = mgr.Drain(context.Background())
		}
	}()

	var seen [][]int64
	for i := 0; i < 3; i++ {
		// Each request has its own context: the worker must survive its end.
		ctx, cancel := context.WithCancel(context.Background())
		var values []int64
		sink := &workerTestSink{write: func(batch arrow.RecordBatch) error {
			values = append(values, batch.Column(0).(*array.Int64).Int64Values()...)
			return nil
		}}
		// Interactive reads use the warm pool; durable operations never do.
		receipt, err := executor.ExecuteOperation(withAdmissionClass(ctx, admission.ClassInteractive), cfg, operationProcessInput(t, operations.QueryRead, "serve"), sink)
		cancel()
		if err != nil || receipt.Outcome != operations.Completed || len(values) != 2 {
			t.Fatal("warm request failed", i, receipt, err, values)
		}
		seen = append(seen, values)
	}
	if seen[0][0] != seen[1][0] || seen[1][0] != seen[2][0] || seen[0][1] != 1 || seen[1][1] != 2 || seen[2][1] != 3 {
		t.Fatal("requests were not served by one warm process", seen)
	}
	metrics := mgr.Metrics()
	if metrics.Spawns.Load() != 1 || metrics.Hits.Load() != 2 || metrics.Fallbacks.Load() != 0 {
		t.Fatal("unexpected pool activity", metrics.Spawns.Load(), metrics.Hits.Load(), metrics.Fallbacks.Load())
	}
	if pool.Snapshot().Active != 1 {
		t.Fatal("idle warm worker does not hold exactly one reservation", pool.Snapshot().Active)
	}

	// The idle worker holds one slot. A one-shot mutation still runs. A second
	// key finds no free slot for a worker of its own: it evicts the idle
	// worker (least recently used) and launches one, instead of falling back.
	write, err := executor.ExecuteOperation(context.Background(), cfg, operationProcessInput(t, operations.StatementExecute, "fixture"), &workerTestSink{})
	if err != nil || write.Outcome != operations.Completed || write.Effect != operations.EffectCommitted {
		t.Fatal("one-shot operation blocked by an idle warm worker", write, err)
	}
	other := operationProcessInput(t, operations.QueryRead, "fixture")
	other.AppTeam = "another-team"
	evicting, err := executor.ExecuteOperation(withAdmissionClass(context.Background(), admission.ClassInteractive), cfg, other, &workerTestSink{})
	if err != nil || evicting.Outcome != operations.Completed || metrics.Fallbacks.Load() != 0 || metrics.Spawns.Load() != 2 || metrics.EvictionsLRU.Load() != 1 {
		t.Fatal("a new key did not take over the idle worker's slot", evicting, err, metrics.Fallbacks.Load(), metrics.Spawns.Load(), metrics.EvictionsLRU.Load())
	}

	drained = true
	if err := mgr.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("drain leaked a warm worker", pool.Snapshot().Active, manager.Status().Active)
	}
	if reclaimed, err := executor.ScratchRoot.Reclaim(); err != nil || reclaimed != 0 {
		t.Fatal("drain left warm scratch behind", reclaimed, err)
	}
}

// This gate runs the real kelvo-adapter-go binary in serve mode through the
// real launcher against a real PostgreSQL server. pg_backend_pid() identifies
// the server-side connection: a warm worker must answer every request on the
// same one, and one-shot operations must each open their own.
func TestContainedWarmAdapterReusesPostgresConnection(t *testing.T) {
	adapterPath, dsn := os.Getenv("KELVO_TEST_WARM_ADAPTER_BINARY"), os.Getenv("KELVO_TEST_WARM_POSTGRES_DSN")
	if adapterPath == "" || dsn == "" {
		t.Skip("set KELVO_TEST_WARM_ADAPTER_BINARY and KELVO_TEST_WARM_POSTGRES_DSN for the real adapter gate")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || strings.Trim(parsed.Path, "/") == "" {
		t.Fatal("invalid PostgreSQL fixture address")
	}
	database := strings.Trim(parsed.Path, "/")
	adapterPath, err = filepath.EvalSymlinks(adapterPath)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(adapterPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	cfg := OperationProcessConfig{Binary: adapterPath, SHA256: hex.EncodeToString(sum[:])}

	executor, manager, _ := containedExecutor(t)
	pool, err := admission.New(admission.Limits{MaxConcurrent: 2, MemoryBytes: 512 << 20, ScratchBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	executor.ResourcePool = pool

	backend := func(label string) int64 {
		t.Helper()
		request := operations.Request{Version: operations.Version, Kind: operations.QueryRead,
			Connection: operations.ConnectionRef{ID: "warm-postgres", Database: database, Schema: "public"},
			Spec:       operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT pg_backend_pid()::bigint AS pid"}}}
		digest, err := operations.Digest(request)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now().Unix()
		input := adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: "operation-test", RequestSHA256: digest, Request: request,
			Limits: adapter.ProcessLimits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 10, TimeoutMS: 10000},
			Source: adapter.ConnectionSpec{Engine: "postgres", DSN: dsn, TenantID: "tenant-a", ConnectionID: request.Connection.ID,
				Revision: "revision-1", Database: database, Schema: "public"},
			CredentialsValidUntil: now + 5, ExpiresAt: now + 60}
		var values []int64
		sink := &workerTestSink{write: func(batch arrow.RecordBatch) error {
			values = append(values, batch.Column(0).(*array.Int64).Int64Values()...)
			return nil
		}}
		ctx, cancel := context.WithCancel(context.Background())
		receipt, err := executor.ExecuteOperation(withAdmissionClass(ctx, admission.ClassInteractive), cfg, input, sink)
		cancel()
		if err != nil || receipt.Outcome != operations.Completed || len(values) != 1 || values[0] < 1 {
			t.Fatal(label, "query failed", receipt.Outcome, receipt.ErrorCode, err, values)
		}
		return values[0]
	}

	warm := DefaultWarmPoolConfig()
	warm.Enabled = true
	mgr, err := NewWarmPoolManager(warm, nil)
	if err != nil {
		t.Fatal(err)
	}
	executor.WarmPool = mgr
	drained := false
	defer func() {
		if !drained {
			_ = mgr.Drain(context.Background())
		}
	}()
	first, second := backend("warm-1"), backend("warm-2")
	// Outlast the five-second credential window of the earlier requests.
	time.Sleep(6 * time.Second)
	third := backend("warm-3")
	metrics := mgr.Metrics()
	if first != second || second != third || metrics.Spawns.Load() != 1 || metrics.Hits.Load() != 2 || metrics.Fallbacks.Load() != 0 {
		t.Fatal("warm worker did not reuse one PostgreSQL connection", first, second, third, metrics.Spawns.Load(), metrics.Hits.Load(), metrics.Fallbacks.Load())
	}
	drained = true
	if err := mgr.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("drain leaked the warm adapter", pool.Snapshot().Active, manager.Status().Active)
	}

	executor.WarmPool = nil
	coldA, coldB := backend("cold-1"), backend("cold-2")
	if coldA == coldB || coldA == first || coldB == first {
		t.Fatal("one-shot operations shared a PostgreSQL connection", first, coldA, coldB)
	}
	t.Logf("warm backend %d served three requests; one-shot backends %d and %d", first, coldA, coldB)
}

// This gate checks the warm pool's capacity contract on the real launcher, in
// a node with room for one worker:
//   - every worker busy: a read for another key waits queue_timeout and is
//     refused with RESOURCE_EXHAUSTED and a retry hint, and no one-shot child
//     runs it;
//   - the worker idle again: the next read evicts it and launches its own;
//   - a launch that fails (here, a worker too large for the node) still falls
//     back to the one-shot path, since nothing was sent.
func TestContainedWarmPoolCapacityContract(t *testing.T) {
	// The node has room for one worker: through the spare-slot rule when the
	// classes share capacity, through the interactive share when separate.
	t.Run("shared classes", func(t *testing.T) {
		warmPoolCapacityContract(t, admission.Limits{MaxConcurrent: 2, MemoryBytes: 512 << 20, ScratchBytes: 64 << 20})
	})
	t.Run("separate classes", func(t *testing.T) {
		warmPoolCapacityContract(t, admission.Limits{MaxConcurrent: 3, MemoryBytes: 512 << 20, ScratchBytes: 64 << 20,
			ReservedSlots: 1, ReservedMemoryBytes: 256 << 20, ReservedScratchBytes: 32 << 20, SeparateClasses: true})
	})
}

func warmPoolCapacityContract(t *testing.T, limits admission.Limits) {
	executor, _, _ := containedExecutor(t)
	pool, err := admission.New(limits)
	if err != nil {
		t.Fatal(err)
	}
	executor.ResourcePool = pool
	cfg := operationExecutable(t)
	warm := DefaultWarmPoolConfig()
	warm.Enabled = true
	warm.QueueTimeout = 500 * time.Millisecond
	mgr, err := NewWarmPoolManager(warm, nil)
	if err != nil {
		t.Fatal(err)
	}
	executor.WarmPool = mgr
	defer func() { _ = mgr.Drain(context.Background()) }()
	read := func(ctx context.Context, team, mode string) (operations.Receipt, error) {
		input := operationProcessInput(t, operations.QueryRead, mode)
		input.AppTeam = team
		// Interactive reads use the warm pool; durable operations never do.
		return executor.ExecuteOperation(withAdmissionClass(ctx, admission.ClassInteractive), cfg, input, &workerTestSink{})
	}

	busy := make(chan error, 1)
	go func() { _, err := read(context.Background(), "team-a", "slow"); busy <- err }()
	waitFor := func(cond func() bool) {
		for i := 0; !cond(); i++ {
			if i > 500 {
				t.Fatal("condition not met")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitFor(func() bool { return mgr.Metrics().Spawns.Load() == 1 })
	start := time.Now()
	refused, err := read(context.Background(), "team-b", "serve")
	waited := time.Since(start)
	var public *query.Error
	if !errors.As(err, &public) || public.Code != "RESOURCE_EXHAUSTED" || !strings.Contains(public.Message, "retry after") ||
		refused.ErrorCode != "RESOURCE_EXHAUSTED" || waited < warm.QueueTimeout || mgr.Metrics().Fallbacks.Load() != 0 {
		t.Fatal("a read with every worker busy was not refused after the wait", refused, err, waited, mgr.Metrics().Fallbacks.Load())
	}
	if err := <-busy; err != nil {
		t.Fatal(err)
	}
	replaced, err := read(context.Background(), "team-b", "serve")
	if err != nil || replaced.Outcome != operations.Completed || mgr.Metrics().EvictionsLRU.Load() != 1 || mgr.Metrics().Spawns.Load() != 2 || mgr.Metrics().Fallbacks.Load() != 0 {
		t.Fatal("the idle worker was not evicted and replaced", replaced, err, mgr.Metrics().EvictionsLRU.Load(), mgr.Metrics().Spawns.Load())
	}
	if err := mgr.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}

	// A worker larger than the node: the launch fails and the read runs one-shot.
	tooLarge := DefaultWarmPoolConfig()
	tooLarge.Enabled, tooLarge.WorkerMemoryMB = true, 1024
	failing, err := NewWarmPoolManager(tooLarge, nil)
	if err != nil {
		t.Fatal(err)
	}
	executor.WarmPool = failing
	defer func() { _ = failing.Drain(context.Background()) }()
	oneShot, err := read(context.Background(), "team-c", "fixture")
	if err != nil || oneShot.Outcome != operations.Completed || failing.Metrics().Fallbacks.Load() != 1 || failing.Metrics().Spawns.Load() != 0 {
		t.Fatal("a failed launch did not fall back to one-shot", oneShot, err, failing.Metrics().Fallbacks.Load(), failing.Metrics().Spawns.Load())
	}
	// Within the failure's backoff window the next read skips the launch.
	again, err := read(context.Background(), "team-c", "fixture")
	if err != nil || again.Outcome != operations.Completed || failing.Metrics().Fallbacks.Load() != 2 || failing.Metrics().LaunchFailures.Load() != 1 {
		t.Fatal("a read inside the backoff window relaunched", again, err, failing.Metrics().Fallbacks.Load(), failing.Metrics().LaunchFailures.Load())
	}
}
