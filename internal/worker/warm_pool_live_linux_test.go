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
	"os"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapter/frame"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
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
	for {
		kind, payload, err := frame.Read(os.Stdin)
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
		if frame.WriteResult(os.Stdout, wire.Bytes()) != nil {
			return 49
		}
		hash := sha256.Sum256(wire.Bytes())
		receipt := operations.Receipt{Version: operations.Version, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256, Outcome: operations.Completed, Effect: operations.EffectNone,
			Result: &operations.ResultRef{ID: input.OperationID, SHA256: hex.EncodeToString(hash[:]), Bytes: int64(wire.Len()), Rows: int64(len(values)), Format: "arrow_ipc"}}
		raw, err := json.Marshal(receipt)
		if err != nil || receipt.Validate() != nil || frame.WriteReceipt(os.Stdout, raw) != nil {
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
		receipt, err := executor.ExecuteOperation(ctx, cfg, operationProcessInput(t, operations.QueryRead, "serve"), sink)
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

	// The idle worker holds one slot. A one-shot mutation still runs, and a
	// second key cannot take the last slot: it falls back to one-shot too.
	write, err := executor.ExecuteOperation(context.Background(), cfg, operationProcessInput(t, operations.StatementExecute, "fixture"), &workerTestSink{})
	if err != nil || write.Outcome != operations.Completed || write.Effect != operations.EffectCommitted {
		t.Fatal("one-shot operation blocked by an idle warm worker", write, err)
	}
	other := operationProcessInput(t, operations.QueryRead, "fixture")
	other.AppTeam = "another-team"
	fallback, err := executor.ExecuteOperation(context.Background(), cfg, other, &workerTestSink{})
	if err != nil || fallback.Outcome != operations.Completed || metrics.Fallbacks.Load() != 1 || metrics.Spawns.Load() != 1 {
		t.Fatal("request without a free warm slot did not fall back", fallback, err, metrics.Fallbacks.Load(), metrics.Spawns.Load())
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
