//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

func TestContainedOperationCleanupObserverAfterPhysicalDrain(t *testing.T) {
	for _, mode := range []string{"fixture", "cancelled", "committed_exit_error", "ack-failure"} {
		t.Run(mode, func(t *testing.T) {
			executor, manager, pool := containedExecutor(t)
			cfg := operationExecutable(t)
			kind, sourceMode := operations.QueryRead, mode
			if mode == "committed_exit_error" {
				kind = operations.StatementExecute
			}
			if mode == "ack-failure" {
				sourceMode = "fixture"
			}
			input := operationProcessInput(t, kind, sourceMode)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancelled" {
				sourceMode = "descendant"
				input = operationProcessInput(t, kind, sourceMode)
			}
			calls := 0
			ackErr := errors.New("fixture acknowledgement unavailable")
			sink := &operationCleanupTestSink{cleaned: func(ctx context.Context) error {
				calls++
				if ctx.Err() != nil || pool.Snapshot().Active != 1 || manager.Status().Active != 0 {
					t.Error("cleanup observer lacks independent context or proven drain")
				}
				if reclaimed, err := executor.ScratchRoot.Reclaim(); err != nil || reclaimed != 0 {
					t.Error("observer preceded scratch cleanup", reclaimed, err)
				}
				if mode == "ack-failure" {
					return ackErr
				}
				return nil
			}}
			if mode == "cancelled" {
				sink.workerTestSink.write = func(arrow.RecordBatch) error { cancel(); return nil }
			}
			receipt, err := executor.ExecuteOperation(parent, cfg, input, sink)
			if calls != 1 || pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
				t.Fatal("physical cleanup was not observed once", calls, err)
			}
			if mode == "fixture" && err != nil {
				t.Fatal(err)
			}
			if mode == "committed_exit_error" && (receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectCommitted) {
				t.Fatal("cleanup changed source commit evidence")
			}
			if mode == "ack-failure" && (!errors.Is(err, ackErr) || receipt.Outcome != operations.Completed) {
				t.Fatal("ack failure erased verified source result", err)
			}
		})
	}
}
