// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

func TestExecutorIPCMetricHook(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		for _, truncated := range []bool{false, true} {
			data := ipcFixture(t)
			if truncated {
				data = data[:len(data)-8]
			}
			metrics := telemetry.New()
			e := &Executor{Metrics: metrics}
			ctx := context.Background()
			kind := telemetry.KindQuery
			if refresh {
				ctx = WithRefreshTelemetry(ctx)
				kind = telemetry.KindRefresh
			}
			stats, err := e.readIPC(ctx, bytes.NewReader(data), query.DefaultLimits(), &workerTestSink{})
			if (err != nil) != truncated {
				t.Fatalf("refresh=%v truncated=%v error=%v", refresh, truncated, err)
			}
			status := telemetry.IPCTransferComplete
			if truncated {
				status = telemetry.IPCTransferIncomplete
			}
			s := metrics.Snapshot().IPCTransfer
			if s.Calls[kind][status] != 1 || s.InputBytes[kind][status] != uint64(len(data)) || s.DecodedBytes[kind][status] != uint64(stats.Bytes) || s.Batches[kind][status] != uint64(stats.Batches) {
				t.Fatalf("wrong parent IPC counters: %+v stats=%+v", s, stats)
			}
		}
	}
}
