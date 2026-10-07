// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"io"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

// readIPC records the parent's byte boundary, independently of child reports.
// A complete IPC read is not a committed query result or a successful operation.
func (e *Executor) readIPC(ctx context.Context, input io.Reader, limits query.Limits, sink query.Sink) (query.Stats, error) {
	if e.Metrics == nil {
		return readWorkerIPC(ctx, input, limits, sink)
	}
	var transfer telemetry.IPCTransfer
	stats, err := readWorkerIPCObserved(ctx, input, limits, sink, &transfer)
	kind := telemetry.KindQuery
	if refresh, _ := ctx.Value(refreshTelemetryKey{}).(bool); refresh {
		kind = telemetry.KindRefresh
	}
	e.Metrics.ObserveIPCTransfer(kind, &transfer)
	return stats, err
}
