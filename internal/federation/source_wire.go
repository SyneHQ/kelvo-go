// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// Some cloud native executors predate SourceWireBytes and report consumed
// response bodies as WireBytes. Adapt only the federation execution boundary;
// their native API statistics and synchronous borrowed sink remain unchanged.
func withSourceWire(engine execution, err error) (execution, error) {
	if err != nil {
		return nil, err
	}
	if engine == nil {
		return nil, query.NewError("CONFIGURATION_ERROR", "Native federation executor is unavailable")
	}
	return &sourceWireExecution{execution: engine}, nil
}

type sourceWireExecution struct{ execution }

func (e *sourceWireExecution) Execute(ctx context.Context, request query.Request, sink query.Sink) (query.Stats, error) {
	stats, err := e.execution.Execute(ctx, request, sink)
	if stats.SourceWireBytes == 0 && stats.WireBytes > 0 {
		stats.SourceWireBytes = stats.WireBytes
	}
	return stats, err
}
