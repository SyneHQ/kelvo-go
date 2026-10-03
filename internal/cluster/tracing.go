// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
)

func jobTrace(carrier *tracing.Carrier) tracing.Carrier {
	if carrier == nil || !carrier.Valid() {
		return tracing.Carrier{}
	}
	return *carrier
}

func copyJobTrace(carrier *tracing.Carrier) *tracing.Carrier {
	return copyJobTraceValue(jobTrace(carrier))
}

func copyJobTraceValue(carrier tracing.Carrier) *tracing.Carrier {
	if !carrier.Valid() {
		return nil
	}
	return &carrier
}

func finishClusterTrace(span *tracing.Lifecycle, ctx context.Context, outcome *telemetry.Outcome) {
	value := *outcome
	if value != telemetry.OutcomeSuccess && ctx.Err() != nil {
		value = telemetry.OutcomeCanceled
	}
	span.End(value)
}

// Call only after request handlers join, including on constructor failure.
func (g *Gateway) closeTracing() error {
	if g.tracing == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return g.tracing.Shutdown(ctx)
}
