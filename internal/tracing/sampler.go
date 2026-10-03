// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package tracing

import (
	"context"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// A propagated sampled bit cannot bypass this node's configured ratio, and an
// unsampled parent cannot be re-enabled downstream. Roots use the local ratio.
type ceilingSampler struct{ ratio sdktrace.Sampler }

func (s ceilingSampler) ShouldSample(parameters sdktrace.SamplingParameters) sdktrace.SamplingResult {
	parent := cleanSpanContext(trace.SpanContextFromContext(parameters.ParentContext))
	parameters.ParentContext = context.Background()
	if parent.IsValid() {
		if !parent.IsSampled() {
			return sdktrace.SamplingResult{Decision: sdktrace.Drop}
		}
		parameters.ParentContext = trace.ContextWithSpanContext(parameters.ParentContext, parent)
	}
	// Ratio sampling needs only IDs. Do not allow ambient sampler state or
	// attributes to enter the resulting trace context.
	return sdktrace.SamplingResult{Decision: s.ratio.ShouldSample(parameters).Decision}
}

func (s ceilingSampler) Description() string { return "KelvoLocalSamplingCeiling" }
