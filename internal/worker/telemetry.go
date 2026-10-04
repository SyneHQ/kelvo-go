// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
)

type refreshTelemetryKey struct{}

// WithRefreshTelemetry labels execution used by a snapshot refresh. It carries
// no identifiers or query data. Ordinary Execute calls are labeled query.
func WithRefreshTelemetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, refreshTelemetryKey{}, true)
}

// recordExecution observes the entire Execute call after cleanup, excluding its
// measured admission wait. Execution includes setup, computation and transfer;
// it is not database execution time alone or full distributed queue latency.
func recordExecution(metrics *telemetry.Registry, ctx context.Context, start time.Time, admissionWait *time.Duration, resultErr *error, recorders ...*tracing.Recorder) {
	recordExecutionPhases(metrics, ctx, start, admissionWait, resultErr, nil, recorders...)
}

func recordExecutionPhases(metrics *telemetry.Registry, ctx context.Context, start time.Time, admissionWait *time.Duration, resultErr *error, phases *executionPhases, recorders ...*tracing.Recorder) {
	if metrics == nil && (len(recorders) == 0 || recorders[0] == nil) {
		return
	}
	kind := telemetry.KindQuery
	if refresh, _ := ctx.Value(refreshTelemetryKey{}).(bool); refresh {
		kind = telemetry.KindRefresh
	}
	finished := time.Now()
	total := finished.Sub(start)
	timings := phases.finish(finished)
	metrics.ObservePhases(kind, timings, total)
	err := *resultErr
	if errors.Is(err, admission.ErrDraining) {
		metrics.Reject(kind, telemetry.RejectionDraining)
		return
	}
	if errors.Is(err, admission.ErrOversize) || errors.Is(err, admission.ErrBusy) {
		metrics.Reject(kind, telemetry.RejectionCapacity)
		return
	}
	outcome := telemetry.OutcomeSuccess
	if err != nil {
		outcome = telemetry.OutcomeError
		// Classify the returned result, not ctx.Err(): deferred worker cleanup may
		// cancel a successful call's derived context before this callback executes.
		code := query.PublicError(err).Code
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || code == "CANCELLED" || code == "DEADLINE_EXCEEDED" {
			outcome = telemetry.OutcomeCanceled
		}
	}
	duration := total - *admissionWait
	metrics.Observe(kind, outcome, *admissionWait, duration)
	if len(recorders) > 0 {
		recorders[0].Record(tracing.Event{Parent: tracing.CarrierFromContext(ctx), Kind: kind, Outcome: outcome, StartedAt: start, AdmissionWait: *admissionWait, Duration: duration, Phases: timings})
	}
}
