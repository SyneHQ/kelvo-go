// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"log"
	"strings"
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
	logFailedRead(ctx, start, *resultErr, phases)
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

// logFailedRead writes one line per failed read: code, coarse class, the stage
// that was running and the elapsed time. It never includes SQL, parameters,
// credentials, DSNs or the error text, which can carry source details.
func logFailedRead(ctx context.Context, start time.Time, err error, phases *executionPhases) {
	if err == nil {
		return
	}
	if refresh, _ := ctx.Value(refreshTelemetryKey{}).(bool); refresh {
		return
	}
	code := query.PublicError(err).Code
	log.Printf("kelvo read failed: code=%s class=%s stage=%s duration_ms=%d", code, failureClass(err, code), failureStage(phases), time.Since(start).Milliseconds())
}

func failureClass(err error, code string) string {
	switch {
	case errors.Is(err, admission.ErrBusy), errors.Is(err, admission.ErrOversize), code == "RESOURCE_EXHAUSTED":
		return "capacity"
	case errors.Is(err, admission.ErrDraining):
		return "draining"
	case errors.Is(err, context.DeadlineExceeded), code == "DEADLINE_EXCEEDED":
		return "deadline"
	case errors.Is(err, context.Canceled), code == "CANCELLED":
		return "cancelled"
	case code == "INVALID_ARGUMENT", code == "PERMISSION_DENIED", code == "CONFIGURATION_ERROR":
		return "request"
	case code == "UNAVAILABLE", code == "DATASET_UNAVAILABLE":
		return "unavailable"
	}
	return "execution"
}

// failureStage names where a read stopped. The node cannot separate source
// connect from execute: both run inside the one-shot child, so they report as
// execute until the first batch and drain after it.
func failureStage(p *executionPhases) string {
	if p == nil {
		return "unknown"
	}
	if p.resolving {
		return "resolve"
	}
	switch p.reached {
	case telemetry.PhaseValidation:
		return "validation"
	case telemetry.PhaseNodeAdmission, telemetry.PhaseSourceAdmission:
		return "admission"
	case telemetry.PhasePrepare:
		return "prepare"
	case telemetry.PhaseExecutionDelivery:
		if p.timings.HasFirstBatch {
			return "drain"
		}
		return "execute"
	}
	return strings.ToLower(p.reached.Name())
}

func observeResolve(metrics *telemetry.Registry, ctx context.Context, d time.Duration) {
	kind := telemetry.KindQuery
	if refresh, _ := ctx.Value(refreshTelemetryKey{}).(bool); refresh {
		kind = telemetry.KindRefresh
	}
	metrics.ObserveResolve(kind, d)
}
