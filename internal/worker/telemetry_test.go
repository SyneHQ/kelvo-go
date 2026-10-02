// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

func TestExecutionTelemetryOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want telemetry.Outcome
	}{
		{"success", nil, telemetry.OutcomeSuccess},
		{"failure", errors.New("private error must not be retained"), telemetry.OutcomeError},
		{"cancel", context.Canceled, telemetry.OutcomeCanceled},
		{"deadline", fmt.Errorf("wrapped: %w", context.DeadlineExceeded), telemetry.OutcomeCanceled},
		{"typed cancel", query.NewError("CANCELLED", "canceled"), telemetry.OutcomeCanceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics := telemetry.New()
			// Even a canceled context cannot turn a successful result into a cancel.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			wait := time.Second
			recordExecution(metrics, ctx, time.Now().Add(-2*time.Second), &wait, &tc.err)
			s := metrics.Snapshot()
			if s.Outcomes[telemetry.KindQuery][tc.want] != 1 || s.QueueWait[telemetry.KindQuery].SumSeconds != 1 {
				t.Fatalf("unexpected metrics: %+v", s)
			}
			if duration := s.Duration[telemetry.KindQuery].SumSeconds; duration < 1 || duration > 1.5 {
				t.Fatalf("admission wait not subtracted: %f", duration)
			}
		})
	}
}

func TestExecutionTelemetryRejections(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want telemetry.Rejection
	}{
		{admission.ErrBusy, telemetry.RejectionCapacity},
		{fmt.Errorf("wrapped: %w", admission.ErrOversize), telemetry.RejectionCapacity},
		{admission.ErrDraining, telemetry.RejectionDraining},
	} {
		metrics := telemetry.New()
		wait := time.Duration(0)
		recordExecution(metrics, WithRefreshTelemetry(context.Background()), time.Now(), &wait, &tc.err)
		s := metrics.Snapshot()
		if s.Rejections[telemetry.KindRefresh][tc.want] != 1 {
			t.Fatalf("missing rejection: %+v", s)
		}
		if s.Duration[telemetry.KindRefresh].Count != 0 || s.Outcomes != ([2][3]uint64{}) {
			t.Fatal("rejection counted as execution")
		}
	}
}
