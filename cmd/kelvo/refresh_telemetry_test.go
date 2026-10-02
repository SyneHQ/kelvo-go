// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

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

func TestRefreshTelemetryAdmissionRejectsOnce(t *testing.T) {
	for _, draining := range []bool{false, true} {
		t.Run(fmt.Sprint("draining=", draining), func(t *testing.T) {
			pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			limits := query.Limits{MemoryMB: 2, Timeout: time.Second}
			wantErr, wantReason := admission.ErrOversize, telemetry.RejectionCapacity
			if draining {
				pool.Drain()
				limits.MemoryMB = 1
				wantErr, wantReason = admission.ErrDraining, telemetry.RejectionDraining
			}
			metrics := telemetry.New()
			err = withRefreshReservation(context.Background(), pool, 0, limits, metrics, func(context.Context) error {
				t.Fatal("rejected refresh executed")
				return nil
			})
			if !errors.Is(err, wantErr) {
				t.Fatalf("error=%v, want %v", err, wantErr)
			}
			want := telemetry.Snapshot{}
			want.Rejections[telemetry.KindRefresh][wantReason] = 1
			if got := metrics.Snapshot(); got != want {
				t.Fatalf("rejection counted as completion or twice: %+v", got)
			}
		})
	}
}

func TestRefreshTelemetryWrappedRejections(t *testing.T) {
	for _, tc := range []struct {
		err    error
		reason telemetry.Rejection
	}{
		{admission.ErrBusy, telemetry.RejectionCapacity},
		{admission.ErrOversize, telemetry.RejectionCapacity},
		{admission.ErrDraining, telemetry.RejectionDraining},
	} {
		metrics := telemetry.New()
		wrapped := fmt.Errorf("wrapped: %w", tc.err)
		err := withRefreshReservation(context.Background(), nil, 0, query.Limits{Timeout: time.Second}, metrics, func(context.Context) error { return wrapped })
		if err != wrapped {
			t.Fatal("helper replaced original error")
		}
		want := telemetry.Snapshot{}
		want.Rejections[telemetry.KindRefresh][tc.reason] = 1
		if got := metrics.Snapshot(); got != want {
			t.Fatalf("incorrect wrapped rejection: %+v", got)
		}
	}
}

func TestRefreshTelemetryTerminalOutcomesStayDistinct(t *testing.T) {
	for _, tc := range []struct {
		err     error
		outcome telemetry.Outcome
	}{
		{nil, telemetry.OutcomeSuccess},
		{admission.ErrInvalid, telemetry.OutcomeError},
		{errors.New("refresh publication failed"), telemetry.OutcomeError},
		{context.Canceled, telemetry.OutcomeCanceled},
		{context.DeadlineExceeded, telemetry.OutcomeCanceled},
	} {
		metrics := telemetry.New()
		_ = withRefreshReservation(context.Background(), nil, 0, query.Limits{Timeout: time.Second}, metrics, func(context.Context) error { return tc.err })
		got := metrics.Snapshot()
		wantOutcomes := [2][3]uint64{}
		wantOutcomes[telemetry.KindRefresh][tc.outcome] = 1
		if got.Outcomes != wantOutcomes || got.Rejections != ([2][2]uint64{}) || got.QueueWait[telemetry.KindRefresh].Count != 1 || got.Duration[telemetry.KindRefresh].Count != 1 {
			t.Fatalf("terminal outcome misclassified: %+v", got)
		}
	}
}
