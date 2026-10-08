// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestStatementTimeoutMilliseconds(t *testing.T) {
	// Future test time separates deadline arithmetic from the test runner clock.
	now := time.Now().Add(time.Hour)
	for _, test := range []struct {
		name      string
		remaining time.Duration
		want      int64
		wantError bool
	}{
		{"expired", -time.Millisecond, 0, true},
		{"exact deadline", 0, 0, true},
		{"sub millisecond", time.Millisecond - 1, 0, true},
		{"one millisecond", time.Millisecond, 1, false},
		{"round down", 2999 * time.Microsecond, 2, false},
		{"server maximum", time.Duration(math.MaxInt32) * time.Millisecond, math.MaxInt32, false},
		{"cap long deadline", time.Duration(math.MaxInt64), math.MaxInt32, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithDeadline(context.Background(), now.Add(test.remaining))
			defer cancel()
			got, err := statementTimeoutMilliseconds(ctx, now)
			if got != test.want || (err != nil) != test.wantError {
				t.Fatalf("milliseconds=%d error=%v, want %d error=%v", got, err, test.want, test.wantError)
			}
			if test.wantError && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected deadline error, got %v", err)
			}
		})
	}
}

func TestStatementTimeoutRequiresLiveDeadline(t *testing.T) {
	if _, err := statementTimeoutMilliseconds(context.Background(), time.Now()); err == nil {
		t.Fatal("accepted a missing deadline")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	cancel()
	if _, err := statementTimeoutMilliseconds(ctx, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation changed: %v", err)
	}
}
