// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

// This gate tracks refresh lifetimes even when resource accounting is disabled.
// Enter and Drain share a lock so Wait cannot miss concurrently admitted work.
type refreshGate struct {
	mu       sync.Mutex
	draining bool
	active   int
	changed  chan struct{}
}

func newRefreshGate() *refreshGate { return &refreshGate{changed: make(chan struct{})} }
func (g *refreshGate) notify()     { close(g.changed); g.changed = make(chan struct{}) }
func (g *refreshGate) Enter() (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return nil, admission.ErrDraining
	}
	g.active++
	var once sync.Once
	return func() { once.Do(func() { g.mu.Lock(); defer g.mu.Unlock(); g.active--; g.notify() }) }, nil
}
func (g *refreshGate) Drain()           { g.mu.Lock(); defer g.mu.Unlock(); g.draining = true; g.notify() }
func (g *refreshGate) IsDraining() bool { g.mu.Lock(); defer g.mu.Unlock(); return g.draining }
func (g *refreshGate) Wait(ctx context.Context) error {
	for {
		g.mu.Lock()
		active, changed := g.active, g.changed
		g.mu.Unlock()
		if active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func withRefreshReservation(ctx context.Context, pool *admission.Pool, overhead int64, limits query.Limits, metrics *telemetry.Registry, run func(context.Context) error) (err error) {
	ctx, cancel := context.WithTimeout(ctx, limits.Timeout)
	defer cancel()
	started := time.Now()
	var wait time.Duration
	defer func() {
		// Admission failures are rejections, not completed refresh attempts.
		// Classify once here so errors returned through any path cannot double
		// count a rejection as an execution failure.
		if errors.Is(err, admission.ErrDraining) {
			metrics.Reject(telemetry.KindRefresh, telemetry.RejectionDraining)
			return
		}
		if errors.Is(err, admission.ErrOversize) || errors.Is(err, admission.ErrBusy) {
			metrics.Reject(telemetry.KindRefresh, telemetry.RejectionCapacity)
			return
		}
		outcome := telemetry.OutcomeSuccess
		if err != nil {
			outcome = telemetry.OutcomeError
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			outcome = telemetry.OutcomeCanceled
		}
		metrics.Observe(telemetry.KindRefresh, outcome, wait, time.Since(started)-wait)
	}()
	if pool != nil {
		reservation, acquireErr := pool.Acquire(ctx, admission.Request{MemoryBytes: (int64(limits.MemoryMB) << 20) + overhead, ScratchBytes: (int64(limits.MaxTempMB) << 20) + limits.MaxBytes})
		wait = time.Since(started)
		if acquireErr != nil {
			return acquireErr
		}
		defer reservation.Release()
	}
	return run(ctx)
}
