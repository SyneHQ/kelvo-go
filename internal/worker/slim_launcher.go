// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// slimRecheckEvery bounds how often a degraded launcher re-verifies the slim
// binary: each check hashes the file.
const slimRecheckEvery = 15 * time.Second

// SlimFallbackLauncher starts warm workers on the slim relational adapter and
// keeps a read from failing when that binary stops being usable. A launch
// asked for the slim binary that fails while the binary no longer verifies
// (missing, replaced, wrong digest or mode) is retried on the full adapter, and
// later launches use the full adapter until a check finds the slim binary
// usable again. The first fall back is logged once. Launches for any other
// binary go straight to the wrapped launcher.
type SlimFallbackLauncher struct {
	inner      WorkerLauncher
	slim, full OperationProcessConfig
	usable     func(context.Context, OperationProcessConfig) bool

	fallbacks atomic.Int64 // launches run on the full adapter instead of the slim one
	mu        sync.Mutex
	degraded  bool
	checked   time.Time
	logged    bool
}

// NewSlimFallbackLauncher wraps inner. slim is the pin native reads pass as
// the pool's adapter; full is what a launch falls back to.
func NewSlimFallbackLauncher(inner WorkerLauncher, slim, full OperationProcessConfig) *SlimFallbackLauncher {
	if inner == nil {
		inner = &defaultWorkerLauncher{}
	}
	return &SlimFallbackLauncher{inner: inner, slim: slim, full: full, usable: OperationBinaryUsable}
}

// Fallbacks counts launches that ran on the full adapter because the slim
// binary was not usable.
func (l *SlimFallbackLauncher) Fallbacks() int64 { return l.fallbacks.Load() }

// Degraded reports whether launches currently use the full adapter.
func (l *SlimFallbackLauncher) Degraded() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.degraded
}

func (l *SlimFallbackLauncher) Launch(ctx context.Context, e *Executor, cfg OperationProcessConfig, limits query.Limits) (WorkerProcess, func() error, error) {
	if cfg.Binary != l.slim.Binary || cfg.SHA256 != l.slim.SHA256 {
		return l.inner.Launch(ctx, e, cfg, limits)
	}
	if l.stillDegraded(ctx) {
		l.fallbacks.Add(1)
		return l.inner.Launch(ctx, e, l.full, limits)
	}
	proc, cleanup, err := l.inner.Launch(ctx, e, cfg, limits)
	if err == nil || ctx.Err() != nil || l.usable(ctx, l.slim) {
		return proc, cleanup, err // a failure that is not the slim binary's
	}
	l.degrade()
	l.fallbacks.Add(1)
	return l.inner.Launch(ctx, e, l.full, limits)
}

// stillDegraded reports whether this launch should use the full adapter,
// re-verifying the slim binary at most every slimRecheckEvery.
func (l *SlimFallbackLauncher) stillDegraded(ctx context.Context) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.degraded {
		return false
	}
	if time.Since(l.checked) < slimRecheckEvery {
		return true
	}
	l.checked = time.Now()
	if l.usable(ctx, l.slim) {
		l.degraded = false
		return false
	}
	return true
}

func (l *SlimFallbackLauncher) degrade() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.degraded, l.checked = true, time.Now()
	if !l.logged {
		l.logged = true
		log.Print("kelvo: the slim relational read adapter failed its launch check; read workers use the full adapter until it verifies again")
	}
}
