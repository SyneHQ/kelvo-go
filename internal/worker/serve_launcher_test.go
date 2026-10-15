// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// An adapter built before --pool-size existed exits with a usage error at the
// unknown argument. The node sees its stdout close before the hello, treats the
// launch as failed, and the operation runs one-shot instead of being refused.
func TestWarmPool_AdapterRejectingServeArgumentsFallsBack(t *testing.T) {
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	launcher := &mockLauncher{onLaunch: func(context.Context) (WorkerProcess, func() error, error) {
		proc := newFakeWorkerProcess()
		go func() { // exits with status 2 without speaking: stdout closes, stdin is gone
			_ = proc.stdoutW.Close()
			_ = proc.stdinR.Close()
		}()
		return proc, proc.Kill, nil
	}}
	mgr, err := NewWarmPoolManager(cfg, launcher)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Drain(context.Background()) }()
	exec := &Executor{Limits: query.Limits{MemoryMB: 64, Timeout: 5 * time.Second}}
	req := testProcessRequest("op-1", "tenant-a", "team-a", "conn-1", strings.Repeat("1", 64))
	for i := 0; i < 2; i++ {
		if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, req, nil); !errors.Is(err, errWarmUnavailable) {
			t.Fatalf("adapter that rejects its arguments was not a fallback: %v", err)
		}
	}
	mgr.mu.Lock()
	leaked := len(mgr.workers)
	mgr.mu.Unlock()
	if leaked != 0 || mgr.metrics.Fallbacks.Load() != 2 || mgr.metrics.Spawns.Load() != 0 {
		t.Fatalf("leaked=%d fallbacks=%d spawns=%d", leaked, mgr.metrics.Fallbacks.Load(), mgr.metrics.Spawns.Load())
	}
}
