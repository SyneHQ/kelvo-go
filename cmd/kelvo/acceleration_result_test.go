// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestAccelerationCommandWaitsForBothCleanupPathsBeforeOutput(t *testing.T) {
	managerStarted, runtimeStarted := make(chan struct{}), make(chan struct{})
	managerRelease, runtimeRelease := make(chan struct{}), make(chan struct{})
	var managerOnce, runtimeOnce sync.Once
	defer managerOnce.Do(func() { close(managerRelease) })
	defer runtimeOnce.Do(func() { close(runtimeRelease) })
	published := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- finishAccelerationCommand(context.Background(), nil, func() error {
			close(managerStarted)
			<-managerRelease
			return nil
		}, func(ctx context.Context) error {
			close(runtimeStarted)
			<-runtimeRelease
			return ctx.Err()
		}, func() error { close(published); return nil })
	}()
	wait := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("cleanup did not start")
		}
	}
	wait(managerStarted)
	select {
	case <-runtimeStarted:
		t.Fatal("runtime closed before its manager finished")
	case <-published:
		t.Fatal("output escaped pending manager cleanup")
	default:
	}
	managerOnce.Do(func() { close(managerRelease) })
	wait(runtimeStarted)
	select {
	case <-published:
		t.Fatal("output escaped pending runtime cleanup")
	default:
	}
	runtimeOnce.Do(func() { close(runtimeRelease) })
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("command did not finish after cleanup")
	}
	wait(published)
}

func TestAccelerationCommandRefusesOutputAfterOperationOrCleanupFailure(t *testing.T) {
	operationErr := errors.New("operation failed")
	privateErr := errors.New("fixture-private-provider-credential")
	for _, tc := range []struct {
		name                        string
		operation, manager, runtime error
		noManager                   bool
	}{
		{name: "operation", operation: operationErr},
		{name: "manager", manager: privateErr},
		{name: "runtime", runtime: privateErr},
		{name: "both", manager: privateErr, runtime: privateErr},
		{name: "partial-runtime-construction", operation: operationErr, runtime: privateErr, noManager: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output strings.Builder
			var managerCalled, runtimeCalled bool
			closeManager := func() error { managerCalled = true; return tc.manager }
			if tc.noManager {
				closeManager = nil
			}
			err := finishAccelerationCommand(context.Background(), tc.operation, closeManager, func(ctx context.Context) error {
				runtimeCalled = true
				deadline, ok := ctx.Deadline()
				if !ok || ctx.Err() != nil || time.Until(deadline) <= 0 || time.Until(deadline) > 10*time.Second {
					t.Fatal("runtime cleanup has no fresh bounded context")
				}
				return tc.runtime
			}, func() error { return encodeAccelerationResult(&output, map[string]bool{"verified": true}) })
			if err == nil || output.Len() != 0 || !runtimeCalled || managerCalled != !tc.noManager {
				t.Fatalf("uncertain command exposed output or skipped cleanup: %q", output.String())
			}
			if tc.operation != nil && !errors.Is(err, tc.operation) {
				t.Fatal("operation error was lost")
			}
			if strings.Contains(query.PublicError(err).Message, privateErr.Error()) {
				t.Fatal("cleanup diagnostic exposed provider details")
			}
		})
	}
}

type accelerationOutputFailure struct{ err error }

func (w accelerationOutputFailure) Write([]byte) (int, error) { return 0, w.err }

func TestAccelerationCommandPropagatesOutputFailureAfterCleanup(t *testing.T) {
	writeErr := errors.New("output disconnected")
	managerClosed, runtimeClosed := false, false
	err := finishAccelerationCommand(context.Background(), nil, func() error { managerClosed = true; return nil }, func(context.Context) error {
		runtimeClosed = true
		return nil
	}, func() error {
		if !managerClosed || !runtimeClosed {
			t.Fatal("output started before cleanup")
		}
		return encodeAccelerationResult(accelerationOutputFailure{writeErr}, map[string]bool{"verified": true})
	})
	if err == nil || !strings.Contains(err.Error(), writeErr.Error()) {
		t.Fatalf("output failure was lost: %v", err)
	}
}

func TestAccelerationCommandCancellationDuringCleanupWithholdsOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var runtimeClosed, published bool
	err := finishAccelerationCommand(ctx, nil, func() error {
		cancel()
		return nil
	}, func(cleanup context.Context) error {
		runtimeClosed = true
		if cleanup.Err() != nil {
			t.Fatal("request cancellation prevented runtime cleanup")
		}
		return nil
	}, func() error { published = true; return nil })
	if !errors.Is(err, context.Canceled) || !runtimeClosed || published {
		t.Fatalf("cancelled cleanup exposed success: %v", err)
	}
	if err := finishAccelerationCommand(ctx, nil, nil, nil, nil); err != nil {
		t.Fatal("normal watch shutdown must remain graceful")
	}
}
