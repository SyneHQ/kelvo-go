// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func onMissFixture(t *testing.T, optIn bool) (*OnMiss, *Manager, *refreshFixture, *atomic.Int32, query.Request) {
	c, m, f := managerFixture(t)
	if optIn {
		c.Acceleration.Datasets[0].OnUnavailable = "refresh"
		m.config = c
	}
	var calls atomic.Int32
	o := &OnMiss{Status: m.StatusContext, Refresh: func(ctx context.Context, id string) error {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		_, err := m.Refresh(ctx, id, true)
		return err
	}}
	return o, m, f, &calls, query.Request{Mode: "federated", SQL: "SELECT 1", Sources: []string{"orders_fast"}}
}

func requireUnavailable(t *testing.T, err error) {
	t.Helper()
	if err == nil || query.PublicError(err).Code != "DATASET_UNAVAILABLE" {
		t.Fatalf("want DATASET_UNAVAILABLE, got %v", err)
	}
}

func TestOnMissRefreshesOnceAndServes(t *testing.T) {
	o, m, f, calls, req := onMissFixture(t, true)
	got, err := o.Ensure(context.Background(), m.config, req)
	if err != nil || !got["orders_fast"] || f.calls != 1 {
		t.Fatalf("refreshed=%v calls=%d err=%v", got, f.calls, err)
	}
	sources, versions, release, err := Resolve(context.Background(), m.config, req)
	if err != nil || len(sources) != 1 || len(versions) != 1 {
		t.Fatalf("resolve: %v", err)
	}
	release()
	// Fresh now: a second query does not refresh again.
	if got, err = o.Ensure(context.Background(), m.config, req); err != nil || got != nil || calls.Load() != 1 {
		t.Fatalf("refreshed=%v calls=%d err=%v", got, calls.Load(), err)
	}
}

func TestOnMissConcurrentQueriesShareOneRefresh(t *testing.T) {
	o, m, f, calls, req := onMissFixture(t, true)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := o.Ensure(context.Background(), m.config, req); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 || f.calls != 1 {
		t.Fatalf("refresh calls=%d executions=%d", calls.Load(), f.calls)
	}
}

func TestOnMissWithoutOptInStaysUnavailable(t *testing.T) {
	o, m, f, _, req := onMissFixture(t, false)
	if got, err := o.Ensure(context.Background(), m.config, req); err != nil || got != nil || f.calls != 0 {
		t.Fatalf("refreshed=%v calls=%d err=%v", got, f.calls, err)
	}
	_, _, _, err := Resolve(context.Background(), m.config, req)
	requireUnavailable(t, err)
}

func TestOnMissIncompatibleSnapshotStillFails(t *testing.T) {
	o, m, f, calls, req := onMissFixture(t, true)
	if _, err := m.Refresh(context.Background(), "orders_fast", false); err != nil {
		t.Fatal(err)
	}
	m.config.Acceleration.Datasets[0].AuthorizationVersion = "v2" // fingerprint changes
	if got, err := o.Ensure(context.Background(), m.config, req); err != nil || got != nil || calls.Load() != 0 {
		t.Fatalf("refreshed=%v calls=%d err=%v", got, calls.Load(), err)
	}
	_, _, _, err := Resolve(context.Background(), m.config, req)
	requireUnavailable(t, err)
	if f.calls != 1 {
		t.Fatalf("executions=%d", f.calls)
	}
}

func TestOnMissExpiredRefreshes(t *testing.T) {
	o, m, _, calls, req := onMissFixture(t, true)
	fingerprint, _ := m.config.DatasetFingerprint("orders_fast")
	o.Status = func(context.Context, string) (Snapshot, error) {
		return Snapshot{Fingerprint: fingerprint, RefreshedAt: time.Now().Add(-2 * time.Hour)}, nil
	}
	if got, err := o.Ensure(context.Background(), m.config, req); err != nil || !got["orders_fast"] || calls.Load() != 1 {
		t.Fatalf("refreshed=%v calls=%d err=%v", got, calls.Load(), err)
	}
}

func TestOnMissRefreshFailureIsUnavailable(t *testing.T) {
	o, m, f, _, req := onMissFixture(t, true)
	f.fail = true
	got, err := o.Ensure(context.Background(), m.config, req)
	requireUnavailable(t, err)
	if got != nil {
		t.Fatal("failed refresh reported as refreshed")
	}
}
