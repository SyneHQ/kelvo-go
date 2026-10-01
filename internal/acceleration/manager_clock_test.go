//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"go.yaml.in/yaml/v3"
)

func TestManagerObjectSchedulingUsesStorageClock(t *testing.T) {
	for _, offset := range []time.Duration{-4 * time.Hour, 4 * time.Hour} {
		t.Run(offset.String(), func(t *testing.T) {
			objects := newFakeSnapshotObjects()
			objects.clockOffset = offset
			backend := testObjectBackend(t, objects)
			config := catalog.Config{Sources: []catalog.Source{{ID: "mongo", Type: "mongodb", DSNEnv: "KELVO_SOURCE_FIXTURE_MONGO"}}, Acceleration: &backend.config}
			config.Acceleration.Datasets = []catalog.Dataset{{ID: "events", Query: query.Request{Mode: "native", ConnectionID: "mongo", SQL: "SELECT id FROM events"},
				RefreshInterval: time.Minute, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits()}}
			fixture := &refreshFixture{value: 7}
			manager := &Manager{config: config, store: backend, factory: func(catalog.Config, query.Limits) (query.Executor, error) { return fixture, nil }}
			initial, err := manager.Refresh(context.Background(), "events", false)
			if err != nil {
				t.Fatal(err)
			}
			assertNextObjectRefresh(t, initial, time.Minute)
			fresh, err := manager.Refresh(context.Background(), "events", true)
			if err != nil || fresh.Generation != initial.Generation || fixture.calls != 1 {
				t.Fatal("host clock repeated an early scheduled refresh")
			}
			assertNextObjectRefresh(t, fresh, time.Minute)
			objects.mutateManifest(t, backend.key("events", storeManifestName), func(manifest *objectManifest) {
				manifest.Committed.RefreshedAt = manifest.Committed.RefreshedAt.Add(-2 * time.Minute)
			})
			old, err := manager.StatusContext(context.Background(), "events")
			if err != nil || old.Age() < 2*time.Minute || time.Until(nextRefreshAt(old, time.Minute)) > time.Second {
				t.Fatal("old object snapshot was not immediately due on the host clock")
			}
			refreshed, err := manager.Refresh(context.Background(), "events", true)
			if err != nil || refreshed.Generation == initial.Generation || fixture.calls != 2 {
				t.Fatal("host clock suppressed a due scheduled refresh")
			}
			assertNextObjectRefresh(t, refreshed, time.Minute)
		})
	}
}

func assertNextObjectRefresh(t *testing.T, snapshot Snapshot, interval time.Duration) {
	t.Helper()
	if age := snapshot.Age(); age < 0 || age > time.Second {
		t.Fatal("new object snapshot has a clock-skewed age")
	}
	if delay := time.Until(nextRefreshAt(snapshot, interval)); delay < interval-time.Second || delay > interval {
		t.Fatal("object scheduler used a remote timestamp as a local deadline")
	}
}

func TestSnapshotAgeObservationIsMonotonicAndNeverSerialized(t *testing.T) {
	remoteNow := time.Now().Add(4 * time.Hour)
	snapshot := Snapshot{RefreshedAt: remoteNow.Add(-time.Minute)}.observeClock(remoteNow)
	// Simulate two seconds of elapsed local time without sleeping.
	snapshot.ageObservedAt = snapshot.ageObservedAt.Add(-2 * time.Second)
	if age := snapshot.Age(); age < 62*time.Second || age > 63*time.Second {
		t.Fatal("snapshot age did not advance with elapsed observation time")
	}
	for _, marshal := range []func(any) ([]byte, error){json.Marshal, yaml.Marshal} {
		data, err := marshal(snapshot)
		if err != nil || bytes.Contains(data, []byte("ageObserved")) || bytes.Contains(data, []byte("age_observed")) {
			t.Fatal("clock observations entered serialized snapshot metadata")
		}
	}
}
