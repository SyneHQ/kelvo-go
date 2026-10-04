//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestResolveRejectedObjectSnapshotsNeverMintCapabilities(t *testing.T) {
	for _, layout := range []string{"single", "multipart"} {
		for _, rejection := range []string{"fingerprint", "stale"} {
			t.Run(layout+"/"+rejection, func(t *testing.T) {
				backend, objects, _ := objectAcquisitionFixture(t, layout == "multipart")
				backend.config.Datasets = []catalog.Dataset{{ID: "events", MaxAge: time.Hour,
					Query:                query.Request{Mode: "native", ConnectionID: "origin", SQL: "SELECT id FROM events"},
					AuthorizationVersion: "v1", Limits: query.DefaultLimits()}}
				config := catalog.Config{Sources: []catalog.Source{{ID: "origin", Type: "clickhouse", URLEnv: "KELVO_SOURCE_ACQUISITION_FIXTURE"}}, Acceleration: &backend.config}
				fingerprint, err := config.DatasetFingerprint("events")
				if err != nil {
					t.Fatal(err)
				}
				objects.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) {
					m.Committed.Fingerprint = fingerprint
					if rejection == "fingerprint" {
						m.Committed.Fingerprint = "previous-configuration"
					} else {
						m.Committed.RefreshedAt = time.Now().Add(-2 * time.Hour).UTC()
					}
				})
				bridges := 0
				sources, versions, release, err := resolve(context.Background(), config, query.Request{Mode: "federated", Sources: []string{"events"}}, resolveResources{
					openBackend: func(catalog.AccelerationConfig) (Backend, error) { return backend, nil },
					openRanges: func(context.Context, catalog.ObjectStorage, []Snapshot) (map[string]catalog.Source, func(), error) {
						bridges++
						return nil, nil, errors.New("rejected snapshots must not mint capabilities")
					},
				})
				if release != nil {
					release()
				}
				if err == nil || sources != nil || versions != nil || bridges != 0 {
					t.Fatal("rejected query exposed snapshot capabilities", sources, versions, bridges, err)
				}
				requireAcquisitionManifestOnly(t, backend, objects, 1)
				objects.mu.Lock()
				closed := objects.closed
				objects.mu.Unlock()
				if closed != 1 {
					t.Fatal("Resolve did not close rejected snapshot backend", closed)
				}
			})
		}
	}
}
