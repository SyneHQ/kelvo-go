// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestObjectSnapshotProvenanceCopiesAcquiredPartsAndScanLimits(t *testing.T) {
	for _, count := range []int{0, 2} {
		t.Run(fmt.Sprintf("parts-%d", count), func(t *testing.T) {
			storage, snapshot := rangeFixture()
			snapshot.SchemaHash, snapshot.Rows = strings.Repeat("e", 64), 10
			var client objectstore.RangeClient = &rangeFixtureClient{snapshot: snapshot}
			if count > 0 {
				storage, snapshot, client = multipartRangeFixture(count)
			}
			sources, release, err := openObjectRanges(context.Background(), storage, []Snapshot{snapshot}, client)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			scan := catalog.SnapshotScanLimits{MaxRows: 5_000_000, MaxBytes: 1 << 30}
			dataset := catalog.Dataset{ID: snapshot.Dataset, Scan: &scan, Limits: query.Limits{MaxRows: 10, MaxBytes: 1024}}
			minted := sources[snapshot.Dataset]
			source, err := withObjectSnapshotProvenance(minted, snapshot, dataset)
			if err != nil || source.ValidateObjectSnapshot() != nil {
				t.Fatal("acquired provenance rejected", err)
			}
			read := source.ObjectSnapshot
			if read.Dataset != snapshot.Dataset || read.Generation != snapshot.Generation || read.SchemaSHA256 != snapshot.SchemaHash || read.Scan != scan {
				t.Fatal("generation identity or independent raw budgets changed")
			}
			httpClient := rangeHTTPClient(t)
			for _, part := range read.Parts {
				response, data, err := rangeRequest(t, httpClient, "GET", part.URL, "bytes=0-31")
				if err != nil || response.StatusCode != 206 || len(data) != 32 || response.Header.Get("ETag") != `"`+part.SHA256+`"` {
					t.Fatal("part no longer matches parent-pinned capability", err)
				}
			}
			raw, err := json.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range []string{storage.Endpoint, storage.Prefix, "KELVO_SOURCE_", "ObjectVersion", "selected-version", "version-0", snapshot.ObjectKey} {
				if forbidden != "" && strings.Contains(string(raw), forbidden) {
					t.Fatal("provider configuration escaped into worker envelope")
				}
			}
			dataset.Scan.MaxRows = 1
			if count > 0 {
				snapshot.Parts[0].Rows, snapshot.Parts[0].Bytes, snapshot.Parts[0].SHA256 = 99, 12, strings.Repeat("f", 64)
				minted.Ranges[0].URL = "changed"
			} else {
				minted.Range.URL = "changed"
			}
			if read.Scan.MaxRows != 5_000_000 || source.ValidateObjectSnapshot() != nil {
				t.Fatal("caller mutation changed trusted descriptor")
			}
			if after, err := json.Marshal(source); err != nil || string(after) != string(raw) {
				t.Fatal("descriptor retained mutable caller metadata", err)
			}
		})
	}
}

func TestObjectSnapshotProvenanceRejectsMismatchAndUsesExplicitDefaults(t *testing.T) {
	storage, snapshot, client := multipartRangeFixture(2)
	sources, release, err := openObjectRanges(context.Background(), storage, []Snapshot{snapshot}, client)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	source := sources[snapshot.Dataset]
	dataset := catalog.Dataset{ID: snapshot.Dataset}
	resolved, err := withObjectSnapshotProvenance(source, snapshot, dataset)
	defaults, _ := (catalog.SnapshotScanLimits{}).Effective()
	if err != nil || resolved.ObjectSnapshot.Scan != defaults {
		t.Fatal("default raw limits not made explicit", err)
	}
	for _, mode := range []string{"dataset", "generation", "rows", "bytes", "parts", "capability-size", "schema", "scan", "prebound"} {
		t.Run(mode, func(t *testing.T) {
			copySnapshot := snapshot
			copySnapshot.Parts = append([]SnapshotPart(nil), snapshot.Parts...)
			copySource := source
			copySource.Ranges = append([]catalog.ObjectRange(nil), source.Ranges...)
			copyDataset := dataset
			switch mode {
			case "dataset":
				copyDataset.ID = "other"
			case "generation":
				copySnapshot.Generation = "invalid"
			case "rows":
				copySnapshot.Rows++
			case "bytes":
				copySnapshot.Bytes++
			case "parts":
				copySnapshot.Parts = copySnapshot.Parts[:1]
			case "capability-size":
				copySource.Ranges[0].Bytes++
			case "schema":
				copySnapshot.SchemaHash = ""
			case "scan":
				copyDataset.Scan = &catalog.SnapshotScanLimits{MaxRows: -1}
			case "prebound":
				copySource.ObjectSnapshot = resolved.ObjectSnapshot
			}
			if _, err := withObjectSnapshotProvenance(copySource, copySnapshot, copyDataset); err == nil {
				t.Fatal("confused acquired provenance accepted")
			}
		})
	}
}

type snapshotResolveBackend struct {
	Backend
	snapshot    Snapshot
	fingerprint string
	events      *[]string
}

func (backend *snapshotResolveBackend) Acquire(_ context.Context, dataset, fingerprint string, _ time.Duration) (*Lease, error) {
	if dataset != backend.snapshot.Dataset || fingerprint != backend.fingerprint {
		return nil, errors.New("unexpected dataset acquisition")
	}
	*backend.events = append(*backend.events, "acquire")
	return &Lease{Snapshot: backend.snapshot, release: func() error {
		*backend.events = append(*backend.events, "lease-close")
		return nil
	}}, nil
}

func (backend *snapshotResolveBackend) Close() error {
	*backend.events = append(*backend.events, "backend-close")
	return nil
}

func TestResolveObjectSnapshotRetainsBridgeAndLeaseUntilRelease(t *testing.T) {
	for _, mode := range []string{"single", "multipart", "missing-capability", "changed-size", "invalid-scan", "bridge-failure"} {
		t.Run(mode, func(t *testing.T) {
			storage, snapshot := rangeFixture()
			snapshot.SchemaHash, snapshot.Rows = strings.Repeat("c", 64), 12
			var client objectstore.RangeClient = &rangeFixtureClient{snapshot: snapshot}
			if mode != "single" {
				storage, snapshot, client = multipartRangeFixture(2)
			}
			scan := catalog.SnapshotScanLimits{MaxRows: 5_000_000, MaxBytes: 1 << 30}
			if mode == "invalid-scan" {
				scan.MaxRows = -1
			}
			config := catalog.Config{
				Sources: []catalog.Source{{ID: "origin", Type: "clickhouse", URLEnv: "KELVO_SOURCE_ORIGIN_URL"}},
				Acceleration: &catalog.AccelerationConfig{TenantID: "tenant-a", ObjectStorage: &storage, Datasets: []catalog.Dataset{{
					ID: snapshot.Dataset, Scan: &scan, Query: query.Request{Mode: "native", ConnectionID: "origin", SQL: "SELECT id FROM events"},
				}}},
			}
			fingerprint, err := config.DatasetFingerprint(snapshot.Dataset)
			if err != nil {
				t.Fatal(err)
			}
			var events []string
			backend := &snapshotResolveBackend{snapshot: snapshot, fingerprint: fingerprint, events: &events}
			var target string
			var actualBridgeClose func()
			resources := resolveResources{
				openBackend: func(catalog.AccelerationConfig) (Backend, error) {
					events = append(events, "backend-open")
					return backend, nil
				},
				openRanges: func(ctx context.Context, location catalog.ObjectStorage, acquired []Snapshot) (map[string]catalog.Source, func(), error) {
					if len(acquired) != 1 || acquired[0].Generation != snapshot.Generation {
						t.Fatal("range bridge lost acquired generation")
					}
					sources, closeBridge, err := openObjectRanges(ctx, location, acquired, client)
					if err != nil {
						return nil, closeBridge, err
					}
					actualBridgeClose = closeBridge
					events = append(events, "bridge-open")
					source := sources[snapshot.Dataset]
					if source.Range != nil {
						target = source.Range.URL
					} else {
						target = source.Ranges[0].URL
					}
					if mode == "missing-capability" {
						delete(sources, snapshot.Dataset)
					} else if mode == "changed-size" {
						source.Ranges[0].Bytes++
					}
					release := func() { closeBridge(); events = append(events, "bridge-close") }
					if mode == "bridge-failure" {
						return nil, release, errors.New("injected post-setup failure")
					}
					return sources, release, nil
				},
			}
			sources, versions, release, err := resolve(context.Background(), config, query.Request{Mode: "federated", Sources: []string{snapshot.Dataset}}, resources)
			if actualBridgeClose != nil {
				defer actualBridgeClose()
			}
			if mode == "single" || mode == "multipart" {
				if err != nil || len(sources) != 1 || len(versions) != 1 || versions[0].Generation != snapshot.Generation || sources[0].ValidateObjectSnapshot() != nil {
					t.Fatal("resolved source lost acquired provenance", err)
				}
				if !reflect.DeepEqual(events, []string{"backend-open", "acquire", "bridge-open"}) {
					t.Fatal("generation lease closed before caller release", events)
				}
				response, data, rangeErr := rangeRequest(t, rangeHTTPClient(t), "GET", target, "bytes=0-31")
				if rangeErr != nil || response.StatusCode != 206 || len(data) != 32 {
					t.Fatal("query lost its live range capability", rangeErr)
				}
				release()
			} else if err == nil || sources != nil || versions != nil {
				t.Fatal("resolution accepted invalid object provenance")
			}
			if !reflect.DeepEqual(events, []string{"backend-open", "acquire", "bridge-open", "bridge-close", "lease-close", "backend-close"}) {
				t.Fatal("bridge was not drained before generation/backend release", events)
			}
			if _, _, err := rangeRequest(t, rangeHTTPClient(t), "GET", target, "bytes=0-31"); err == nil {
				t.Fatal("released generation retained a live capability")
			}
		})
	}
}
