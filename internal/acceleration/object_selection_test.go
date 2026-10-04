//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

func protectedGenerationEvent(event protectedObjectEvent) bool {
	return (event.operation == "get" || event.operation == "head" || event.operation == "range") &&
		(strings.HasSuffix(event.key, ".parquet") || strings.HasSuffix(event.key, ".parts.yaml"))
}

func requirePinBeforeGeneration(t *testing.T, events []protectedObjectEvent) {
	t.Helper()
	pinned, accessed := false, false
	for _, event := range events {
		if event.operation == "pin" {
			pinned = true
		}
		if protectedGenerationEvent(event) {
			accessed = true
			if !pinned {
				t.Fatalf("generation access preceded durable pin: %+v", event)
			}
		}
	}
	if !pinned || !accessed {
		t.Fatalf("fixture did not exercise pin and generation I/O: %+v", events)
	}
}

func closeProtectedTestGuard(t *testing.T, guard *ReadGuard) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := guard.Close(ctx); err != nil {
		t.Error("protected guard cleanup", err)
	}
}

func TestProtectedSelectionPinsBeforeDescriptorAndKeepsOneRoot(t *testing.T) {
	runtime, service, config := protectedPublicationFixture(t)
	backend, ctx := runtime.backend, context.Background()
	fp := protectedFingerprint(t, config)
	writer := beginRemoteMultipart(t, backend)
	if err := writeRemoteMultipartPart(t, writer, "id", 3, 3); err != nil {
		t.Fatal(err)
	}
	first, err := writer.Commit(fp)
	if err != nil {
		t.Fatal(err)
	}
	service.takeEvents()
	op, err := runtime.beginOperation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer op.Close()
	selected, err := backend.selectObject(op.Context(), "events", fp, time.Hour, true)
	if err != nil {
		t.Fatal(err)
	}
	events := service.takeEvents()
	if len(events) != 1 || events[0].operation != "get" || !strings.HasSuffix(events[0].key, storeManifestName) {
		t.Fatalf("selection performed generation or registry I/O: %+v", events)
	}
	_ = commitRemoteRecovery(t, backend, "id", fp)
	service.takeEvents()
	guard, err := runtime.acquire(op.Context(), []readerlease.Binding{selected.binding})
	if guard != nil {
		defer closeProtectedTestGuard(t, guard)
	}
	if err != nil {
		t.Fatal(err)
	}
	done, err := guard.HoldConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	loaded, err := backend.loadSelectedObject(guard.Context(), selected)
	if err != nil || loaded.Generation != first.Generation || len(loaded.Parts) != 1 {
		t.Fatalf("selection changed across publication: %+v %v", loaded, err)
	}
	if err := guard.Check(); err != nil {
		t.Fatal(err)
	}
	events = service.takeEvents()
	requirePinBeforeGeneration(t, events)
	for _, event := range events {
		if event.operation == "get" && strings.HasSuffix(event.key, storeManifestName) {
			t.Fatal("load reselected current root")
		}
	}
}

func TestProtectedSelectionRejectsPolicyAndLegacyBeforePinOrData(t *testing.T) {
	for _, mode := range []string{"fingerprint", "stale", "binding", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedPublicationFixture(t)
			backend := runtime.backend
			fp := protectedFingerprint(t, config)
			commitRemoteRecovery(t, backend, "id", fp)
			want := ErrFingerprintMismatch
			switch mode {
			case "fingerprint":
				fp = strings.Repeat("f", 64)
			case "stale":
				want = ErrStale
				service.mutateManifest(t, backend.key("events", storeManifestName), func(root *objectManifest) { root.Committed.RefreshedAt = time.Now().Add(-2 * time.Hour).UTC() })
			case "binding":
				want = ErrCorrupt
				service.mutateManifest(t, backend.key("events", storeManifestName), func(root *objectManifest) { root.Committed.ReaderBinding.ContentSHA256 = strings.Repeat("0", 64) })
			case "legacy":
				want = ErrProtectionRequired
				service.mutateManifest(t, backend.key("events", storeManifestName), func(root *objectManifest) { root.Version = 4; root.Committed.ReaderBinding = nil })
			}
			service.takeEvents()
			op, err := runtime.beginOperation(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer op.Close()
			_, err = backend.selectObject(op.Context(), "events", fp, time.Hour, true)
			if !errors.Is(err, want) {
				t.Fatalf("wanted %v, got %v", want, err)
			}
			for _, event := range service.takeEvents() {
				if event.operation != "get" || !strings.HasSuffix(event.key, storeManifestName) {
					t.Fatalf("rejected selection touched registry or generation: %+v", event)
				}
			}
		})
	}
}

func TestProtectedMaintenanceUsesPinsOrRefusesBeforeGenerationIO(t *testing.T) {
	runtime, service, config := protectedPublicationFixture(t)
	backend, ctx := runtime.backend, context.Background()
	fp := protectedFingerprint(t, config)
	snapshot := commitRemoteRecovery(t, backend, "id", fp)
	service.takeEvents()
	current, err := backend.Status(ctx, "events")
	if err != nil || current.Generation != snapshot.Generation {
		t.Fatal("protected status", err)
	}
	requirePinBeforeGeneration(t, service.takeEvents())
	writer, err := backend.Begin(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Abort()
	service.takeEvents()
	schema, err := writer.(SchemaWriter).PreviousSchema()
	if err != nil || schema.NumFields() != 1 || schema.Field(0).Name != "id" {
		t.Fatal("protected previous schema", err)
	}
	requirePinBeforeGeneration(t, service.takeEvents())
	if err := writer.Abort(); err != nil {
		t.Fatal(err)
	}
	service.takeEvents()
	for _, call := range []func() error{
		func() error { _, err := backend.Verify(ctx, "events"); return err },
		func() error { _, err := backend.Inventory(ctx, "events"); return err },
		func() error {
			_, err := backend.Restore(ctx, RestoreRequest{Dataset: "events", Generation: snapshot.Generation, ExpectedGeneration: snapshot.Generation, Fingerprint: fp})
			return err
		},
		func() error {
			_, err := backend.MigrateBackup(ctx, MigrationRequest{Dataset: "events", SourceFingerprint: fp, TargetFingerprint: strings.Repeat("f", 64), Destination: filepath.Join(t.TempDir(), "unused"), MaxRows: 10, MaxBytes: 1 << 20})
			return err
		},
	} {
		if err := call(); !errors.Is(err, ErrRecoveryUnsupported) {
			t.Fatalf("maintenance did not refuse: %v", err)
		}
	}
	if _, err := backend.Acquire(ctx, "events", fp, time.Hour); !errors.Is(err, ErrProtectionRequired) {
		t.Fatal("legacy acquisition admitted protected generation", err)
	}
	if events := service.takeEvents(); len(events) != 0 {
		t.Fatalf("unsupported maintenance performed provider I/O: %+v", events)
	}
}

func TestProtectedDescriptorDelayPreservesSelectedFreshness(t *testing.T) {
	for _, mode := range []string{"stale", "status", "unlimited"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedPublicationFixture(t)
			backend := runtime.backend
			writer := beginRemoteMultipart(t, backend)
			if err := writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
				t.Fatal(err)
			}
			fp := protectedFingerprint(t, config)
			committed, err := writer.Commit(fp)
			if err != nil {
				t.Fatal(err)
			}
			// A selected service-clock observation behind the immutable timestamp
			// starts at zero age. Descriptor time must still consume that age budget.
			service.infoHook = func(operation, key string, info objectstore.Info) objectstore.Info {
				if operation == "get" && strings.HasSuffix(key, storeManifestName) {
					info.ServerTime = committed.RefreshedAt.Add(-time.Hour)
				}
				return info
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var entered time.Time
			var waited time.Duration
			service.beforeGet = func(key string) {
				if key == backend.key("events", objectDescriptorName(committed.Generation)) {
					entered, waited = delayAcquisitionRead(ctx)
				}
			}
			service.takeEvents()
			maxAge := time.Duration(0)
			if mode == "stale" {
				maxAge = acquisitionReadDelay / 3
			}
			snapshot, err := backend.protectedCurrentSnapshot(ctx, "events", fp, maxAge, mode != "status")
			if entered.IsZero() || waited < acquisitionReadDelay {
				t.Fatalf("descriptor delay was not exercised: %v %v", waited, err)
			}
			events := service.takeEvents()
			requirePinBeforeGeneration(t, events)
			if mode == "stale" {
				if !errors.Is(err, ErrStale) || snapshot.Generation != "" {
					t.Fatalf("descriptor time reset freshness: %+v %v", snapshot, err)
				}
				for _, event := range events {
					if event.operation == "head" || event.operation == "range" {
						t.Fatal("expired descriptor triggered payload access", event)
					}
				}
				return
			}
			if err != nil || snapshot.Generation != committed.Generation {
				t.Fatalf("protected descriptor read: %+v %v", snapshot, err)
			}
			if snapshot.ageObserved != 0 || snapshot.ageObservedAt.After(entered) || snapshot.Age() < waited {
				t.Fatal("protected snapshot lost its selected monotonic clock", snapshot.Age(), waited)
			}
		})
	}
}

func TestProtectedMetadataClosesPartialBodiesAndRefusesCloseErrors(t *testing.T) {
	for _, operation := range []string{"root", "descriptor", "schema"} {
		for _, fault := range []string{"close", "partial", "partial_not_found_close"} {
			t.Run(operation+"/"+fault, func(t *testing.T) {
				runtime, service, config := protectedPublicationFixture(t)
				backend := runtime.backend
				writer := beginRemoteMultipart(t, backend)
				if err := writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
					t.Fatal(err)
				}
				committed, err := writer.Commit(protectedFingerprint(t, config))
				if err != nil {
					t.Fatal(err)
				}
				var schemaWriter SchemaWriter
				if operation == "schema" {
					refresh, err := backend.Begin(context.Background(), "events")
					if err != nil {
						t.Fatal(err)
					}
					defer refresh.Abort()
					schemaWriter = refresh.(SchemaWriter)
				}
				var bodies []*protectedFaultBody
				service.bodyHook = func(method, key string, body io.ReadCloser, resultErr error) (io.ReadCloser, error) {
					matches := operation == "root" && method == "get" && strings.HasSuffix(key, storeManifestName) ||
						operation == "descriptor" && method == "get" && key == backend.key("events", objectDescriptorName(committed.Generation)) ||
						operation == "schema" && method == "range"
					if !matches || body == nil {
						return body, resultErr
					}
					broken := &protectedFaultBody{ReadCloser: body}
					bodies = append(bodies, broken)
					if fault != "partial" {
						broken.closeErr = errObjectNetwork
					}
					if fault == "partial" {
						resultErr = errObjectNetwork
					} else if fault == "partial_not_found_close" {
						resultErr = objectstore.ErrNotFound
					}
					return broken, resultErr
				}
				service.takeEvents()
				if operation == "schema" {
					schema, readErr := schemaWriter.PreviousSchema()
					err = readErr
					if schema != nil {
						t.Fatal("failed schema body returned a schema")
					}
				} else {
					snapshot, readErr := backend.Status(context.Background(), "events")
					err = readErr
					if snapshot.Generation != "" {
						t.Fatal("failed metadata body returned a snapshot")
					}
				}
				if err == nil || len(bodies) == 0 {
					t.Fatalf("failed provider body reported success: bodies=%d error=%v", len(bodies), err)
				}
				if operation != "schema" && fault != "partial" && !errors.Is(err, errReaderCleanupUnknown) {
					t.Fatalf("body cleanup failure was downgraded: %v", err)
				}
				for _, body := range bodies {
					if body.closed.Load() != 1 {
						t.Fatal("partial/error response body not closed exactly once", body.closed.Load())
					}
				}
				if operation != "root" {
					requirePinBeforeGeneration(t, service.takeEvents())
				}
			})
		}
	}
}
