//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
	"go.yaml.in/yaml/v3"
)

func openVerificationFixture(t *testing.T, config catalog.Config, service *protectedObjectService) *ObjectRuntime {
	t.Helper()
	runtime, err := openObjectRuntime(config, func(catalog.ObjectLocation, catalog.ObjectCredentials, *objectstore.SharedTransport) (objectstore.Client, error) {
		return &protectedObjectClient{service}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error("verification fixture cleanup", err)
		}
	})
	return runtime
}

func protectedVerificationFixture(t *testing.T) (*ObjectRuntime, *protectedObjectService, catalog.Config) {
	t.Helper()
	config := objectRuntimeTestConfig(t)
	config.Acceleration.Datasets[0].Verification = &catalog.VerificationLimits{MaxBytes: 32 << 20}
	service := &protectedObjectService{fakeSnapshotObjects: newFakeSnapshotObjects()}
	return openVerificationFixture(t, config, service), service, config
}

func commitVerificationMultipart(t *testing.T, backend *objectBackend, fp string) Snapshot {
	t.Helper()
	writer := beginRemoteMultipart(t, backend)
	for range 2 {
		if err := writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := writer.Commit(fp)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func verificationRuntimeOperations(runtime *ObjectRuntime) int {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.operations
}

func TestProtectedVerificationOneRootAllPinsAndSingleDescriptorLoad(t *testing.T) {
	runtime, service, config := protectedVerificationFixture(t)
	backend, fp := runtime.backend, protectedFingerprint(t, config)
	var replace atomic.Bool
	service.beforeGet = func(key string) {
		if strings.HasSuffix(key, ".parts.yaml") && replace.CompareAndSwap(true, false) {
			// Model a concurrent valid root replacement after the captured root
			// has been selected. Its payloads and sealed registries stay intact.
			service.mutateManifest(t, backend.key("events", storeManifestName), func(root *objectManifest) {
				root.Committed, root.History[1] = root.History[1], root.Committed
			})
		}
	}
	first := commitRemoteRecovery(t, backend, "id", fp)
	second := commitVerificationMultipart(t, backend, fp)
	third := commitVerificationMultipart(t, backend, fp)
	service.takeEvents()
	replace.Store(true)
	entries, err := backend.Inventory(context.Background(), "events")
	if err != nil || len(entries) != 3 || replace.Load() {
		t.Fatalf("complete captured inventory: entries=%d error=%v", len(entries), err)
	}
	for index, want := range []Snapshot{third, second, first} {
		got := entries[index]
		if !got.Verified || got.Active != (index == 0) || got.Snapshot.Generation != want.Generation || got.Snapshot.Rows != want.Rows || got.CatalogScope != "retained_manifest" {
			t.Fatalf("entry %d: %+v", index, got)
		}
		if got.Snapshot.ageObservedAt != entries[0].Snapshot.ageObservedAt {
			t.Fatal("inventory entries do not share the captured monotonic anchor")
		}
	}
	pins, roots := 0, 0
	descriptors := map[string]int{}
	for _, event := range service.takeEvents() {
		if event.operation == "pin" {
			pins++
		}
		if event.operation == "get" && strings.HasSuffix(event.key, storeManifestName) {
			roots++
		}
		if protectedGenerationEvent(event) && pins != 3 {
			t.Fatal("generation I/O preceded the complete pin set", event, pins)
		}
		if event.operation == "get" && strings.HasSuffix(event.key, ".parts.yaml") {
			descriptors[event.key]++
		}
	}
	if roots != 1 || pins != 3 || len(descriptors) != 2 {
		t.Fatal("unexpected selection or loading counts", roots, pins, descriptors)
	}
	for _, count := range descriptors {
		if count != 1 {
			t.Fatal("descriptor loaded more than once", descriptors)
		}
	}
	if verificationRuntimeOperations(runtime) != 0 {
		t.Fatal("successful inventory retained its operation")
	}
}

func TestProtectedVerificationVerifiesSingleAndMultipart(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		t.Run(fmt.Sprint(multipart), func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			var committed Snapshot
			if multipart {
				committed = commitVerificationMultipart(t, runtime.backend, protectedFingerprint(t, config))
			} else {
				committed = commitRemoteRecovery(t, runtime.backend, "id", protectedFingerprint(t, config))
			}
			service.takeEvents()
			got, err := runtime.backend.Verify(context.Background(), "events")
			if err != nil || got.Generation != committed.Generation || got.Rows != committed.Rows || len(got.Parts) != len(committed.Parts) {
				t.Fatalf("protected verify: %+v %v", got, err)
			}
			requirePinBeforeGeneration(t, service.takeEvents())
		})
	}
}

func TestProtectedVerificationRejectsInvalidHistoricalBindingBeforePins(t *testing.T) {
	runtime, service, config := protectedVerificationFixture(t)
	for range 2 {
		commitRemoteRecovery(t, runtime.backend, "id", protectedFingerprint(t, config))
	}
	service.mutateManifest(t, runtime.backend.key("events", storeManifestName), func(root *objectManifest) {
		root.History[0].ReaderBinding.ContentSHA256 = strings.Repeat("0", 64)
	})
	service.takeEvents()
	entries, err := runtime.backend.Inventory(context.Background(), "events")
	if err == nil || entries != nil {
		t.Fatal("invalid historical binding admitted", entries, err)
	}
	for _, event := range service.takeEvents() {
		if event.operation != "get" || !strings.HasSuffix(event.key, storeManifestName) {
			t.Fatal("invalid binding reached registry or generation I/O", event)
		}
	}
}

func TestProtectedVerificationBudgetCoversHistoryDescriptorsAndFooters(t *testing.T) {
	for _, mode := range []string{"history_preflight", "descriptor_preflight", "footer"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			fp := protectedFingerprint(t, config)
			if mode == "history_preflight" {
				commitVerificationMultipart(t, runtime.backend, fp)
			}
			commitVerificationMultipart(t, runtime.backend, fp)
			state, err := runtime.backend.readState(context.Background(), "events", runtime.backend.reader)
			if err != nil {
				t.Fatal(err)
			}
			limit := state.info.Size + state.manifest.Committed.Bytes
			if mode != "descriptor_preflight" {
				limit += descriptorBytes(state.manifest.Committed) + 1
			}
			config.Acceleration.Datasets[0].Verification = &catalog.VerificationLimits{MaxBytes: limit}
			limited := openVerificationFixture(t, config, service)
			service.takeEvents()
			entries, err := limited.backend.Inventory(context.Background(), "events")
			if !errors.Is(err, errVerificationLimit) || entries != nil {
				t.Fatalf("budget failure became an inventory entry: %+v %v", entries, err)
			}
			generationReads := 0
			for _, event := range service.takeEvents() {
				if protectedGenerationEvent(event) {
					generationReads++
				}
			}
			if mode == "footer" && generationReads == 0 || mode != "footer" && generationReads != 0 {
				t.Fatal("budget refused at the wrong boundary", mode, generationReads)
			}
		})
	}
}

func TestProtectedVerificationInventoryCorruptionHasNoCapabilities(t *testing.T) {
	for _, fault := range []string{"descriptor", "payload", "metadata"} {
		t.Run(fault, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			var armed atomic.Bool
			service.infoHook = func(method, key string, info objectstore.Info) objectstore.Info {
				if fault == "metadata" && armed.Load() && method == "get" && strings.HasSuffix(key, ".parquet") {
					info.Size++
				}
				return info
			}
			first := commitRemoteRecovery(t, runtime.backend, "id", protectedFingerprint(t, config))
			current := commitVerificationMultipart(t, runtime.backend, protectedFingerprint(t, config))
			key := current.Parts[0].ObjectKey
			if fault == "descriptor" {
				key = runtime.backend.key("events", objectDescriptorName(current.Generation))
			}
			if fault != "metadata" {
				service.mu.Lock()
				object := service.objects[key]
				object.data[0] ^= 1
				service.objects[key] = object
				service.mu.Unlock()
			}
			armed.Store(true)
			entries, err := runtime.backend.Inventory(context.Background(), "events")
			if err != nil || len(entries) != 2 || entries[0].Verified || entries[0].Snapshot.Generation != current.Generation {
				t.Fatalf("pure corruption not retained: %+v %v", entries, err)
			}
			bad := entries[0].Snapshot
			if bad.Path != "" || bad.ObjectKey != "" || bad.ObjectVersion != "" || len(bad.Parts) != 0 || bad.Bytes != current.Bytes || bad.Rows != current.Rows {
				t.Fatalf("corruption returned capabilities or lost root metadata: %+v", bad)
			}
			if fault != "metadata" && (!entries[1].Verified || entries[1].Snapshot.Generation != first.Generation) {
				t.Fatal("independent retained generation not verified")
			}
			snapshot, err := runtime.backend.Verify(context.Background(), "events")
			if !errors.Is(err, ErrCorrupt) || snapshot.Generation != "" {
				t.Fatal("Verify accepted content corruption", snapshot, err)
			}
		})
	}
}

type verificationReadFaultBody struct {
	*protectedFaultBody
	err error
}

func (b *verificationReadFaultBody) Read(p []byte) (int, error) {
	n, _ := b.ReadCloser.Read(p)
	return n, b.err
}

func TestProtectedVerificationOperationalErrorsInvalidateInventory(t *testing.T) {
	for _, phase := range []string{"root", "descriptor", "checksum", "footer"} {
		for _, fault := range []string{"missing", "partial", "nil", "data_error", "close", "corrupt_and_close"} {
			t.Run(phase+"/"+fault, func(t *testing.T) {
				runtime, service, config := protectedVerificationFixture(t)
				var armed atomic.Bool
				var bodies []*protectedFaultBody
				service.bodyHook = func(method, key string, body io.ReadCloser, err error) (io.ReadCloser, error) {
					match := phase == "root" && method == "get" && strings.HasSuffix(key, storeManifestName) ||
						phase == "descriptor" && method == "get" && strings.HasSuffix(key, ".parts.yaml") ||
						phase == "checksum" && method == "get" && strings.HasSuffix(key, ".parquet") ||
						phase == "footer" && method == "range"
					if !armed.Load() || !match || body == nil || err != nil {
						return body, err
					}
					if fault == "missing" || fault == "nil" {
						_ = body.Close()
						if fault == "missing" {
							return nil, objectstore.ErrNotFound
						}
						return nil, nil
					}
					tracked := &protectedFaultBody{ReadCloser: body}
					bodies = append(bodies, tracked)
					switch fault {
					case "partial":
						return tracked, errObjectNetwork
					case "data_error":
						return &verificationReadFaultBody{tracked, errObjectNetwork}, nil
					case "close", "corrupt_and_close":
						tracked.closeErr = errObjectNetwork
						if fault == "corrupt_and_close" {
							return &verificationReadFaultBody{tracked, ErrCorrupt}, nil
						}
					}
					return tracked, nil
				}
				commitVerificationMultipart(t, runtime.backend, protectedFingerprint(t, config))
				armed.Store(true)
				entries, err := runtime.backend.Inventory(context.Background(), "events")
				if err == nil || entries != nil {
					t.Fatalf("operational failure reported an inventory: %+v %v", entries, err)
				}
				if strings.Contains(fault, "close") && !errors.Is(err, errReaderCleanupUnknown) {
					t.Fatal("cleanup uncertainty was downgraded", err)
				}
				for _, body := range bodies {
					if body.closed.Load() != 1 {
						t.Fatal("body handback was not exactly once", body.closed.Load())
					}
				}
				if verificationRuntimeOperations(runtime) != 0 {
					t.Fatal("joined provider failure retained its operation")
				}
			})
		}
	}
}

type verificationClaimedCorruption struct{}

func (verificationClaimedCorruption) Error() string { return "claimed corruption" }
func (verificationClaimedCorruption) Is(error) bool { return true }

type verificationCyclicError struct{}

func (e *verificationCyclicError) Error() string { return "cycle" }
func (e *verificationCyclicError) Unwrap() error { return e }

func TestProtectedVerificationCorruptionClassifierIsBounded(t *testing.T) {
	wide := make([]error, 65)
	for i := range wide {
		wide[i] = ErrCorrupt
	}
	for _, err := range []error{nil, errObjectNetwork, errors.Join(ErrCorrupt, errObjectNetwork),
		verificationClaimedCorruption{}, &verificationCyclicError{}, errors.Join(wide...)} {
		if onlyObjectCorruption(err) {
			t.Fatal("operational, unknown or excessive error tree classified as content corruption")
		}
	}
	if !onlyObjectCorruption(fmt.Errorf("descriptor: %w", errors.Join(ErrCorrupt, fmt.Errorf("digest: %w", ErrCorrupt)))) {
		t.Fatal("pure wrapped content corruption was rejected")
	}
}

func TestProtectedVerificationUsesFrozenPolicy(t *testing.T) {
	runtime, service, config := protectedVerificationFixture(t)
	commitRemoteRecovery(t, runtime.backend, "id", protectedFingerprint(t, config))
	config.Acceleration.Datasets[0].Verification.MaxBytes = 1
	config.Acceleration.Datasets[0].Limits.Timeout = time.Nanosecond
	config.Acceleration.ObjectStorage.Prefix = "replacement"
	if err := runtime.Match(config); err == nil {
		t.Fatal("mutated catalog matched the frozen runtime")
	}
	service.takeEvents()
	if _, err := runtime.backend.Verify(context.Background(), "events"); err != nil {
		t.Fatal("caller mutation changed the live reader policy", err)
	}
	for _, event := range service.takeEvents() {
		if strings.Contains(event.key, "replacement") {
			t.Fatal("caller mutation retargeted a provider request", event)
		}
	}
}

func TestProtectedVerificationConstructorsRejectUnsupportedPolicyBeforeResources(t *testing.T) {
	for _, storage := range []string{"local", "legacy"} {
		for _, constructor := range []string{"manager", "open", "injected", "private"} {
			t.Run(storage+"/"+constructor, func(t *testing.T) {
				config := objectRuntimeTestConfig(t)
				config.Acceleration.Datasets[0].Verification = &catalog.VerificationLimits{MaxBytes: 1 << 20}
				if storage == "local" {
					config.Acceleration.ObjectStorage = nil
				} else {
					config.Acceleration.ObjectStorage.ReaderRegistry = nil
				}
				client := newFakeSnapshotObjects()
				var err error
				switch constructor {
				case "manager":
					_, err = NewManager(config, func(catalog.Config, query.Limits) (query.Executor, error) {
						t.Fatal("invalid constructor created an executor")
						return nil, nil
					})
				case "open":
					_, err = OpenBackend(*config.Acceleration)
				case "injected":
					_, err = NewObjectBackend(*config.Acceleration, client)
				case "private":
					_, err = newObjectBackend(*config.Acceleration, client)
				}
				if err == nil || !strings.Contains(err.Error(), "verification limits require protected object storage") {
					t.Fatal("constructor ignored unsupported verification policy", err)
				}
				if _, err := os.Stat(config.Acceleration.Directory); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("invalid policy created local resources", err)
				}
				if client.closed != 0 || client.manifestGets != 0 || client.payloadGets != 0 || len(client.objects) != 0 {
					t.Fatal("rejected constructor adopted or accessed the injected provider")
				}
			})
		}
	}
}

func TestProtectedVerificationInventoryRejectsPersistedRowAndSchemaMismatch(t *testing.T) {
	for _, field := range []string{"rows", "schema"} {
		t.Run(field, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			snapshot := commitRemoteRecovery(t, runtime.backend, "id", protectedFingerprint(t, config))
			var binding readerlease.Binding
			// Model a self-consistent stored identity with incorrect content
			// metadata, so the failure must come from Parquet verification.
			service.mutateManifest(t, runtime.backend.key("events", storeManifestName), func(root *objectManifest) {
				if field == "rows" {
					root.Committed.Rows++
				} else {
					root.Committed.SchemaHash = strings.Repeat("0", 64)
				}
				var err error
				binding, err = objectReaderBinding(root.Committed.ReaderBinding.Reference, config.Acceleration.ObjectStorage.ObjectLocation, root.Committed)
				if err != nil {
					t.Fatal(err)
				}
				root.Committed.ReaderBinding = &binding
			})
			func() {
				service.mu.Lock()
				defer service.mu.Unlock()
				key := runtime.backend.key("events", "reader-leases/"+snapshot.Generation+".yml")
				object := service.objects[key]
				var document map[string]any
				if err := yaml.Unmarshal(object.data, &document); err != nil {
					t.Fatal(err)
				}
				document["content_sha256"] = binding.ContentSHA256
				raw, err := yaml.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				object.data, object.info.Size, object.info.SHA256 = raw, int64(len(raw)), objectSHA256(raw)
				object.info.Version += "-metadata"
				service.objects[key] = object
			}()
			service.takeEvents()
			entries, err := runtime.backend.Inventory(context.Background(), "events")
			if err != nil || len(entries) != 1 || entries[0].Verified || entries[0].Snapshot.ObjectKey != "" {
				t.Fatal("persisted content mismatch not classified safely", entries, err)
			}
			ranges := 0
			for _, event := range service.takeEvents() {
				if event.operation == "range" {
					ranges++
				}
			}
			if ranges == 0 {
				t.Fatal("content mismatch was not discovered by reading Parquet")
			}
		})
	}
}
