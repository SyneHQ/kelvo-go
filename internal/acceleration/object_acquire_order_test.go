//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestObjectAcquirePolicyPrecedesGenerationReads(t *testing.T) {
	for _, layout := range []string{"single", "multipart"} {
		for _, policy := range []string{"fingerprint", "stale"} {
			t.Run(layout+"/"+policy, func(t *testing.T) {
				backend, objects, _ := objectAcquisitionFixture(t, layout == "multipart")
				fingerprint, maxAge, want := "source-config", time.Second, ErrStale
				if policy == "fingerprint" {
					fingerprint, maxAge, want = "different-config", 0, ErrFingerprintMismatch
				}
				lease, err := backend.Acquire(context.Background(), "events", fingerprint, maxAge)
				if lease != nil || !errors.Is(err, want) {
					t.Fatalf("acquisition rejection: lease=%v error=%v", lease, err)
				}
				requireAcquisitionManifestOnly(t, backend, objects, 1)
			})
		}
	}
}

func TestObjectAcquireManifestFailuresStopGenerationReads(t *testing.T) {
	for _, mode := range []string{"negative-age", "cancelled", "missing", "uncommitted", "malformed", "cancel-on-close"} {
		t.Run(mode, func(t *testing.T) {
			backend, objects, _ := objectAcquisitionFixture(t, true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			maxAge, calls, want := time.Duration(0), 1, ErrNotFound
			switch mode {
			case "negative-age":
				maxAge, calls, want = -1, 0, nil
			case "cancelled":
				cancel()
				calls, want = 0, context.Canceled
			case "missing":
				objects.mu.Lock()
				delete(objects.objects, backend.key("events", storeManifestName))
				objects.mu.Unlock()
			case "uncommitted":
				objects.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) { m.Committed = nil })
			case "malformed":
				objects.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) { m.Dataset = "another_dataset" })
				want = ErrCorrupt
			case "cancel-on-close":
				objects.bodyHook = func(key string, body io.ReadCloser) io.ReadCloser {
					if key == backend.key("events", storeManifestName) {
						return &acquisitionBody{ReadCloser: body, afterClose: cancel}
					}
					return body
				}
				want = context.Canceled
			}
			lease, err := backend.Acquire(ctx, "events", "source-config", maxAge)
			if lease != nil || err == nil || (want != nil && !errors.Is(err, want)) {
				t.Fatalf("manifest failure: lease=%v error=%v", lease, err)
			}
			requireAcquisitionManifestOnly(t, backend, objects, calls)
		})
	}
}

func TestObjectAcquireUsesOneImmutableSelection(t *testing.T) {
	for _, layout := range []string{"single", "multipart"} {
		t.Run(layout, func(t *testing.T) {
			backend, objects, committed := objectAcquisitionFixture(t, layout == "multipart")
			var replaced sync.Once
			objects.bodyHook = func(key string, body io.ReadCloser) io.ReadCloser {
				if key == backend.key("events", storeManifestName) {
					replaced.Do(func() {
						// Get already captured the old bytes. The new pointer names
						// a different valid commit whose objects do not exist here.
						objects.mutateManifest(t, key, func(m *objectManifest) {
							m.Committed.Generation = strings.Repeat("c", 32)
							m.Committed.Fingerprint = "replacement-config"
						})
					})
				}
				return body
			}
			lease, err := backend.Acquire(context.Background(), "events", committed.Fingerprint, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			if lease.Snapshot.Generation != committed.Generation || lease.Snapshot.Fingerprint != committed.Fingerprint || !lease.Snapshot.RefreshedAt.Equal(committed.RefreshedAt) {
				t.Fatal("acquisition mixed current pointers", lease.Snapshot)
			}
			manifestReads, descriptorReads, heads := 0, 0, 0
			for _, call := range objects.observed() {
				if call.key == backend.key("events", storeManifestName) {
					manifestReads++
					continue
				}
				objects.mu.Lock()
				stored, exists := objects.objects[call.key]
				objects.mu.Unlock()
				if !exists || !strings.Contains(call.key, committed.Generation) || call.version != stored.info.Version {
					t.Fatalf("generation read lost selected exact version: %+v", call)
				}
				switch call.method {
				case "get":
					descriptorReads++
					if committed.Descriptor == nil || call.version != committed.Descriptor.ObjectVersion {
						t.Fatal("downloaded payload or wrong descriptor version")
					}
				case "head":
					heads++
				default:
					t.Fatal("unexpected generation operation", call)
				}
			}
			wantDescriptors, wantHeads := 0, 1
			if layout == "multipart" {
				wantDescriptors, wantHeads = 1, 2
			}
			if manifestReads != 1 || descriptorReads != wantDescriptors || heads != wantHeads {
				t.Fatal("unexpected immutable selection reads", objects.observed())
			}
		})
	}
}

func TestObjectAcquireRetainsGenerationIntegrityChecks(t *testing.T) {
	for _, layout := range []string{"single", "multipart"} {
		for _, fault := range []string{"size", "version", "digest", "descriptor-data"} {
			if layout == "single" && fault == "descriptor-data" {
				continue
			}
			t.Run(layout+"/"+fault, func(t *testing.T) {
				backend, objects, _ := objectAcquisitionFixture(t, layout == "multipart")
				if fault == "descriptor-data" {
					objects.corruptPayloadGet = true
				} else {
					objects.headFault = fault
				}
				lease, err := backend.Acquire(context.Background(), "events", "source-config", 0)
				if lease != nil || !errors.Is(err, ErrCorrupt) {
					t.Fatalf("acquisition lost integrity validation: lease=%v error=%v", lease, err)
				}
			})
		}
	}
}

func TestObjectStatusPreservesUnfilteredValidation(t *testing.T) {
	for _, layout := range []string{"single", "multipart"} {
		t.Run(layout, func(t *testing.T) {
			backend, objects, _ := objectAcquisitionFixture(t, layout == "multipart")
			objects.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) {
				m.Committed.Fingerprint = "previous-configuration"
				m.Committed.RefreshedAt = time.Now().Add(-48 * time.Hour).UTC()
			})
			snapshot, err := backend.Status(context.Background(), "events")
			if err != nil || snapshot.Fingerprint != "previous-configuration" || snapshot.Age() < 48*time.Hour {
				t.Fatal("Status applied query policy", snapshot, err)
			}
			objects.headFault = "digest"
			if _, err := backend.Status(context.Background(), "events"); !errors.Is(err, ErrCorrupt) {
				t.Fatal("Status skipped stale snapshot metadata validation", err)
			}
		})
	}
}

func TestObjectAcquireChecksCancellationAndClosureAfterIO(t *testing.T) {
	for _, mode := range []string{"descriptor-close", "head-cancel", "head-backend-close"} {
		t.Run(mode, func(t *testing.T) {
			backend, objects, committed := objectAcquisitionFixture(t, mode == "descriptor-close")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := error(context.Canceled)
			if mode == "descriptor-close" {
				objects.bodyHook = func(key string, body io.ReadCloser) io.ReadCloser {
					if key == backend.key("events", objectDescriptorName(committed.Generation)) {
						return &acquisitionBody{ReadCloser: body, afterClose: cancel}
					}
					return body
				}
			} else {
				objects.afterHead = func(string) { cancel() }
				if mode == "head-backend-close" {
					objects.afterHead = func(string) { backend.closed.Store(true) }
					want = errBackendClosed
				}
			}
			lease, err := backend.Acquire(ctx, "events", "source-config", 0)
			if lease != nil || !errors.Is(err, want) {
				t.Fatalf("successful I/O hid cancellation/closure: lease=%v error=%v", lease, err)
			}
			if mode == "descriptor-close" {
				for _, call := range objects.observed() {
					if call.method == "head" || call.method == "range" {
						t.Fatal("cancelled descriptor triggered payload access", call)
					}
				}
			}
		})
	}
}
