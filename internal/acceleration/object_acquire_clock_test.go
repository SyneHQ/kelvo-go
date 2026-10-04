//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

const acquisitionReadDelay = 750 * time.Millisecond

// A future timestamp forces the existing zero-age clamp independently of host
// scheduling before the read. The provider clock may be ahead of or behind us.
func futureAcquisitionFixture(t *testing.T, multipart bool, offset time.Duration) (*objectBackend, *acquisitionObjects, *objectCommitted) {
	t.Helper()
	backend, objects, committed := objectAcquisitionFixture(t, multipart)
	objects.clockOffset = offset
	committed.RefreshedAt = time.Now().Add(offset + time.Hour).UTC()
	objects.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) {
		m.Committed.RefreshedAt = committed.RefreshedAt
	})
	return backend, objects, committed
}

func delayAcquisitionRead(ctx context.Context) (time.Time, time.Duration) {
	started := time.Now()
	timer := time.NewTimer(acquisitionReadDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
	return started, time.Since(started)
}

func TestObjectAcquireDescriptorReadConsumesFreshness(t *testing.T) {
	for _, offset := range []time.Duration{-4 * time.Hour, 4 * time.Hour} {
		t.Run(offset.String(), func(t *testing.T) {
			backend, objects, committed := futureAcquisitionFixture(t, true, offset)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var waited time.Duration
			objects.bodyHook = func(key string, body io.ReadCloser) io.ReadCloser {
				if key == backend.key("events", objectDescriptorName(committed.Generation)) {
					return &acquisitionBody{ReadCloser: body, beforeRead: func() { _, waited = delayAcquisitionRead(ctx) }}
				}
				return body
			}
			lease, err := backend.Acquire(ctx, "events", committed.Fingerprint, acquisitionReadDelay/3)
			if lease != nil || !errors.Is(err, ErrStale) || waited < acquisitionReadDelay {
				t.Fatalf("descriptor delay did not expire initially accepted snapshot: lease=%v error=%v waited=%v", lease, err, waited)
			}
			for _, call := range objects.observed() {
				if call.method == "head" || call.method == "range" {
					t.Fatal("expired descriptor triggered payload access", call)
				}
			}
		})
	}
}

func TestObjectSnapshotAgeIncludesDescriptorRead(t *testing.T) {
	for _, operation := range []string{"status", "unlimited-acquire"} {
		for _, offset := range []time.Duration{-4 * time.Hour, 4 * time.Hour} {
			t.Run(operation+"/"+offset.String(), func(t *testing.T) {
				backend, objects, committed := futureAcquisitionFixture(t, true, offset)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var entered time.Time
				var waited time.Duration
				objects.bodyHook = func(key string, body io.ReadCloser) io.ReadCloser {
					if key == backend.key("events", objectDescriptorName(committed.Generation)) {
						return &acquisitionBody{ReadCloser: body, beforeRead: func() { entered, waited = delayAcquisitionRead(ctx) }}
					}
					return body
				}
				var snapshot Snapshot
				var err error
				if operation == "status" {
					snapshot, err = backend.Status(ctx, "events")
				} else {
					var lease *Lease
					lease, err = backend.Acquire(ctx, "events", committed.Fingerprint, 0)
					if err == nil {
						snapshot = lease.Snapshot
						defer lease.Close()
					}
				}
				if err != nil {
					t.Fatal("future timestamp or unlimited age rejected", err)
				}
				// Assert where the observation was captured as well as elapsed
				// age, so scheduler stalls after the descriptor cannot mask a reset.
				if entered.IsZero() {
					t.Fatal("descriptor fixture did not enter read")
				}
				if snapshot.ageObservedAt.After(entered) {
					t.Fatal("descriptor read reset the selected clock observation")
				}
				if snapshot.ageObserved != 0 || !snapshot.RefreshedAt.Equal(committed.RefreshedAt) {
					t.Fatal("zero-clamped service observation changed")
				}
				if waited < acquisitionReadDelay || snapshot.Age() < waited {
					t.Fatal("reported age omitted descriptor time", snapshot.Age(), waited)
				}
			})
		}
	}
}

func TestObjectAcquireMetadataReadConsumesFreshness(t *testing.T) {
	for _, layout := range []string{"single", "multipart"} {
		t.Run(layout, func(t *testing.T) {
			backend, objects, committed := futureAcquisitionFixture(t, layout == "multipart", 0)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var once sync.Once
			var waited time.Duration
			objects.afterHead = func(string) { once.Do(func() { _, waited = delayAcquisitionRead(ctx) }) }
			lease, err := backend.Acquire(ctx, "events", committed.Fingerprint, acquisitionReadDelay/3)
			if lease != nil || !errors.Is(err, ErrStale) || waited < acquisitionReadDelay {
				t.Fatalf("metadata delay bypassed final freshness check: lease=%v error=%v waited=%v", lease, err, waited)
			}
		})
	}
}

func TestObjectAcquireMultipartFreshnessUsesSelectedServiceClock(t *testing.T) {
	for _, offset := range []time.Duration{-4 * time.Hour, 4 * time.Hour} {
		for _, stale := range []bool{false, true} {
			name := "fresh/"
			if stale {
				name = "stale/"
			}
			t.Run(name+offset.String(), func(t *testing.T) {
				backend, objects, committed := objectAcquisitionFixture(t, true)
				objects.clockOffset = offset
				age := 10 * time.Second
				if stale {
					age = 2 * time.Hour
				}
				objects.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) {
					m.Committed.RefreshedAt = time.Now().Add(offset - age).UTC()
				})
				lease, err := backend.Acquire(context.Background(), "events", committed.Fingerprint, time.Hour)
				if stale {
					if lease != nil || !errors.Is(err, ErrStale) {
						t.Fatal("host clock extended stale multipart snapshot", err)
					}
					requireAcquisitionManifestOnly(t, backend, objects, 1)
					return
				}
				if err != nil {
					t.Fatal("host clock expired fresh multipart snapshot", err)
				}
				defer lease.Close()
				if lease.Snapshot.Age() < age || lease.Snapshot.Age() >= time.Hour {
					t.Fatal("multipart snapshot did not retain service-clock age", lease.Snapshot.Age())
				}
			})
		}
	}
}
