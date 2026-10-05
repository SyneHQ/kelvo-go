//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"go.yaml.in/yaml/v3"
)

// Deliberately exposes no RangeClient capability on the writer identity.
type protectedRestoreWriter struct {
	objectstore.Client
	target      string
	publication func(context.Context, func() (objectstore.Info, error)) (objectstore.Info, error)
	get         func(context.Context, string, string) (io.ReadCloser, objectstore.Info, error)
	attempts    atomic.Int32
}

func (c *protectedRestoreWriter) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	if c.get != nil {
		return c.get(ctx, key, version)
	}
	return c.Client.Get(ctx, key, version)
}

func (c *protectedRestoreWriter) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
	if strings.HasSuffix(key, storeManifestName) {
		raw, err := io.ReadAll(body)
		if err != nil {
			return objectstore.Info{}, err
		}
		if _, err := body.Seek(0, io.SeekStart); err != nil {
			return objectstore.Info{}, err
		}
		var root objectManifest
		if err := yaml.Unmarshal(raw, &root); err != nil {
			return objectstore.Info{}, err
		}
		if root.Writer == nil && root.Committed != nil && root.Committed.Generation == c.target {
			c.attempts.Add(1)
			if c.publication != nil {
				return c.publication(ctx, func() (objectstore.Info, error) { return c.Client.Put(ctx, key, body, size, digest, condition) })
			}
		}
	}
	return c.Client.Put(ctx, key, body, size, digest, condition)
}

func attachProtectedRestoreWriter(backend *objectBackend, service *protectedObjectService, target Snapshot) *protectedRestoreWriter {
	writer := &protectedRestoreWriter{Client: &protectedObjectClient{service}, target: target.Generation}
	backend.writeClient = func() (objectstore.Client, bool, error) { return writer, false, nil }
	return writer
}

func requireRestoreQuiescent(t *testing.T, runtime *ObjectRuntime) {
	t.Helper()
	if verificationRuntimeOperations(runtime) != 0 {
		t.Fatal("restore retained a joined operation")
	}
	runtime.backend.writers.mu.Lock()
	pending := len(runtime.backend.writers.pending)
	runtime.backend.writers.mu.Unlock()
	if pending != 0 {
		t.Fatal("restore retained a joined writer", pending)
	}
}

func TestProtectedRestorePreservesVersionsAndPinsBeforeAllData(t *testing.T) {
	for _, mode := range []string{"single-single", "single-multipart", "multipart-single", "noop"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			backend, fp := runtime.backend, protectedFingerprint(t, config)
			var target, current Snapshot
			if mode == "multipart-single" {
				target = commitVerificationMultipart(t, backend, fp)
			} else {
				target = commitRemoteRecovery(t, backend, "id", fp)
			}
			if mode == "single-multipart" {
				current = commitVerificationMultipart(t, backend, fp)
			} else if mode == "noop" {
				current = target
			} else {
				current = commitRemoteRecovery(t, backend, "id", fp)
			}
			initial, err := backend.readState(context.Background(), "events", backend.reader)
			if err != nil {
				t.Fatal(err)
			}
			_, expected := restoreEntry(initial.manifest, target.Generation)
			expected = cloneObjectCommit(expected)
			backend.config.Directory = filepath.Join(t.TempDir(), "no-restore-staging")
			writer := attachProtectedRestoreWriter(backend, service, target)
			service.takeEvents()
			got, err := backend.Restore(context.Background(), restoreRequest(target, current))
			if err != nil {
				t.Fatal(err)
			}
			if got.Generation != target.Generation || got.ObjectVersion != target.ObjectVersion || got.Rows != target.Rows || got.Bytes != target.Bytes || len(got.Parts) != len(target.Parts) || !got.RefreshedAt.Equal(target.RefreshedAt) {
				t.Fatal("restore changed immutable snapshot identity")
			}
			events := service.takeEvents()
			pins, expectedPins := 0, 2
			if mode == "noop" {
				expectedPins = 1
			}
			descriptors := map[string]int{}
			for _, event := range events {
				if event.operation == "pin" {
					pins++
				}
				if protectedGenerationEvent(event) && pins != expectedPins {
					t.Fatal("data accessed before complete pins", event, pins)
				}
				if event.operation == "stage" || event.operation == "seal" || event.operation == "payload" {
					t.Fatal("restore created snapshot or registry state", event)
				}
				if event.operation == "get" && strings.HasSuffix(event.key, ".parts.yaml") {
					descriptors[event.key]++
				}
			}
			for _, count := range descriptors {
				if count != 1 {
					t.Fatal("descriptor was loaded repeatedly", descriptors)
				}
			}
			if pins != expectedPins || writer.attempts.Load() != 1 {
				t.Fatal("unexpected pins/publication-or-release count", pins, writer.attempts.Load())
			}
			state, err := backend.readState(context.Background(), "events", backend.reader)
			if err != nil || state.manifest.Writer != nil || !sameObjectCommit(state.manifest.Committed, expected) {
				t.Fatal("restored commit or lease differs", err)
			}
			if _, err := os.Stat(backend.config.Directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("restore opened local staging", err)
			}
			requireRestoreQuiescent(t, runtime)
		})
	}
}

func TestProtectedRestoreRejectsInvalidRequestsBeforeProviderIO(t *testing.T) {
	for _, mode := range []string{"dataset", "generation", "expected", "fingerprint", "canceled", "missing-budget"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			snapshot := commitRemoteRecovery(t, runtime.backend, "id", protectedFingerprint(t, config))
			request := restoreRequest(snapshot, snapshot)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "dataset":
				request.Dataset = "unconfigured"
			case "generation":
				request.Generation = "../../secret"
			case "expected":
				request.ExpectedGeneration = "invalid"
			case "fingerprint":
				request.Fingerprint = "old-authority"
			case "canceled":
				cancel()
			case "missing-budget":
				config.Acceleration.Datasets[0].Verification = nil
				runtime = openVerificationFixture(t, config, service)
			}
			service.takeEvents()
			got, err := runtime.backend.Restore(ctx, request)
			if err == nil || got.Generation != "" || RestoreOutcomeOf(err) != RestoreNotAttempted {
				t.Fatal("invalid request reached restore", got, err)
			}
			if len(service.takeEvents()) != 0 {
				t.Fatal("invalid request performed provider I/O")
			}
			requireRestoreQuiescent(t, runtime)
		})
	}
}

func TestProtectedRestoreExactFinalFence(t *testing.T) {
	for _, mode := range []string{"writer-reference", "current", "target", "target-removed"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			backend, fp := runtime.backend, protectedFingerprint(t, config)
			target := commitRemoteRecovery(t, backend, "id", fp)
			current := commitRemoteRecovery(t, backend, "id", fp)
			writer := attachProtectedRestoreWriter(backend, service, target)
			var fired atomic.Bool
			service.beforeGet = func(key string) {
				if !strings.HasSuffix(key, ".parquet") || !fired.CompareAndSwap(false, true) {
					return
				}
				service.mutateManifest(t, backend.key("events", storeManifestName), func(root *objectManifest) {
					switch mode {
					case "writer-reference":
						root.Writer.ReaderReference.Incarnation = strings.Repeat("d", 64)
					case "current":
						root.Committed.RefreshedAt = root.Committed.RefreshedAt.Add(-1)
					case "target":
						root.History[0].RefreshedAt = root.History[0].RefreshedAt.Add(-1)
					case "target-removed":
						root.History = nil
					}
				})
			}
			got, err := backend.Restore(context.Background(), restoreRequest(target, current))
			if err == nil || got.Generation != target.Generation || writer.attempts.Load() != 0 || !fired.Load() {
				t.Fatal("changed fence was accepted or verified candidate lost", got.Generation, err, writer.attempts.Load())
			}
			if RestoreOutcomeOf(err) != RestoreNotAttempted {
				t.Fatal("fence failure claimed publication", err)
			}
			requireRestoreQuiescent(t, runtime)
		})
	}
}

func TestProtectedRestorePublicationOutcomesAndNoRetries(t *testing.T) {
	for _, mode := range []string{"applied", "not-applied", "failed-reconcile", "conflict", "cancel-after-apply"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			backend, fp := runtime.backend, protectedFingerprint(t, config)
			target := commitRemoteRecovery(t, backend, "id", fp)
			current := commitRemoteRecovery(t, backend, "id", fp)
			writer := attachProtectedRestoreWriter(backend, service, target)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			writer.publication = func(_ context.Context, put func() (objectstore.Info, error)) (objectstore.Info, error) {
				if mode == "conflict" {
					return objectstore.Info{}, objectstore.ErrConflict
				}
				if mode == "not-applied" {
					return objectstore.Info{}, errObjectNetwork
				}
				info, err := put()
				if err != nil {
					return info, err
				}
				if mode == "cancel-after-apply" {
					cancel()
					return info, nil
				}
				return info, errObjectNetwork
			}
			if mode == "failed-reconcile" {
				writer.get = func(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
					if writer.attempts.Load() != 0 {
						return nil, objectstore.Info{}, errObjectNetwork
					}
					return writer.Client.Get(ctx, key, version)
				}
			}
			got, err := backend.Restore(ctx, restoreRequest(target, current))
			if got.Generation != target.Generation || writer.attempts.Load() != 1 {
				t.Fatal("candidate lost or publication retried", got.Generation, writer.attempts.Load(), err)
			}
			switch mode {
			case "applied":
				if err != nil {
					t.Fatal("applied CAS was not reconciled", err)
				}
			case "not-applied", "failed-reconcile":
				if !errors.Is(err, ErrPublicationUnknown) || RestoreOutcomeOf(err) != RestoreUnknown {
					t.Fatal("uncertain CAS became definitive", err)
				}
			case "conflict":
				if !errors.Is(err, ErrRestoreConflict) || RestoreOutcomeOf(err) != RestoreNotPublished {
					t.Fatal("definite conflict became uncertain", err)
				}
			case "cancel-after-apply":
				if !errors.Is(err, context.Canceled) || RestoreOutcomeOf(err) != RestoreTargetObserved {
					t.Fatal("applied candidate or cancellation lost", err)
				}
			}
			if err != nil && strings.Contains(err.Error(), errObjectNetwork.Error()) {
				t.Fatal("provider diagnostics escaped outcome wrapper")
			}
			requireRestoreQuiescent(t, runtime)
		})
	}
}
