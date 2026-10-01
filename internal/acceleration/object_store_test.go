//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"go.yaml.in/yaml/v3"
)

type fakeSnapshotObject struct {
	data []byte
	info objectstore.Info
}

// Independent backends share this conditional object service, not a filesystem.
// It models provider versions separately from payload digests.
type fakeSnapshotObjects struct {
	mu                sync.Mutex
	objects           map[string]fakeSnapshotObject
	sequence          int
	closed            int
	clockOffset       time.Duration
	failPayload       bool
	rejectRenewal     bool
	publicationError  string
	unavailable       bool
	corruptPayloadGet bool
	headFault         string
	manifestGets      int
	payloadGets       int
}

var errObjectNetwork = errors.New("simulated object service interruption")

func newFakeSnapshotObjects() *fakeSnapshotObjects {
	return &fakeSnapshotObjects{objects: make(map[string]fakeSnapshotObject)}
}

func (service *fakeSnapshotObjects) info(object fakeSnapshotObject) objectstore.Info {
	info := object.info
	info.ServerTime = time.Now().Add(service.clockOffset).UTC()
	return info
}

func (service *fakeSnapshotObjects) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, objectstore.Info{}, err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.unavailable {
		return nil, objectstore.Info{}, errObjectNetwork
	}
	object, found := service.objects[key]
	info := service.info(object)
	if !found {
		return nil, info, objectstore.ErrNotFound
	}
	if version != "" && object.info.Version != version {
		return nil, info, objectstore.ErrConflict
	}
	data := bytes.Clone(object.data)
	if strings.HasSuffix(key, storeManifestName) {
		service.manifestGets++
	} else {
		service.payloadGets++
		if service.corruptPayloadGet && len(data) > 0 {
			data[0] ^= 1
		}
	}
	return io.NopCloser(bytes.NewReader(data)), info, nil
}

func (service *fakeSnapshotObjects) Head(ctx context.Context, key, version string) (objectstore.Info, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.Info{}, err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.unavailable {
		return objectstore.Info{}, errObjectNetwork
	}
	object, found := service.objects[key]
	info := service.info(object)
	if !found {
		return info, objectstore.ErrNotFound
	}
	if version != "" && object.info.Version != version {
		return info, objectstore.ErrConflict
	}
	switch service.headFault {
	case "size":
		info.Size++
	case "empty":
		info.Size = 0
	case "version":
		info.Version = "other-version"
	case "digest":
		info.SHA256 = strings.Repeat("0", 64)
	}
	return info, nil
}

func (service *fakeSnapshotObjects) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.Info{}, err
	}
	if condition.Absent == (condition.Version != "") {
		return objectstore.Info{}, errors.New("fake requires exactly one write precondition")
	}
	data, err := io.ReadAll(io.LimitReader(body, 1<<20))
	if err != nil {
		return objectstore.Info{}, err
	}
	if int64(len(data)) != size || objectSHA256(data) != digest {
		return objectstore.Info{}, errors.New("fake received incomplete or corrupt upload")
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.unavailable {
		return objectstore.Info{}, errObjectNetwork
	}
	existing, found := service.objects[key]
	if condition.Absent && found || condition.Version != "" && (!found || condition.Version != existing.info.Version) {
		return service.info(existing), objectstore.ErrConflict
	}
	publication := false
	if strings.HasSuffix(key, storeManifestName) {
		var previous, next objectManifest
		_ = yaml.Unmarshal(existing.data, &previous)
		if err := yaml.Unmarshal(data, &next); err != nil {
			return objectstore.Info{}, err
		}
		if next.Writer != nil && previous.Writer != nil && next.Writer.Owner == previous.Writer.Owner && service.rejectRenewal {
			return objectstore.Info{}, objectstore.ErrConflict
		}
		publication = next.Committed != nil && next.Writer == nil && (previous.Committed == nil || next.Committed.Generation != previous.Committed.Generation)
		if publication && service.publicationError == "before" {
			return objectstore.Info{}, errObjectNetwork
		}
	} else if service.failPayload {
		return objectstore.Info{}, errObjectNetwork
	}
	service.sequence++
	object := fakeSnapshotObject{data: data, info: objectstore.Info{Size: size, SHA256: digest, Version: fmt.Sprintf("revision-%d", service.sequence)}}
	service.objects[key] = object
	if publication && service.publicationError != "" {
		if service.publicationError == "after-unavailable" {
			service.unavailable = true
		}
		return objectstore.Info{}, errObjectNetwork
	}
	return service.info(object), nil
}

func (service *fakeSnapshotObjects) Close() {
	service.mu.Lock()
	service.closed++
	service.mu.Unlock()
}

func (service *fakeSnapshotObjects) mutateManifest(t *testing.T, key string, mutate func(*objectManifest)) {
	t.Helper()
	service.mu.Lock()
	defer service.mu.Unlock()
	object, ok := service.objects[key]
	if !ok {
		t.Fatal("missing fake manifest")
	}
	var manifest objectManifest
	if err := yaml.Unmarshal(object.data, &manifest); err != nil {
		t.Fatal(err)
	}
	mutate(&manifest)
	data, err := yaml.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	service.sequence++
	service.objects[key] = fakeSnapshotObject{data: data, info: objectstore.Info{Size: int64(len(data)), SHA256: objectSHA256(data), Version: fmt.Sprintf("revision-%d", service.sequence)}}
}

func testObjectConfig(t *testing.T) catalog.AccelerationConfig {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return catalog.AccelerationConfig{Directory: filepath.Join(dir, "staging"), TenantID: "tenant-a", ObjectStorage: &catalog.ObjectStorage{
		ObjectLocation: catalog.ObjectLocation{Provider: "s3", Endpoint: "https://storage.example.test", Bucket: "snapshot-fixtures", Prefix: "private/cache", Region: "us-east-1"}}}
}

func testObjectBackend(t *testing.T, objects *fakeSnapshotObjects) *objectBackend {
	t.Helper()
	backend, err := NewObjectBackend(testObjectConfig(t), objects)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	remote := backend.(*objectBackend)
	remote.pollInterval = 5 * time.Millisecond
	return remote
}

func writeObjectSnapshot(t *testing.T, backend Backend, payload string) Snapshot {
	t.Helper()
	tx, err := backend.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	if _, err := io.WriteString(tx.File(), payload); err != nil {
		t.Fatal(err)
	}
	snapshot, err := tx.Commit("source-config", 3)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestObjectBackendPersistsAcrossIndependentWorkersWithoutSharedScratch(t *testing.T) {
	objects := newFakeSnapshotObjects()
	writer := testObjectBackend(t, objects)
	first := writeObjectSnapshot(t, writer, "PAR1remote snapshotPAR1")
	reader := testObjectBackend(t, objects)
	if reader.config.Directory == writer.config.Directory {
		t.Fatal("test workers share scratch")
	}
	for _, operation := range []func() error{
		func() error { _, err := reader.Status(context.Background(), "events"); return err },
		func() error {
			lease, err := reader.Acquire(context.Background(), "events", "source-config", time.Hour)
			if err == nil {
				err = lease.Close()
			}
			return err
		},
		func() error { _, err := reader.Verify(context.Background(), "events"); return err },
	} {
		if err := operation(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(reader.config.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("remote reads created or required local scratch: %v", err)
	}
	if first.ObjectKey != "private/cache/tenant-a/events/"+first.Generation+".parquet" || first.ObjectVersion == "" || first.Path != "s3://snapshot-fixtures/"+first.ObjectKey {
		t.Fatalf("invalid object snapshot: %+v", first)
	}
	objects.mu.Lock()
	for key, value := range objects.objects {
		if strings.HasSuffix(key, storeManifestName) && (bytes.Contains(value.data, []byte("storage.example")) || bytes.Contains(value.data, []byte(writer.config.Directory))) {
			t.Error("manifest contains an endpoint or local path")
		}
	}
	objects.mu.Unlock()
	if entries, err := os.ReadDir(filepath.Join(writer.config.Directory, "tenant-a", "events")); err != nil {
		t.Fatal(err)
	} else {
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".stage-") || entry.Name() == storeManifestName {
				t.Fatalf("remote refresh leaked local publication: %s", entry.Name())
			}
		}
	}
}

func TestObjectBackendPreservesLastGoodSnapshotWhileRefreshingAndAfterFailure(t *testing.T) {
	for _, failure := range []string{"abort", "cancel", "upload", "bad-digest", "empty", "oversize"} {
		t.Run(failure, func(t *testing.T) {
			objects := newFakeSnapshotObjects()
			backend := testObjectBackend(t, objects)
			good := writeObjectSnapshot(t, backend, "last-good")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tx, err := backend.Begin(ctx, "events")
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Abort()
			current, err := backend.Status(context.Background(), "events")
			if err != nil || current.Generation != good.Generation {
				t.Fatalf("writer hid committed snapshot: %+v %v", current, err)
			}
			if failure != "empty" {
				if _, err := io.WriteString(tx.File(), "replacement"); err != nil {
					t.Fatal(err)
				}
			}
			switch failure {
			case "abort":
				if err := tx.Abort(); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			case "upload":
				objects.mu.Lock()
				objects.failPayload = true
				objects.mu.Unlock()
			case "bad-digest":
				objects.mu.Lock()
				objects.headFault = "digest"
				objects.mu.Unlock()
			case "oversize":
				if err := tx.File().Truncate(objectstore.MaxUploadBytes + 1); err != nil {
					t.Fatal(err)
				}
			}
			if failure != "abort" {
				if _, err := tx.Commit("source-config", 3); err == nil {
					t.Fatal("failed refresh published")
				}
			}
			if err := tx.Abort(); err != nil {
				t.Fatal(err)
			}
			objects.mu.Lock()
			objects.headFault = ""
			objects.mu.Unlock()
			current, err = backend.Verify(context.Background(), "events")
			if err != nil || current.Generation != good.Generation {
				t.Fatalf("last good snapshot lost: %+v %v", current, err)
			}
		})
	}
}

func TestObjectBackendSerializesIndependentWritersAndHonorsCancellation(t *testing.T) {
	objects := newFakeSnapshotObjects()
	first := testObjectBackend(t, objects)
	second := testObjectBackend(t, objects)
	tx, err := first.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	if _, err := second.Begin(ctx, "events"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("remote writer was not excluded: %v", err)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
	writeObjectSnapshot(t, second, "after-abort")
}

func TestObjectBackendExpiredOwnerIsFencedFromPublishingOrReleasingNewOwner(t *testing.T) {
	objects := newFakeSnapshotObjects()
	first := testObjectBackend(t, objects)
	second := testObjectBackend(t, objects)
	good := writeObjectSnapshot(t, first, "initial")
	old, err := first.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer old.Abort()
	if _, err := io.WriteString(old.File(), "stale-writer"); err != nil {
		t.Fatal(err)
	}
	objects.mutateManifest(t, first.key("events", storeManifestName), func(manifest *objectManifest) { manifest.Writer.ExpiresAt = time.Now().Add(-time.Minute) })
	newOwner, err := second.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer newOwner.Abort()
	if _, err := old.Commit("source-config", 3); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale owner published: %v", err)
	}
	state, err := second.readState(context.Background(), "events", objects)
	if err != nil || state.manifest.Writer == nil || state.manifest.Writer.Owner != newOwner.(*objectTransaction).owner {
		t.Fatalf("stale owner cleared new owner: %+v %v", state.manifest.Writer, err)
	}
	if state.manifest.Committed.Generation != good.Generation {
		t.Fatal("fenced commit changed active snapshot")
	}
	if _, err := io.WriteString(newOwner.File(), "new-owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := newOwner.Commit("source-config", 3); err != nil {
		t.Fatal(err)
	}
}

func TestObjectBackendRenewalFailureCancelsExtractionContext(t *testing.T) {
	objects := newFakeSnapshotObjects()
	backend := testObjectBackend(t, objects)
	backend.renewInterval = 10 * time.Millisecond
	tx, err := backend.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	objects.mu.Lock()
	objects.rejectRenewal = true
	objects.mu.Unlock()
	select {
	case <-tx.Context().Done():
		if !errors.Is(context.Cause(tx.Context()), ErrLeaseLost) {
			t.Fatalf("wrong cancellation cause: %v", context.Cause(tx.Context()))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lost lease did not cancel extraction")
	}
	if _, err := tx.Commit("source-config", 0); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("lost lease commit: %v", err)
	}
}

func TestObjectBackendRenewsUsingStorageClockAndKeepsCommittedEntry(t *testing.T) {
	objects := newFakeSnapshotObjects()
	objects.clockOffset = 4 * time.Hour
	backend := testObjectBackend(t, objects)
	backend.leaseDuration = 200 * time.Millisecond
	backend.renewInterval = 20 * time.Millisecond
	good := writeObjectSnapshot(t, backend, "clock-test")
	tx, err := backend.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	time.Sleep(240 * time.Millisecond)
	state, err := backend.readState(context.Background(), "events", objects)
	if err != nil {
		t.Fatal(err)
	}
	if state.manifest.Writer == nil || !state.manifest.Writer.ExpiresAt.After(state.now()) || state.manifest.Committed.Generation != good.Generation {
		t.Fatal("renewal did not preserve snapshot or server-clock lease")
	}
	if tx.Context().Err() != nil {
		t.Fatalf("valid lease canceled: %v", context.Cause(tx.Context()))
	}
}

func TestObjectBackendReconcilesAmbiguousPublicationWithoutDeletingObjects(t *testing.T) {
	for _, failure := range []string{"after", "after-unavailable", "before"} {
		t.Run(failure, func(t *testing.T) {
			objects := newFakeSnapshotObjects()
			backend := testObjectBackend(t, objects)
			previous := writeObjectSnapshot(t, backend, "previous")
			tx, err := backend.Begin(context.Background(), "events")
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Abort()
			if _, err := io.WriteString(tx.File(), "new generation"); err != nil {
				t.Fatal(err)
			}
			objects.mu.Lock()
			objects.publicationError = failure
			objects.mu.Unlock()
			snapshot, err := tx.Commit("source-config", 3)
			if failure == "after" {
				if err != nil || snapshot.Generation == previous.Generation || snapshot.Generation == "" {
					t.Fatalf("successful publication was not reconciled: %+v %v", snapshot, err)
				}
			} else if !errors.Is(err, ErrPublicationUnknown) {
				t.Fatalf("ambiguous publication was misreported: %v", err)
			}
			objects.mu.Lock()
			objects.unavailable = false
			objects.publicationError = ""
			count := len(objects.objects)
			objects.mu.Unlock()
			if count != 3 {
				t.Fatalf("an immutable object was removed after uncertain publication: %d objects", count)
			}
			current, err := backend.Verify(context.Background(), "events")
			if err != nil {
				t.Fatal(err)
			}
			if failure == "before" && current.Generation != previous.Generation {
				t.Fatal("unsuccessful publication replaced previous snapshot")
			}
			if failure == "after-unavailable" && current.Generation == previous.Generation {
				t.Fatal("successful but unconfirmed publication was rolled back")
			}
		})
	}
}

func TestObjectBackendReadersAndPruneRetainImmutableObjects(t *testing.T) {
	objects := newFakeSnapshotObjects()
	backend := testObjectBackend(t, objects)
	old := writeObjectSnapshot(t, backend, "old")
	lease, err := backend.Acquire(context.Background(), "events", "source-config", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		writeObjectSnapshot(t, backend, fmt.Sprintf("new-%d", i))
	}
	if err := backend.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.Head(context.Background(), old.ObjectKey, old.ObjectVersion); err != nil {
		t.Fatal("remote prune removed a leased generation")
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.Head(context.Background(), old.ObjectKey, old.ObjectVersion); err != nil {
		t.Fatal("remote prune deletes despite no distributed reader tracking")
	}
}

func TestObjectBackendPolicyAndIntegrity(t *testing.T) {
	objects := newFakeSnapshotObjects()
	backend := testObjectBackend(t, objects)
	writeObjectSnapshot(t, backend, "original")
	if _, err := backend.Acquire(context.Background(), "events", "changed", 0); !errors.Is(err, ErrFingerprintMismatch) {
		t.Fatalf("fingerprint mismatch: %v", err)
	}
	if _, err := backend.Acquire(context.Background(), "events", "source-config", time.Nanosecond); !errors.Is(err, ErrStale) {
		t.Fatalf("stale snapshot: %v", err)
	}
	objects.mu.Lock()
	reads := objects.payloadGets
	objects.mu.Unlock()
	lease, err := backend.Acquire(context.Background(), "events", "source-config", 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = lease.Close()
	objects.mu.Lock()
	readsAfter := objects.payloadGets
	objects.corruptPayloadGet = true
	objects.mu.Unlock()
	if readsAfter != reads {
		t.Fatal("Acquire downloaded the payload")
	}
	if _, err := backend.Verify(context.Background(), "events"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Verify ignored corrupt bytes: %v", err)
	}
	for _, fault := range []string{"size", "empty", "version", "digest"} {
		objects.mu.Lock()
		objects.headFault = fault
		objects.mu.Unlock()
		if _, err := backend.Status(context.Background(), "events"); !errors.Is(err, ErrCorrupt) {
			t.Errorf("accepted %s metadata: %v", fault, err)
		}
	}
}

func TestObjectBackendFreshnessUsesStorageClock(t *testing.T) {
	for _, offset := range []time.Duration{-4 * time.Hour, 4 * time.Hour} {
		t.Run(offset.String(), func(t *testing.T) {
			objects := newFakeSnapshotObjects()
			objects.clockOffset = offset
			backend := testObjectBackend(t, objects)
			writeObjectSnapshot(t, backend, "storage-clock snapshot")
			lease, err := backend.Acquire(context.Background(), "events", "source-config", time.Hour)
			if err != nil {
				t.Fatalf("worker clock expired a fresh snapshot: %v", err)
			}
			_ = lease.Close()
			objects.mutateManifest(t, backend.key("events", storeManifestName), func(manifest *objectManifest) {
				manifest.Committed.RefreshedAt = manifest.Committed.RefreshedAt.Add(-2 * time.Hour)
			})
			if _, err := backend.Acquire(context.Background(), "events", "source-config", time.Hour); !errors.Is(err, ErrStale) {
				t.Fatalf("worker clock extended an expired snapshot: %v", err)
			}
		})
	}
}

func TestObjectBackendRejectsMalformedManifests(t *testing.T) {
	for _, fault := range []string{"generation", "version", "dataset", "bytes", "sha256", "object-version", "owner", "missing-version", "missing-dataset", "null", "unknown-field", "oversized", "multiple-documents"} {
		t.Run(fault, func(t *testing.T) {
			objects := newFakeSnapshotObjects()
			backend := testObjectBackend(t, objects)
			writeObjectSnapshot(t, backend, "safe snapshot")
			key := backend.key("events", storeManifestName)
			objects.mutateManifest(t, key, func(manifest *objectManifest) {
				switch fault {
				case "generation":
					manifest.Committed.Generation = "../../other-tenant"
				case "version":
					manifest.Version = 3
				case "dataset":
					manifest.Dataset = "other"
				case "bytes":
					manifest.Committed.Bytes = 0
				case "sha256":
					manifest.Committed.SHA256 = "not-a-digest"
				case "object-version":
					manifest.Committed.ObjectVersion = "malformed\r\nheader"
				case "owner":
					manifest.Writer = &objectWriterLease{Owner: "../bad", ExpiresAt: time.Now()}
				}
			})
			objects.mu.Lock()
			object := objects.objects[key]
			switch fault {
			case "missing-version":
				object.data = bytes.Replace(object.data, []byte("version: 2\n"), nil, 1)
			case "missing-dataset":
				object.data = bytes.Replace(object.data, []byte("dataset: events\n"), nil, 1)
			case "null":
				object.data = []byte("null\n")
			case "unknown-field":
				object.data = append(object.data, []byte("endpoint: https://evil.example\n")...)
			case "oversized":
				object.data = bytes.Repeat([]byte("x"), storeManifestLimit+1)
			case "multiple-documents":
				object.data = append(object.data, []byte("---\nsecond: document\n")...)
			}
			object.info.Size, object.info.SHA256 = int64(len(object.data)), objectSHA256(object.data)
			objects.objects[key] = object
			objects.mu.Unlock()
			if _, err := backend.Status(context.Background(), "events"); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("accepted unsafe manifest: %v", err)
			}
		})
	}
}

func TestObjectBackendNeverLoadsWriterCredentialsForReadConstruction(t *testing.T) {
	config := testObjectConfig(t)
	config.ObjectStorage.ReadCredentials = catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_OBJECT_READER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_OBJECT_READER_SECRET"}
	config.ObjectStorage.WriteCredentials = catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_OBJECT_WRITER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_OBJECT_WRITER_SECRET"}
	t.Setenv("KELVO_SOURCE_OBJECT_READER_ID", "fixture-read-id")
	t.Setenv("KELVO_SOURCE_OBJECT_READER_SECRET", "fixture-read-secret")
	t.Setenv("KELVO_SOURCE_OBJECT_WRITER_ID", "")
	t.Setenv("KELVO_SOURCE_OBJECT_WRITER_SECRET", "")
	backend, err := OpenBackend(config)
	if err != nil {
		t.Fatalf("read construction loaded writer identity: %v", err)
	}
	defer backend.Close()
	if tx, err := backend.Begin(context.Background(), "events"); err == nil {
		_ = tx.Abort()
		t.Fatal("refresh accepted missing writer identity")
	}
	if _, err := os.Stat(config.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read construction touched staging: %v", err)
	}
}

func TestObjectBackendNamespacesAndClosedState(t *testing.T) {
	objects := newFakeSnapshotObjects()
	backend := testObjectBackend(t, objects)
	writeObjectSnapshot(t, backend, "tenant-a-only")
	config := testObjectConfig(t)
	config.TenantID = "tenant-b"
	other, err := NewObjectBackend(config, objects)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Status(context.Background(), "events"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tenant isolation: %v", err)
	}
	if _, err := backend.Begin(context.Background(), "../events"); err == nil {
		t.Fatal("traversal accepted")
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Status(context.Background(), "events"); !errors.Is(err, errBackendClosed) {
		t.Fatalf("closed backend usable: %v", err)
	}
}
