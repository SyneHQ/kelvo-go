//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.yaml.in/yaml/v3"
)

type recoveryObjectClient struct {
	*fakeSnapshotObjects
	beforeGet   func(string)
	ranges      int
	failRelease bool
}

func (c *recoveryObjectClient) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	if c.beforeGet != nil {
		c.beforeGet(key)
	}
	return c.fakeSnapshotObjects.Get(ctx, key, version)
}
func (c *recoveryObjectClient) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
	if c.failRelease && strings.HasSuffix(key, storeManifestName) {
		data, err := io.ReadAll(body)
		if err != nil {
			return objectstore.Info{}, err
		}
		if _, err := body.Seek(0, io.SeekStart); err != nil {
			return objectstore.Info{}, err
		}
		var manifest objectManifest
		if err := yaml.Unmarshal(data, &manifest); err != nil {
			return objectstore.Info{}, err
		}
		if manifest.Writer == nil && manifest.Committed != nil {
			c.failRelease = false
			return objectstore.Info{}, errObjectNetwork
		}
	}
	return c.fakeSnapshotObjects.Put(ctx, key, body, size, digest, condition)
}
func (c *recoveryObjectClient) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, objectstore.Info, error) {
	body, info, err := c.fakeSnapshotObjects.Get(ctx, key, version)
	if err != nil {
		return nil, info, err
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, info, err
	}
	if offset < 0 || length < 1 || offset > int64(len(data)) || length > int64(len(data))-offset {
		return nil, info, errors.New("invalid fixture range")
	}
	c.ranges++
	return io.NopCloser(bytes.NewReader(data[offset : offset+length])), info, nil
}
func recoveryObjectBackend(t *testing.T) (*objectBackend, *recoveryObjectClient) {
	t.Helper()
	client := &recoveryObjectClient{fakeSnapshotObjects: newFakeSnapshotObjects()}
	backend, err := NewObjectBackend(testObjectConfig(t), client)
	if err != nil {
		t.Fatal(err)
	}
	remote := backend.(*objectBackend)
	remote.pollInterval = time.Millisecond
	t.Cleanup(func() { remote.Close() })
	return remote, client
}
func commitRemoteRecovery(t *testing.T, backend *objectBackend, name, fp string) Snapshot {
	t.Helper()
	writer, err := backend.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Abort()
	schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err := writer.(SchemaWriter).SetSchema(schema); err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(7)
	record := builder.NewRecordBatch()
	defer record.Release()
	sink := NewParquetSink(writer.File(), query.DefaultLimits())
	defer sink.Abort()
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := writer.Commit(fp, 1)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
func TestRemoteRecoveryLegacyMigrationRestoreAndPinnedVersions(t *testing.T) {
	backend, client := recoveryObjectBackend(t)
	first := commitRemoteRecovery(t, backend, "id", "v1")
	client.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) { m.Version = 2; m.History = nil; m.HistoryTruncated = false })
	state, err := backend.readState(context.Background(), "events", client)
	if err != nil || state.manifest.Version != 2 {
		t.Fatal("legacy v2 manifest not readable")
	}
	inventory, err := backend.Inventory(context.Background(), "events")
	if err != nil || len(inventory) != 1 || inventory[0].CatalogScope != "retained_manifest" {
		t.Fatalf("legacy inventory: %v", err)
	}
	second := commitRemoteRecovery(t, backend, "id", "v1")
	state, err = backend.readState(context.Background(), "events", client)
	if err != nil || state.manifest.Version != 3 || len(state.manifest.History) != 1 {
		t.Fatal("legacy commit did not migrate to v3 history")
	}
	pinned, err := backend.Acquire(context.Background(), "events", "v1", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	restored, err := backend.Restore(context.Background(), restoreRequest(first, second))
	if err != nil {
		t.Fatal(err)
	}
	if restored.Generation != first.Generation || restored.ObjectVersion != first.ObjectVersion || !restored.RefreshedAt.Equal(first.RefreshedAt) {
		t.Fatal("restore replaced immutable identity or source data timestamp")
	}
	inventory, err = backend.Inventory(context.Background(), "events")
	if err != nil || len(inventory) != 2 {
		t.Fatal(err)
	}
	for _, generation := range inventory {
		if !generation.Verified {
			t.Fatal("retained generation failed verification")
		}
	}
	if _, err := backend.reader.Head(context.Background(), pinned.Snapshot.ObjectKey, pinned.Snapshot.ObjectVersion); err != nil {
		t.Fatal("restore changed pinned reader version")
	}
	if client.ranges == 0 {
		t.Fatal("schema verification did not use range reads")
	}
	// An ambiguous successful CAS is reconciled by exact immutable commit identity.
	client.publicationError = "after"
	again, err := backend.Restore(context.Background(), restoreRequest(second, first))
	if err != nil || again.Generation != second.Generation {
		t.Fatalf("ambiguous restore not reconciled: %v", err)
	}
}
func TestRemoteHistoryCatalogBoundedAndNeverDeletesObjects(t *testing.T) {
	backend, client := recoveryObjectBackend(t)
	var first Snapshot
	for i := 0; i < ObjectHistoryLimit+3; i++ {
		snapshot := commitRemoteRecovery(t, backend, "id", "v1")
		if i == 0 {
			first = snapshot
		}
	}
	inventory, err := backend.Inventory(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory) != ObjectHistoryLimit+1 || !inventory[0].CatalogTruncated {
		t.Fatalf("unbounded history catalog: %d", len(inventory))
	}
	if _, err := client.Head(context.Background(), first.ObjectKey, first.ObjectVersion); err != nil {
		t.Fatal("metadata eviction deleted immutable data")
	}
	_, err = backend.Restore(context.Background(), RestoreRequest{Dataset: "events", Generation: first.Generation, Fingerprint: "v1", ExpectedGeneration: inventory[0].Snapshot.Generation})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("untracked older generation restored: %v", err)
	}
}
func TestRemoteRestoreRejectsChangedAuthorizationSchemaCorruptionAndFence(t *testing.T) {
	for _, kind := range []string{"authorization", "schema", "checksum", "fence", "stale_intent", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			backend, client := recoveryObjectBackend(t)
			first := commitRemoteRecovery(t, backend, "id", "v1")
			name, fp := "id", "v1"
			if kind == "authorization" {
				fp = "v2"
			}
			if kind == "schema" {
				name = "new_column"
			}
			second := commitRemoteRecovery(t, backend, name, fp)
			request := restoreRequest(first, second)
			if kind == "checksum" {
				client.mu.Lock()
				object := client.objects[first.ObjectKey]
				object.data[0] ^= 1
				client.objects[first.ObjectKey] = object
				client.mu.Unlock()
			}
			if kind == "fence" {
				client.beforeGet = func(key string) {
					if key == first.ObjectKey {
						client.beforeGet = nil
						client.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) {
							m.Writer = &objectWriterLease{Owner: strings.Repeat("b", 32), ExpiresAt: time.Now().Add(time.Minute)}
						})
					}
				}
			}
			if kind == "stale_intent" {
				request.ExpectedGeneration = first.Generation
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if kind == "canceled" {
				cancel()
			}
			_, err := backend.Restore(ctx, request)
			if err == nil {
				t.Fatal("unsafe restore accepted")
			}
			expected := map[string]error{"authorization": ErrFingerprintMismatch, "schema": ErrCorrupt, "checksum": ErrCorrupt, "fence": ErrLeaseLost, "stale_intent": ErrRestoreConflict, "canceled": context.Canceled}[kind]
			if !errors.Is(err, expected) {
				t.Fatalf("expected %v, got %v", expected, err)
			}
			status, err := backend.Status(context.Background(), "events")
			if err != nil || status.Generation != second.Generation {
				t.Fatal("failed restore changed current generation")
			}
		})
	}
}
func TestRemoteManifestHistoryValidationAndByteBudget(t *testing.T) {
	base := &objectCommitted{Generation: strings.Repeat("a", 32), Fingerprint: strings.Repeat("f", 4096), SHA256: strings.Repeat("d", 64), Rows: 1, Bytes: 1, RefreshedAt: time.Now().UTC(), ObjectVersion: strings.Repeat("v", 4096)}
	previous := objectManifest{Version: 3, Dataset: "events", Committed: base}
	for i := 0; i < ObjectHistoryLimit; i++ {
		copy := *base
		copy.Generation = fmt.Sprintf("%032x", i)
		previous.History = append(previous.History, &copy)
	}
	next := *base
	next.Generation = strings.Repeat("c", 32)
	manifest, err := nextObjectManifest(previous, &next)
	if err != nil {
		t.Fatal(err)
	}
	data, err := yaml.Marshal(manifest)
	if err != nil || len(data) > storeManifestLimit-objectHistoryLeaseReserve || !manifest.HistoryTruncated {
		t.Fatal("byte budget not enforced")
	}
	for _, mutate := range []func(*objectManifest){
		func(m *objectManifest) { m.Version = 4 },
		func(m *objectManifest) { m.History = []*objectCommitted{m.Committed} },
		func(m *objectManifest) { m.History = []*objectCommitted{nil} },
		func(m *objectManifest) { m.Version = 2; m.History = previous.History[:1] },
		func(m *objectManifest) { m.History = append(previous.History, base) },
	} {
		invalid := manifest
		mutate(&invalid)
		if validateObjectManifest(invalid, "events") == nil {
			t.Fatal("invalid retained catalog accepted")
		}
	}
}

func TestRemoteRestoreNoOpReleasesLeaseAndReportsReleaseFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			backend, client := recoveryObjectBackend(t)
			current := commitRemoteRecovery(t, backend, "id", "v1")
			client.failRelease = fail
			restored, err := backend.Restore(context.Background(), restoreRequest(current, current))
			if fail {
				if !errors.Is(err, errObjectNetwork) {
					t.Fatalf("release failure falsely reported as successful no-op: %v", err)
				}
			} else if err != nil || restored.Generation != current.Generation {
				t.Fatalf("no-op restore: %v", err)
			}
			state, readErr := backend.readState(context.Background(), "events", client)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if state.manifest.Committed.Generation != current.Generation {
				t.Fatal("no-op changed current")
			}
			if !fail && state.manifest.Writer != nil {
				t.Fatal("successful no-op left writer lease")
			}
		})
	}
}
