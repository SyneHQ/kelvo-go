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
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

func TestRefreshCommitMarksFailedManifestStageCleanup(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "multipart"}[multipart], func(t *testing.T) {
			store, _ := newTestStore(t)
			var directory, generation string
			var commit func() (Snapshot, error)
			if multipart {
				writer, err := store.BeginMultipart(context.Background(), "events", multipartTestOptions())
				if err != nil {
					t.Fatal(err)
				}
				if err := writeMultipartTestPart(t, writer, "id", 2); err != nil {
					t.Fatal(err)
				}
				tx := writer.(*multipartTransaction)
				directory, generation = tx.tx.dir.Name(), tx.tx.generation
				commit = func() (Snapshot, error) { return writer.Commit("config-v1") }
			} else {
				tx, err := store.Begin(context.Background(), "events")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.WriteString(tx.File(), "PAR1fixturePAR1"); err != nil {
					t.Fatal(err)
				}
				directory, generation = tx.dir.Name(), tx.generation
				commit = func() (Snapshot, error) { return tx.Commit("config-v1", 1) }
			}
			// The payload has been renamed before manifest writing encounters
			// this collision. Cleanup must report the undeletable stage.
			stage := filepath.Join(directory, ".manifest-"+generation+".yaml")
			if err := os.Mkdir(stage, 0700); err != nil {
				t.Fatal(err)
			}
			if snapshot, err := commit(); !errors.Is(err, ErrRefreshCleanup) || snapshot.Generation != "" {
				t.Fatal("failed stage cleanup was reported as settled", err, snapshot.Generation)
			}
			entries, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), generation) {
					t.Fatal("failed publication left a renamed payload or sidecar", entry.Name())
				}
			}
			if _, err := os.Stat(stage); err != nil {
				t.Fatal("fixture collision was unexpectedly removed", err)
			}
		})
	}
}

type lostClaimCleanupClient struct{ *fakeSnapshotObjects }

func (c *lostClaimCleanupClient) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
	info, err := c.fakeSnapshotObjects.Put(ctx, key, body, size, digest, condition)
	if err != nil {
		return info, err
	}
	c.mu.Lock()
	c.unavailable = true
	c.mu.Unlock()
	return objectstore.Info{}, errObjectNetwork
}

func TestRefreshBeginReportsLostClaimAndFailedCleanup(t *testing.T) {
	objects := newFakeSnapshotObjects()
	backend := testObjectBackend(t, objects)
	backend.writeClient = func() (objectstore.Client, bool, error) { return &lostClaimCleanupClient{objects}, false, nil }
	writer, err := backend.Begin(context.Background(), "events")
	if writer != nil || !errors.Is(err, errObjectNetwork) || !errors.Is(err, ErrRefreshCleanup) {
		t.Fatal("failed Begin hid its remote cleanup failure", writer, err)
	}
	objects.mu.Lock()
	defer objects.mu.Unlock()
	if len(objects.objects) != 1 {
		t.Fatal("fixture did not persist the ambiguous writer claim")
	}
}
