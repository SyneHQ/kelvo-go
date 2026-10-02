//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func commitRecoverySnapshot(t *testing.T, s *Store, column, fingerprint string) Snapshot {
	t.Helper()
	tx, err := s.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	return finishRecoverySnapshot(t, tx, column, fingerprint)
}
func finishRecoverySnapshot(t *testing.T, tx *Transaction, column, fingerprint string) Snapshot {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: column, Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err := tx.SetSchema(schema); err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(42)
	record := builder.NewRecordBatch()
	defer record.Release()
	sink := NewParquetSink(tx.File(), query.DefaultLimits())
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
	snapshot, err := tx.Commit(fingerprint, 1)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
func restoreRequest(target, current Snapshot) RestoreRequest {
	return RestoreRequest{Dataset: "events", Generation: target.Generation, ExpectedGeneration: current.Generation, Fingerprint: current.Fingerprint}
}
func TestRecoveryInventoryRestoreRetainsTimestampAndReaderPins(t *testing.T) {
	s, _ := newTestStore(t)
	first := commitRecoverySnapshot(t, s, "id", "config-v1")
	// A prior-version current snapshot without a sidecar is backfilled by commit.
	if err := os.Remove(filepath.Join(filepath.Dir(first.Path), generationManifestName(first.Generation))); err != nil {
		t.Fatal(err)
	}
	second := commitRecoverySnapshot(t, s, "id", "config-v1")
	inventory, err := s.Inventory(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory) != 2 {
		t.Fatalf("inventory count: %d", len(inventory))
	}
	for _, entry := range inventory {
		if !entry.Verified {
			t.Fatalf("generation failed verification: %s", entry.Snapshot.Generation)
		}
	}
	lease, err := s.Acquire(context.Background(), "events", "config-v1", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	restored, err := s.Restore(context.Background(), restoreRequest(first, second))
	if err != nil {
		t.Fatal(err)
	}
	if restored.Generation != first.Generation || !restored.RefreshedAt.Equal(first.RefreshedAt) {
		t.Fatal("restore changed generation or source data time")
	}
	if _, err := s.Acquire(context.Background(), "events", "config-v1", time.Nanosecond); !errors.Is(err, ErrStale) {
		t.Fatalf("restore relabeled old data fresh: %v", err)
	}
	if err := s.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(second.Path); err != nil {
		t.Fatal("restore/prune removed pinned reader payload")
	}
	lease.Close()
	if err := s.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(second.Path), generationManifestName(second.Generation))); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pruned generation metadata remains")
	}
}
func TestRecoveryRejectsStaleIntentAuthorizationAndSchema(t *testing.T) {
	t.Run("stale_intent", func(t *testing.T) {
		s, _ := newTestStore(t)
		first := commitRecoverySnapshot(t, s, "id", "v1")
		second := commitRecoverySnapshot(t, s, "id", "v1")
		tx, err := s.Begin(context.Background(), "events")
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Abort()
		done := make(chan error, 1)
		go func() { _, err := s.Restore(context.Background(), restoreRequest(first, second)); done <- err }()
		third := finishRecoverySnapshot(t, tx, "id", "v1")
		select {
		case err := <-done:
			if !errors.Is(err, ErrRestoreConflict) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("restore blocked after writer released")
		}
		requireCurrentGeneration(t, s, third.Generation)
	})
	for _, kind := range []string{"authorization", "schema"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := newTestStore(t)
			first := commitRecoverySnapshot(t, s, "id", "v1")
			field, fp := "id", "v2"
			if kind == "schema" {
				field, fp = "different", "v1"
			}
			second := commitRecoverySnapshot(t, s, field, fp)
			_, err := s.Restore(context.Background(), restoreRequest(first, second))
			if kind == "authorization" && !errors.Is(err, ErrFingerprintMismatch) {
				t.Fatal(err)
			}
			if kind == "schema" && !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			requireCurrentGeneration(t, s, second.Generation)
		})
	}
}
func TestRecoveryCorruptPayloadAndRequiredCurrentVerification(t *testing.T) {
	for _, which := range []string{"target", "current"} {
		t.Run(which, func(t *testing.T) {
			s, _ := newTestStore(t)
			first := commitRecoverySnapshot(t, s, "id", "v1")
			second := commitRecoverySnapshot(t, s, "id", "v1")
			path := first.Path
			if which == "current" {
				path = second.Path
			}
			if err := os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.WriteAt([]byte("BAD!"), 0)
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			os.Chmod(path, 0400)
			entries, err := s.Inventory(context.Background(), "events")
			if err != nil {
				t.Fatal(err)
			}
			unverified := 0
			for _, entry := range entries {
				if !entry.Verified {
					unverified++
				}
			}
			if unverified != 1 {
				t.Fatalf("corrupt inventory entries: %d", unverified)
			}
			if _, err := s.Restore(context.Background(), restoreRequest(first, second)); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("corrupt %s restore: %v", which, err)
			}
			current, err := s.Status("events")
			if err != nil {
				t.Fatal(err)
			}
			if current.Generation != second.Generation {
				t.Fatal("failed restore changed current")
			}
		})
	}
}
func TestRecoveryBoundedInventoryAndUnsupportedRemote(t *testing.T) {
	s, _ := newTestStore(t)
	first := commitRecoverySnapshot(t, s, "id", "v1")
	dir, err := s.openDataset("events", false)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	manifest, err := storeReadManifest(dir, "events")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < InventoryLimit; i++ {
		manifest.Generation = fmt.Sprintf("%032x", i)
		if err := storeSaveGeneration(dir, manifest); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Inventory(context.Background(), "events"); !errors.Is(err, ErrInventoryLimit) {
		t.Fatalf("unbounded inventory: %v", err)
	}
	if _, err := s.Restore(context.Background(), RestoreRequest{Dataset: "events", Generation: first.Generation, Fingerprint: "v1"}); err == nil {
		t.Fatal("missing expected current accepted")
	}
	remote := &objectBackend{}
	if _, err := remote.Inventory(context.Background(), "events"); !errors.Is(err, ErrRecoveryUnsupported) {
		t.Fatal(err)
	}
	if _, err := remote.Restore(context.Background(), RestoreRequest{}); !errors.Is(err, ErrRecoveryUnsupported) {
		t.Fatal(err)
	}
}
