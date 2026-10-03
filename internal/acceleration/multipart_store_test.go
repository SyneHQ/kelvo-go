//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.yaml.in/yaml/v3"
)

func multipartTestOptions() MultipartOptions {
	return MultipartOptions{MaxParts: 4, MaxPartBytes: 1 << 20, MaxTotalBytes: 4 << 20}
}
func writeMultipartTestPart(t *testing.T, tx MultipartRefreshWriter, column string, rows int64) error {
	t.Helper()
	file, err := tx.NewPart()
	if err != nil {
		return err
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: column, Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for i := int64(0); i < rows; i++ {
		builder.Field(0).(*array.Int64Builder).Append(i)
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	sink := NewParquetSink(file, query.DefaultLimits())
	defer sink.Abort()
	if err = sink.Schema(schema); err != nil {
		return err
	}
	if rows > 0 {
		if err = sink.Write(record); err != nil {
			return err
		}
	}
	if err = sink.Finish(); err != nil {
		return err
	}
	return tx.SealPart(rows)
}
func commitMultipartTest(t *testing.T, s *Store, counts ...int64) Snapshot {
	t.Helper()
	tx, err := s.BeginMultipart(context.Background(), "events", multipartTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	for _, count := range counts {
		if err = writeMultipartTestPart(t, tx, "id", count); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := tx.Commit("config-v1")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
func TestMultipartStoreRoundTripAndEmpty(t *testing.T) {
	for _, counts := range [][]int64{{3, 5}, {0}} {
		t.Run(string(rune('0'+len(counts))), func(t *testing.T) {
			s, _ := newTestStore(t)
			snapshot := commitMultipartTest(t, s, counts...)
			if snapshot.Path != "" || len(snapshot.Parts) != len(counts) {
				t.Fatalf("bad multipart snapshot: %+v", snapshot)
			}
			var rows, bytes int64
			for i, p := range snapshot.Parts {
				if p.Rows != counts[i] || filepath.Base(p.Path) != multipartName(snapshot.Generation, i) {
					t.Fatal("part metadata mismatch")
				}
				rows += p.Rows
				bytes += p.Bytes
			}
			if snapshot.Rows != rows || snapshot.Bytes != bytes {
				t.Fatal("aggregate mismatch")
			}
			got, err := s.Verify(context.Background(), "events")
			if err != nil || got.Generation != snapshot.Generation {
				t.Fatalf("verify: %v", err)
			}
			inv, err := s.Inventory(context.Background(), "events")
			if err != nil || len(inv) != 1 || !inv[0].Verified {
				t.Fatalf("inventory: %+v, %v", inv, err)
			}
		})
	}
}
func TestMultipartStoreRestoreAndWholeGenerationPins(t *testing.T) {
	s, _ := newTestStore(t)
	legacy := commitRecoverySnapshot(t, s, "id", "config-v1")
	multi := commitMultipartTest(t, s, 2, 3)
	lease, err := s.Acquire(context.Background(), "events", "config-v1", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	restored, err := s.Restore(context.Background(), restoreRequest(legacy, multi))
	if err != nil || restored.Generation != legacy.Generation {
		t.Fatalf("restore legacy: %v", err)
	}
	if err = s.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
	for _, part := range multi.Parts {
		if _, err = os.Stat(part.Path); err != nil {
			t.Fatal("prune removed pinned part")
		}
	}
	if _, err = s.Restore(context.Background(), restoreRequest(multi, legacy)); err != nil {
		t.Fatal(err)
	}
	tx, err := s.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	schema, err := tx.PreviousSchema()
	if err != nil || schema.Field(0).Name != "id" {
		tx.Abort()
		t.Fatalf("previous multipart schema: %v", err)
	}
	tx.Abort()
	if _, err = s.Restore(context.Background(), restoreRequest(legacy, multi)); err != nil {
		t.Fatal(err)
	}
	lease.Close()
	if err = s.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
	for _, part := range multi.Parts {
		if _, err = os.Stat(part.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unleased part retained: %v", err)
		}
	}
}
func TestMultipartStoreRejectsCorruptAnyPart(t *testing.T) {
	for _, mode := range []string{"missing", "modified", "manifest"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := newTestStore(t)
			snapshot := commitMultipartTest(t, s, 2, 3)
			part := snapshot.Parts[1]
			switch mode {
			case "missing":
				if err := os.Remove(part.Path); err != nil {
					t.Fatal(err)
				}
			case "modified":
				if err := os.Chmod(part.Path, 0600); err != nil {
					t.Fatal(err)
				}
				f, err := os.OpenFile(part.Path, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.WriteAt([]byte("BAD!"), 0); err != nil {
					t.Fatal(err)
				}
				f.Close()
				os.Chmod(part.Path, 0400)
			case "manifest":
				dir := filepath.Dir(part.Path)
				data, err := os.ReadFile(filepath.Join(dir, storeManifestName))
				if err != nil {
					t.Fatal(err)
				}
				var m storeManifest
				if err = yaml.Unmarshal(data, &m); err != nil {
					t.Fatal(err)
				}
				m.Parts[1].Path = "../escape.parquet"
				data, err = yaml.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				os.Chmod(filepath.Join(dir, storeManifestName), 0600)
				if err = os.WriteFile(filepath.Join(dir, storeManifestName), data, 0400); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.Verify(context.Background(), "events"); err == nil {
				t.Fatal("corrupt multipart verified")
			}
			tx, err := s.Begin(context.Background(), "events")
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Abort()
			if _, err = tx.PreviousSchema(); err == nil {
				t.Fatal("corrupt previous multipart accepted")
			}
		})
	}
}
func TestMultipartStoreBoundsAndFailuresPreserveCurrent(t *testing.T) {
	for _, mode := range []string{"parts", "part_bytes", "total_bytes", "schema", "unsealed", "cancel", "publish"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := newTestStore(t)
			prior := commitRecoverySnapshot(t, s, "id", "config-v1")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			options := multipartTestOptions()
			switch mode {
			case "parts":
				options.MaxParts = 1
			case "part_bytes":
				options.MaxPartBytes = 1
			case "total_bytes":
				options.MaxTotalBytes = 1
			}
			tx, err := s.BeginMultipart(ctx, "events", options)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Abort()
			firstErr := writeMultipartTestPart(t, tx, "id", 2)
			switch mode {
			case "part_bytes", "total_bytes":
				if firstErr == nil {
					t.Fatal("byte bound ignored")
				}
			default:
				if firstErr != nil {
					t.Fatal(firstErr)
				}
				switch mode {
				case "parts":
					if _, err = tx.NewPart(); err == nil {
						t.Fatal("part bound ignored")
					}
				case "schema":
					if err = writeMultipartTestPart(t, tx, "other", 1); err == nil {
						t.Fatal("schema mismatch accepted")
					}
				case "unsealed":
					if _, err = tx.NewPart(); err != nil {
						t.Fatal(err)
					}
					if _, err = tx.Commit("config-v1"); err == nil {
						t.Fatal("unsealed committed")
					}
				case "cancel":
					cancel()
					if _, err = tx.Commit("config-v1"); !errors.Is(err, context.Canceled) {
						t.Fatalf("cancel: %v", err)
					}
				case "publish":
					concrete := tx.(*multipartTransaction)
					if err = os.Mkdir(filepath.Join(concrete.tx.dir.Name(), ".manifest-"+concrete.tx.generation+".yaml"), 0700); err != nil {
						t.Fatal(err)
					}
					if _, err = tx.Commit("config-v1"); err == nil {
						t.Fatal("failed manifest published")
					}
				}
			}
			tx.Abort()
			requireCurrentGeneration(t, s, prior.Generation)
		})
	}
}
func TestMultipartSetSchemaChecksSealedParts(t *testing.T) {
	s, _ := newTestStore(t)
	tx, err := s.BeginMultipart(context.Background(), "events", multipartTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	if err = writeMultipartTestPart(t, tx, "id", 1); err != nil {
		t.Fatal(err)
	}
	wrong := arrow.NewSchema([]arrow.Field{{Name: "other", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err = tx.SetSchema(wrong); err == nil {
		t.Fatal("changed schema accepted")
	}
}

func TestMultipartPruneResumesInterruptedRetirement(t *testing.T) {
	s, _ := newTestStore(t)
	retired := commitMultipartTest(t, s, 2, 3)
	current := commitRecoverySnapshot(t, s, "id", "config-v1")
	directory := filepath.Dir(retired.Parts[0].Path)
	tombstone := filepath.Join(directory, ".prune-"+retired.Generation+".yaml")
	if err := os.Rename(filepath.Join(directory, generationManifestName(retired.Generation)), tombstone); err != nil {
		t.Fatal(err)
	}
	dir, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err = dir.Sync(); err != nil {
		t.Fatal(err)
	}
	dir.Close()
	if err = os.Remove(retired.Parts[0].Path); err != nil {
		t.Fatal(err)
	}
	inventory, err := s.Inventory(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory) != 1 || inventory[0].Snapshot.Generation != current.Generation {
		t.Fatal("retired generation remained in inventory")
	}
	if _, err = s.Restore(context.Background(), restoreRequest(retired, current)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retired generation restored: %v", err)
	}
	if err = s.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
	for _, part := range retired.Parts {
		if _, err = os.Stat(part.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retired part retained: %v", err)
		}
	}
	if _, err = os.Stat(tombstone); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retirement tombstone retained: %v", err)
	}
	requireCurrentGeneration(t, s, current.Generation)
	if err = s.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
}

func TestMultipartPruneReclaimsInterruptedPublication(t *testing.T) {
	for _, mode := range []string{"orphan", "malformed_metadata", "pinned"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := newTestStore(t)
			current := commitRecoverySnapshot(t, s, "id", "config-v1")
			writer, err := s.BeginMultipart(context.Background(), "events", multipartTestOptions())
			if err != nil {
				t.Fatal(err)
			}
			tx := writer.(*multipartTransaction)
			if err = writeMultipartTestPart(t, writer, "id", 2); err != nil {
				t.Fatal(err)
			}
			if err = writeMultipartTestPart(t, writer, "id", 3); err != nil {
				t.Fatal(err)
			}
			orphan := filepath.Join(tx.tx.dir.Name(), tx.parts[0].Path)
			sidecar := filepath.Join(tx.tx.dir.Name(), generationManifestName(tx.tx.generation))
			// Crash halfway through final-name publication, before the sidecar exists.
			if err = storePublishPayload(tx.tx.dir, tx.stages[0], tx.parts[0].Path); err != nil {
				t.Fatal(err)
			}
			if err = writer.Abort(); err != nil {
				t.Fatal(err)
			}
			var pin *os.File
			switch mode {
			case "malformed_metadata":
				if err = os.WriteFile(sidecar, []byte("invalid: metadata\n"), 0400); err != nil {
					t.Fatal(err)
				}
			case "pinned":
				pin, err = os.Open(orphan)
				if err != nil {
					t.Fatal(err)
				}
				defer pin.Close()
				if err = storeLock(context.Background(), pin, false); err != nil {
					t.Fatal(err)
				}
			}
			err = s.Prune(context.Background(), "events", 1)
			if mode == "malformed_metadata" {
				if err == nil {
					t.Fatal("malformed metadata treated as absence")
				}
				if _, err = os.Stat(orphan); err != nil {
					t.Fatal("unsafe orphan deletion")
				}
			} else if err != nil {
				t.Fatal(err)
			} else if mode == "pinned" {
				if _, err = os.Stat(orphan); err != nil {
					t.Fatal("pinned orphan deleted")
				}
				pin.Close()
				if err = s.Prune(context.Background(), "events", 1); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "malformed_metadata" {
				if _, err = os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("orphan not reclaimed: %v", err)
				}
			}
			requireCurrentGeneration(t, s, current.Generation)
		})
	}
}

func TestMultipartSealRejectsFalseRowCount(t *testing.T) {
	s, _ := newTestStore(t)
	current := commitRecoverySnapshot(t, s, "id", "config-v1")
	data, err := os.ReadFile(current.Path)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.BeginMultipart(context.Background(), "events", multipartTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	part, err := tx.NewPart()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = part.Write(data); err != nil {
		t.Fatal(err)
	}
	if err = tx.SealPart(999); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("false rows accepted: %v", err)
	}
}

func TestMultipartVerifyRejectsForgedManifestRows(t *testing.T) {
	s, _ := newTestStore(t)
	snapshot := commitMultipartTest(t, s, 2, 3)
	path := filepath.Join(filepath.Dir(snapshot.Parts[0].Path), storeManifestName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest storeManifest
	if err = yaml.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Parts[1].Rows++
	manifest.Rows++
	manifest.SHA256 = multipartDigest(manifest.Parts)
	data, err = yaml.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0400); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Verify(context.Background(), "events"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("forged rows verified: %v", err)
	}
	tx, err := s.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	if _, err = tx.PreviousSchema(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("forged previous rows accepted: %v", err)
	}
}

func TestMultipartPruneReclaimsInitialInterruptedPublication(t *testing.T) {
	s, _ := newTestStore(t)
	writer, err := s.BeginMultipart(context.Background(), "events", multipartTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	tx := writer.(*multipartTransaction)
	if err = writeMultipartTestPart(t, writer, "id", 2); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(tx.tx.dir.Name(), tx.parts[0].Path)
	if err = storePublishPayload(tx.tx.dir, tx.stages[0], tx.parts[0].Path); err != nil {
		t.Fatal(err)
	}
	if err = writer.Abort(); err != nil {
		t.Fatal(err)
	}
	if err = s.Prune(context.Background(), "events", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty dataset status changed: %v", err)
	}
	if _, err = os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("initial orphan retained: %v", err)
	}
	if _, err = s.Status("events"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphan became current: %v", err)
	}
	snapshot := commitMultipartTest(t, s, 1, 2)
	requireCurrentGeneration(t, s, snapshot.Generation)
}
