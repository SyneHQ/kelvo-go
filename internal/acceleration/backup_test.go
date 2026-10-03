//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.yaml.in/yaml/v3"
)

func backupTestParent(t *testing.T) string {
	t.Helper()
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(parent, 0700); err != nil {
		t.Fatal(err)
	}
	return parent
}
func backupTestRequest(t *testing.T) BackupRequest {
	t.Helper()
	return BackupRequest{Dataset: "events", Fingerprint: "config-v1", Destination: filepath.Join(backupTestParent(t), "backup-root"), MaxBytes: 4 << 20}
}
func backupTestSchema() *arrow.Schema {
	metadata := arrow.MetadataFrom(map[string]string{"meaning": "nullable boundary values"})
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: metadata}, {Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true}}, &metadata)
}
func backupTestParquet(t *testing.T, out io.Writer, empty bool) int64 {
	t.Helper()
	schema := backupTestSchema()
	sink := NewParquetSink(out, query.DefaultLimits())
	defer sink.Abort()
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	var rows int64
	if !empty {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer builder.Release()
		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{-9223372036854775808, 0, 9223372036854775807}, []bool{true, false, true})
		payload := builder.Field(1).(*array.BinaryBuilder)
		payload.Append([]byte{0, 255, 128})
		payload.AppendNull()
		payload.Append([]byte{})
		batch := builder.NewRecordBatch()
		defer batch.Release()
		if err := sink.Write(batch); err != nil {
			t.Fatal(err)
		}
		rows = 3
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	return rows
}
func backupTestCommit(t *testing.T, store *Store, multipart, empty, legacy bool) Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if multipart {
		writer, err := store.BeginMultipart(ctx, "events", multipartTestOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Abort()
		count := 2
		if empty {
			count = 1
		}
		for range count {
			file, err := writer.NewPart()
			if err != nil {
				t.Fatal(err)
			}
			rows := backupTestParquet(t, file, empty)
			if err = writer.SealPart(rows); err != nil {
				t.Fatal(err)
			}
		}
		if err = writer.SetSchema(backupTestSchema()); err != nil {
			t.Fatal(err)
		}
		snapshot, err := writer.Commit("config-v1")
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	writer, err := store.Begin(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Abort()
	rows := backupTestParquet(t, writer.File(), empty)
	if !legacy {
		if err = writer.SetSchema(backupTestSchema()); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := writer.Commit("config-v1", rows)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
func backupTestParts(snapshot Snapshot) []SnapshotPart {
	if len(snapshot.Parts) != 0 {
		return snapshot.Parts
	}
	return []SnapshotPart{{Path: snapshot.Path, Rows: snapshot.Rows, Bytes: snapshot.Bytes, SHA256: snapshot.SHA256}}
}
func backupTestIdentity(t *testing.T, source, destination Snapshot) {
	t.Helper()
	if source.Dataset != destination.Dataset || source.Generation != destination.Generation || source.Fingerprint != destination.Fingerprint || source.SHA256 != destination.SHA256 || source.SchemaHash != destination.SchemaHash || source.Rows != destination.Rows || source.Bytes != destination.Bytes || !source.RefreshedAt.Equal(destination.RefreshedAt) || len(source.Parts) != len(destination.Parts) {
		t.Fatal("backup changed immutable snapshot identity")
	}
	oldParts, newParts := backupTestParts(source), backupTestParts(destination)
	for i, old := range oldParts {
		next := newParts[i]
		if old.Path == next.Path || filepath.Base(old.Path) != filepath.Base(next.Path) || old.Rows != next.Rows || old.Bytes != next.Bytes || old.SHA256 != next.SHA256 {
			t.Fatal("backup part descriptor changed")
		}
		original, err := os.ReadFile(old.Path)
		if err != nil {
			t.Fatal(err)
		}
		copied, err := os.ReadFile(next.Path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(original, copied) {
			t.Fatal("backup changed Parquet bytes including NULL/binary/empty values")
		}
		oldInfo, err := os.Stat(old.Path)
		if err != nil {
			t.Fatal(err)
		}
		newInfo, err := os.Stat(next.Path)
		if err != nil {
			t.Fatal(err)
		}
		if os.SameFile(oldInfo, newInfo) || newInfo.Mode().Perm() != 0400 {
			t.Fatal("backup is linked or writable")
		}
	}
}
func backupTestMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed backup published destination: %v", err)
	}
}

func TestBackupPreservesVerifiedSingleMultipartEmptyAndLegacy(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		multipart, empty, legacy bool
	}{{"single_nulls", false, false, false}, {"single_empty", false, true, false}, {"legacy_schema", false, false, true}, {"multipart_nulls", true, false, false}, {"multipart_empty", true, true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			store, sourceRoot := newTestStore(t)
			initial := backupTestCommit(t, store, tc.multipart, tc.empty, tc.legacy)
			request := backupTestRequest(t)
			request.MaxBytes = initial.Bytes // Exact payload budget is sufficient.
			rawManifest, err := os.ReadFile(filepath.Join(sourceRoot, "tenant-a", "events", storeManifestName))
			if err != nil {
				t.Fatal(err)
			}
			copied, err := store.Backup(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			backupTestIdentity(t, initial, copied)
			rawCopied, err := os.ReadFile(filepath.Join(request.Destination, "tenant-a", "events", storeManifestName))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(rawManifest, rawCopied) {
				t.Fatal("backup rewrote raw manifest metadata")
			}
			destination, err := OpenStore(request.Destination, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			verified, err := destination.Verify(context.Background(), "events")
			if err != nil {
				t.Fatal(err)
			}
			backupTestIdentity(t, initial, verified)
			for _, directory := range []string{request.Destination, filepath.Join(request.Destination, "tenant-a"), filepath.Join(request.Destination, "tenant-a", "events")} {
				info, err := os.Stat(directory)
				if err != nil || info.Mode().Perm() != 0700 {
					t.Fatal("backup namespace is not private")
				}
			}
			if unauthorized, acquireErr := destination.Acquire(context.Background(), "events", "wrong-authorization", 0); acquireErr == nil {
				_ = unauthorized.Close()
				t.Fatal("backup bypassed authorization fingerprint")
			}
			current, err := store.Verify(context.Background(), "events")
			if err != nil || current.Generation != initial.Generation {
				t.Fatal("backup mutated source")
			}
		})
	}
}
func TestBackupRejectsAuthorizationBudgetsAndInvalidRequests(t *testing.T) {
	for _, tc := range []string{"wrong_fingerprint", "empty_fingerprint", "unknown_dataset", "zero_budget", "negative_budget", "oversized_budget", "insufficient_budget", "relative_destination", "missing_parent"} {
		t.Run(tc, func(t *testing.T) {
			store, _ := newTestStore(t)
			snapshot := backupTestCommit(t, store, false, false, false)
			request := backupTestRequest(t)
			switch tc {
			case "wrong_fingerprint":
				request.Fingerprint = "revoked"
			case "empty_fingerprint":
				request.Fingerprint = ""
			case "unknown_dataset":
				request.Dataset = "missing"
			case "zero_budget":
				request.MaxBytes = 0
			case "negative_budget":
				request.MaxBytes = -1
			case "oversized_budget":
				request.MaxBytes = (1 << 40) + 1
			case "insufficient_budget":
				request.MaxBytes = snapshot.Bytes - 1
			case "relative_destination":
				request.Destination = "relative-backup"
			case "missing_parent":
				request.Destination = filepath.Join(filepath.Dir(request.Destination), "missing", "backup")
			}
			if _, err := store.Backup(context.Background(), request); err == nil {
				t.Fatal("invalid backup request accepted")
			}
			if filepath.IsAbs(request.Destination) {
				backupTestMissing(t, request.Destination)
			}
		})
	}
}
func TestBackupDestinationNeverOverwritesOrTraversesLinks(t *testing.T) {
	for _, scenario := range []string{"empty_directory", "nonempty_directory", "file", "symlink", "dangling_symlink", "symlink_parent", "public_parent"} {
		t.Run(scenario, func(t *testing.T) {
			store, _ := newTestStore(t)
			backupTestCommit(t, store, false, false, false)
			request := backupTestRequest(t)
			parent := filepath.Dir(request.Destination)
			sentinel := filepath.Join(parent, "sentinel")
			if err := os.WriteFile(sentinel, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "empty_directory", "nonempty_directory":
				if err := os.Mkdir(request.Destination, 0700); err != nil {
					t.Fatal(err)
				}
				if scenario == "nonempty_directory" {
					if err := os.WriteFile(filepath.Join(request.Destination, "keep"), []byte("keep"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "file":
				if err := os.WriteFile(request.Destination, []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(sentinel, request.Destination); err != nil {
					t.Fatal(err)
				}
			case "dangling_symlink":
				if err := os.Symlink(filepath.Join(parent, "absent"), request.Destination); err != nil {
					t.Fatal(err)
				}
			case "symlink_parent":
				link := filepath.Join(parent, "alias")
				if err := os.Symlink(parent, link); err != nil {
					t.Fatal(err)
				}
				request.Destination = filepath.Join(link, "new-root")
			case "public_parent":
				if err := os.Chmod(parent, 0755); err != nil {
					t.Fatal(err)
				}
			}
			before, beforeErr := os.Lstat(request.Destination)
			if _, err := store.Backup(context.Background(), request); err == nil {
				t.Fatal("unsafe destination accepted")
			}
			data, err := os.ReadFile(sentinel)
			if err != nil || string(data) != "keep" {
				t.Fatal("destination rejection changed unrelated file")
			}
			after, afterErr := os.Lstat(request.Destination)
			if beforeErr == nil {
				if afterErr != nil || !os.SameFile(before, after) {
					t.Fatal("existing destination replaced")
				}
			} else if !errors.Is(afterErr, os.ErrNotExist) {
				t.Fatal("unsafe destination created")
			}
			if scenario == "nonempty_directory" {
				data, err := os.ReadFile(filepath.Join(request.Destination, "keep"))
				if err != nil || string(data) != "keep" {
					t.Fatal("existing backup contents removed")
				}
			}
		})
	}
}

func TestBackupRejectsCorruptManifestRowsSchemaFooterAndLinks(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		for _, scenario := range []string{"rows", "schema", "footer", "checksum", "unknown_manifest_field", "payload_symlink", "payload_hardlink"} {
			layout := "single"
			if multipart {
				layout = "multipart"
			}
			t.Run(layout+"/"+scenario, func(t *testing.T) {
				store, root := newTestStore(t)
				snapshot := backupTestCommit(t, store, multipart, false, false)
				request := backupTestRequest(t)
				manifestPath := filepath.Join(root, "tenant-a", "events", storeManifestName)
				data, err := os.ReadFile(manifestPath)
				if err != nil {
					t.Fatal(err)
				}
				var manifest storeManifest
				if err = yaml.Unmarshal(data, &manifest); err != nil {
					t.Fatal(err)
				}
				payload := backupTestParts(snapshot)[0].Path
				switch scenario {
				case "rows":
					manifest.Rows++
					if multipart {
						manifest.Parts[0].Rows++
						manifest.SHA256 = multipartDigest(manifest.Parts)
					}
				case "schema":
					manifest.SchemaHash = strings.Repeat("f", 64)
				case "footer", "checksum":
					content, err := os.ReadFile(payload)
					if err != nil {
						t.Fatal(err)
					}
					content[len(content)-1] ^= 1
					if err = os.Chmod(payload, 0600); err != nil {
						t.Fatal(err)
					}
					if err = os.WriteFile(payload, content, 0600); err != nil {
						t.Fatal(err)
					}
					if err = os.Chmod(payload, 0400); err != nil {
						t.Fatal(err)
					}
					if scenario == "footer" {
						digest := fmt.Sprintf("%x", sha256.Sum256(content))
						if multipart {
							manifest.Parts[0].SHA256 = digest
							manifest.SHA256 = multipartDigest(manifest.Parts)
						} else {
							manifest.SHA256 = digest
						}
					}
				case "payload_symlink":
					target := filepath.Join(filepath.Dir(payload), "original-payload")
					if err = os.Rename(payload, target); err != nil {
						t.Fatal(err)
					}
					if err = os.Symlink(target, payload); err != nil {
						t.Fatal(err)
					}
				case "payload_hardlink":
					if err = os.Link(payload, filepath.Join(filepath.Dir(payload), "extra-link")); err != nil {
						t.Fatal(err)
					}
				}
				data, err = yaml.Marshal(manifest)
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "unknown_manifest_field" {
					data = append(data, []byte("unexpected: fixture\n")...)
				}
				if err = os.Chmod(manifestPath, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(manifestPath, data, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.Chmod(manifestPath, 0400); err != nil {
					t.Fatal(err)
				}
				// Keep the sidecar consistent so payload corruption scenarios reach
				// hash/footer/schema verification instead of sidecar rejection.
				sidecar := filepath.Join(filepath.Dir(manifestPath), generationManifestName(manifest.Generation))
				if err = os.Chmod(sidecar, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(sidecar, data, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.Chmod(sidecar, 0400); err != nil {
					t.Fatal(err)
				}
				if _, err = store.Backup(context.Background(), request); err == nil {
					t.Fatal("corrupt source backed up as verified")
				}
				backupTestMissing(t, request.Destination)
			})
		}
	}
}

type backupShortWriter struct{}

func (backupShortWriter) Write(data []byte) (int, error) { return len(data) - 1, nil }

type backupCancelWriter struct {
	cancel context.CancelFunc
	bytes.Buffer
}

func (w *backupCancelWriter) Write(data []byte) (int, error) {
	n, err := w.Buffer.Write(data)
	w.cancel()
	return n, err
}
func TestBackupCopyExactLengthShortWriteAndCancellation(t *testing.T) {
	payload := bytes.Repeat([]byte{0, 255, 128}, 200000)
	var out bytes.Buffer
	if err := backupCopy(context.Background(), &out, bytes.NewReader(payload), int64(len(payload))); err != nil || !bytes.Equal(payload, out.Bytes()) {
		t.Fatalf("bounded copy changed bytes: %v", err)
	}
	for _, size := range []int64{int64(len(payload) - 1), int64(len(payload) + 1)} {
		if err := backupCopy(context.Background(), io.Discard, bytes.NewReader(payload), size); err == nil {
			t.Fatal("inexact source length accepted")
		}
	}
	if err := backupCopy(context.Background(), backupShortWriter{}, bytes.NewReader(payload), int64(len(payload))); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	writer := &backupCancelWriter{cancel: cancel}
	defer cancel()
	if err := backupCopy(ctx, writer, bytes.NewReader(payload), int64(len(payload))); !errors.Is(err, context.Canceled) {
		t.Fatalf("copy ignored cancellation: %v", err)
	}
	if writer.Len() >= len(payload) {
		t.Fatal("cancelled copy consumed the entire payload")
	}
}

func TestBackupFailedCopyLeavesNoPublishedRootAndPreservesCrashSibling(t *testing.T) {
	for _, scenario := range []string{"short_write", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			store, _ := newTestStore(t)
			snapshot := backupTestCommit(t, store, true, false, false)
			request := backupTestRequest(t)
			sibling := filepath.Join(filepath.Dir(request.Destination), ".kelvo-backup-crash-leftover")
			if err := os.Mkdir(sibling, 0700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(sibling, "keep")
			if err := os.WriteFile(marker, []byte("private orphan"), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := store.backup(ctx, request, func(ctx context.Context, dst io.Writer, src io.Reader, size int64) error {
				if scenario == "cancelled" {
					cancel()
					return backupCopy(ctx, dst, src, size)
				}
				return backupCopy(ctx, backupShortWriter{}, src, size)
			})
			if err == nil {
				t.Fatal("failed copy reported backup success")
			}
			backupTestMissing(t, request.Destination)
			data, err := os.ReadFile(marker)
			if err != nil || string(data) != "private orphan" {
				t.Fatal("failed backup removed unrelated crash leftover")
			}
			verified, err := store.Verify(context.Background(), "events")
			if err != nil || verified.Generation != snapshot.Generation {
				t.Fatal("failed backup changed source")
			}
		})
	}
}

func TestBackupPinsCapturedGenerationAcrossRefreshAndPrune(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		layout := "single"
		if multipart {
			layout = "multipart"
		}
		t.Run(layout, func(t *testing.T) {
			store, _ := newTestStore(t)
			initial := backupTestCommit(t, store, multipart, false, false)
			request := backupTestRequest(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			started, release := make(chan struct{}), make(chan struct{})
			var once, sent sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			type result struct {
				snapshot Snapshot
				err      error
			}
			done := make(chan result, 1)
			go func() {
				snapshot, err := store.backup(ctx, request, func(ctx context.Context, dst io.Writer, src io.Reader, size int64) error {
					sent.Do(func() { close(started) })
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
					return backupCopy(ctx, dst, src, size)
				})
				done <- result{snapshot, err}
			}()
			select {
			case <-started:
			case failed := <-done:
				t.Fatalf("backup failed before pinning: %v", failed.err)
			case <-ctx.Done():
				t.Fatal("backup copy never started")
			}
			next := backupTestCommit(t, store, multipart, false, false)
			if err := store.Prune(ctx, "events", 1); err != nil {
				t.Fatal(err)
			}
			for _, part := range backupTestParts(initial) {
				if _, err := os.Stat(part.Path); err != nil {
					t.Fatal("prune removed backup-pinned payload")
				}
			}
			unblock()
			var copied result
			select {
			case copied = <-done:
			case <-ctx.Done():
				t.Fatal("backup did not finish")
			}
			if copied.err != nil {
				t.Fatal(copied.err)
			}
			backupTestIdentity(t, initial, copied.snapshot)
			current, err := store.Verify(ctx, "events")
			if err != nil || current.Generation != next.Generation {
				t.Fatal("backup reverted concurrent publication")
			}
			if err = store.Prune(ctx, "events", 1); err != nil {
				t.Fatal(err)
			}
			for _, part := range backupTestParts(initial) {
				if _, err = os.Stat(part.Path); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("backup did not release generation pin")
				}
			}
		})
	}
}

func TestBackupPublicationNeverReplacesConcurrentDestination(t *testing.T) {
	store, _ := newTestStore(t)
	initial := backupTestCommit(t, store, true, false, false)
	request := backupTestRequest(t)
	var once sync.Once
	var existing os.FileInfo
	_, err := store.backup(context.Background(), request, func(ctx context.Context, dst io.Writer, src io.Reader, size int64) error {
		var setupErr error
		once.Do(func() {
			setupErr = os.Mkdir(request.Destination, 0700)
			if setupErr != nil {
				return
			}
			existing, setupErr = os.Stat(request.Destination)
			if setupErr != nil {
				return
			}
			setupErr = os.WriteFile(filepath.Join(request.Destination, "keep"), []byte("concurrent owner"), 0600)
		})
		if setupErr != nil {
			return setupErr
		}
		return backupCopy(ctx, dst, src, size)
	})
	if err == nil {
		t.Fatal("backup replaced concurrently created destination")
	}
	info, statErr := os.Stat(request.Destination)
	if statErr != nil || existing == nil || !os.SameFile(existing, info) {
		t.Fatal("concurrent destination inode changed")
	}
	marker, readErr := os.ReadFile(filepath.Join(request.Destination, "keep"))
	if readErr != nil || string(marker) != "concurrent owner" {
		t.Fatal("concurrent destination data changed")
	}
	entries, readErr := os.ReadDir(filepath.Dir(request.Destination))
	if readErr != nil || len(entries) != 1 {
		t.Fatalf("failed backup leaked staging entries: %v (%d)", readErr, len(entries))
	}
	current, verifyErr := store.Verify(context.Background(), "events")
	if verifyErr != nil || current.Generation != initial.Generation {
		t.Fatal("failed publication changed source")
	}
}

func TestBackupRejectsConflictingImmutableGenerationManifest(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		t.Run(fmt.Sprintf("multipart_%t", multipart), func(t *testing.T) {
			store, root := newTestStore(t)
			initial := backupTestCommit(t, store, multipart, false, false)
			request := backupTestRequest(t)
			sidecar := filepath.Join(root, "tenant-a", "events", generationManifestName(initial.Generation))
			original, err := os.ReadFile(sidecar)
			if err != nil {
				t.Fatal(err)
			}
			var manifest storeManifest
			if err = yaml.Unmarshal(original, &manifest); err != nil {
				t.Fatal(err)
			}
			manifest.Fingerprint = "other-catalog-identity"
			conflicting, err := yaml.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.Chmod(sidecar, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(sidecar, conflicting, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.Chmod(sidecar, 0400); err != nil {
				t.Fatal(err)
			}
			if _, err = store.Backup(context.Background(), request); err == nil {
				t.Fatal("conflicting immutable generation identity accepted")
			}
			backupTestMissing(t, request.Destination)
			after, err := os.ReadFile(sidecar)
			if err != nil || !bytes.Equal(after, conflicting) {
				t.Fatal("backup rewrote conflicting source metadata")
			}
		})
	}
}

func TestBackupPostPublicationDurabilityErrorRetainsVerifiedDestination(t *testing.T) {
	store, _ := newTestStore(t)
	initial := backupTestCommit(t, store, true, false, false)
	request := backupTestRequest(t)
	failure := errors.New("injected directory sync failure")
	calls := 0
	copied, err := store.backupWithSync(context.Background(), request, backupCopy, func(parent *os.File) error {
		calls++
		info, statErr := os.Stat(request.Destination)
		if statErr != nil || !info.IsDir() {
			t.Fatal("durability callback invoked before publication")
		}
		if parent.Name() != filepath.Dir(request.Destination) {
			t.Fatal("durability callback received incorrect parent")
		}
		return failure
	})
	if calls != 1 || !errors.Is(err, failure) || copied.Generation == "" {
		t.Fatalf("publication durability failure lost outcome: calls=%d snapshot=%+v err=%v", calls, copied, err)
	}
	backupTestIdentity(t, initial, copied)
	recovered, openErr := OpenStore(request.Destination, "tenant-a")
	if openErr != nil {
		t.Fatal(openErr)
	}
	verified, verifyErr := recovered.Verify(context.Background(), "events")
	if verifyErr != nil {
		t.Fatal(verifyErr)
	}
	backupTestIdentity(t, initial, verified)
	current, verifyErr := store.Verify(context.Background(), "events")
	if verifyErr != nil || current.Generation != initial.Generation {
		t.Fatal("durability failure changed source")
	}
	entries, readErr := os.ReadDir(filepath.Dir(request.Destination))
	if readErr != nil || len(entries) != 1 {
		t.Fatalf("publication left unexpected staging directories: %v (%d)", readErr, len(entries))
	}
	if _, retryErr := store.Backup(context.Background(), request); retryErr == nil {
		t.Fatal("retry replaced published destination after durability failure")
	}
}
