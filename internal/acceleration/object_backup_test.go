//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

func migrationRemoteFixture(t *testing.T, multipart, empty bool) (*objectBackend, *fakeSnapshotObjects, Snapshot, MigrationRequest) {
	t.Helper()
	objects := newFakeSnapshotObjects()
	backend := testObjectBackend(t, objects)
	fingerprint := strings.Repeat("a", 64)
	var snapshot Snapshot
	var err error
	if multipart {
		tx, e := backend.BeginMultipart(context.Background(), "events", remoteMultipartOptions())
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Abort()
		if e := tx.SetSchema(backupTestSchema()); e != nil {
			t.Fatal(e)
		}
		count := 2
		if empty {
			count = 1
		}
		for i := 0; i < count; i++ {
			f, e := tx.NewPart()
			if e != nil {
				t.Fatal(e)
			}
			rows := backupTestParquet(t, f, empty)
			if e := tx.SealPart(rows); e != nil {
				t.Fatal(e)
			}
		}
		snapshot, err = tx.Commit(fingerprint)
	} else {
		tx, e := backend.Begin(context.Background(), "events")
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Abort()
		if e := tx.(SchemaWriter).SetSchema(backupTestSchema()); e != nil {
			t.Fatal(e)
		}
		rows := backupTestParquet(t, tx.File(), empty)
		snapshot, err = tx.Commit(fingerprint, rows)
	}
	if err != nil {
		t.Fatal(err)
	}
	backend.writeClient = func() (objectstore.Client, bool, error) {
		t.Error("migration requested object write credentials")
		return nil, false, errors.New("writes prohibited")
	}
	request := MigrationRequest{Dataset: "events", SourceFingerprint: fingerprint, TargetFingerprint: strings.Repeat("b", 64), Destination: filepath.Join(backupTestParent(t), "migrated"), MaxBytes: snapshot.Bytes, MaxRows: 100}
	return backend, objects, snapshot, request
}

func TestObjectMigrationPreservesSingleMultipartEmptyPayloadsAndAge(t *testing.T) {
	for _, tc := range []struct {
		name             string
		multipart, empty bool
	}{{"single", false, false}, {"multipart", true, false}, {"empty single", false, true}, {"empty multipart", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			backend, objects, source, request := migrationRemoteFixture(t, tc.multipart, tc.empty)
			objects.mu.Lock()
			sequence := objects.sequence
			objects.mu.Unlock()
			result, err := backend.MigrateBackup(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			got := result.Snapshot
			if got.Generation != source.Generation || got.Fingerprint != request.TargetFingerprint || !got.RefreshedAt.Equal(source.RefreshedAt) || got.SchemaHash != source.SchemaHash || got.Rows != source.Rows || got.Bytes != source.Bytes || result.SourceFingerprint != source.Fingerprint || result.SourceSHA256 != source.SHA256 {
				t.Fatal("migration changed source data or lost provenance")
			}
			payloads, _ := objectPayloadSnapshots(source)
			for i, payload := range payloads {
				filename := source.Generation + ".parquet"
				if tc.multipart {
					filename = multipartName(source.Generation, i)
				}
				path := filepath.Join(request.Destination, "tenant-a", "events", filename)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				objects.mu.Lock()
				expected := bytes.Clone(objects.objects[payload.ObjectKey].data)
				objects.mu.Unlock()
				if !bytes.Equal(data, expected) {
					t.Fatal("migration altered Parquet payload values")
				}
				info, err := os.Stat(path)
				if err != nil || info.Mode().Perm() != 0400 {
					t.Fatal("migration file not private and immutable")
				}
			}
			objects.mu.Lock()
			if objects.sequence != sequence {
				t.Error("migration wrote remote state")
			}
			objects.objects = map[string]fakeSnapshotObject{}
			objects.unavailable = true
			objects.mu.Unlock()
			local, err := OpenStore(request.Destination, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := local.Verify(context.Background(), "events"); err != nil {
				t.Fatal("recovered store depends on remote source", err)
			}
			lease, err := local.Acquire(context.Background(), "events", request.TargetFingerprint, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			lease.Close()
			if _, err := local.Acquire(context.Background(), "events", request.SourceFingerprint, 0); !errors.Is(err, ErrFingerprintMismatch) {
				t.Fatal("source fingerprint bypassed explicit target policy")
			}
			if tc.multipart && got.SHA256 == source.SHA256 {
				t.Fatal("remote descriptor digest reused as local multipart digest")
			}
		})
	}
}

func TestObjectMigrationRejectsCorruptionBudgetsSkewAndNoOverwrite(t *testing.T) {
	for _, mode := range []string{"digest", "footer rows", "schema", "missing part", "bytes", "rows", "fingerprint", "clock skew", "existing", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			backend, objects, _, request := migrationRemoteFixture(t, mode != "digest" && mode != "footer rows" && mode != "schema", false)
			switch mode {
			case "digest":
				objects.corruptPayloadGet = true
			case "footer rows":
				objects.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) { m.Committed.Rows++ })
			case "schema":
				objects.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) { m.Committed.SchemaHash = strings.Repeat("0", 64) })
			case "missing part":
				objects.mu.Lock()
				for key := range objects.objects {
					if strings.HasSuffix(key, "-part-0001.parquet") {
						delete(objects.objects, key)
					}
				}
				objects.mu.Unlock()
			case "bytes":
				request.MaxBytes--
			case "rows":
				request.MaxRows = 1
			case "fingerprint":
				request.SourceFingerprint = strings.Repeat("c", 64)
			case "clock skew":
				objects.clockOffset = time.Hour
			case "existing":
				if err := os.Mkdir(request.Destination, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(backupTestParent(t), request.Destination); err != nil {
					t.Fatal(err)
				}
			}
			result, err := backend.MigrateBackup(context.Background(), request)
			if err == nil || result.Snapshot.Generation != "" {
				t.Fatal("invalid migration published")
			}
			if mode != "existing" && mode != "symlink" {
				backupTestMissing(t, request.Destination)
			}
			entries, _ := os.ReadDir(filepath.Dir(request.Destination))
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".kelvo-backup-") {
					t.Fatal("failed migration leaked staging")
				}
			}
		})
	}
}

func TestObjectMigrationCancellationAndPostPublicationUncertainty(t *testing.T) {
	backend, _, _, request := migrationRemoteFixture(t, false, false)
	ctx, cancel := context.WithCancel(context.Background())
	result, err := backend.migrateBackup(ctx, request, func(ctx context.Context, dst io.Writer, src io.Reader, size int64) error {
		if err := backupCopy(ctx, dst, src, size); err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	}, nil)
	if !errors.Is(err, context.Canceled) || result.Snapshot.Generation != "" {
		t.Fatal("canceled migration succeeded")
	}
	backupTestMissing(t, request.Destination)
	cause := errors.New("injected directory fsync failure")
	result, err = backend.migrateBackup(context.Background(), request, backupCopy, func(*os.File) error { return cause })
	if !errors.Is(err, cause) || result.Snapshot.Generation == "" {
		t.Fatal("uncertain publication was hidden")
	}
	local, openErr := OpenStore(request.Destination, "tenant-a")
	if openErr != nil {
		t.Fatal(openErr)
	}
	if _, err := local.Verify(context.Background(), "events"); err != nil {
		t.Fatal("published uncertain result was removed")
	}
}

func TestObjectMigrationFencesChangedAuthorizationButPinsSamePolicyRefresh(t *testing.T) {
	for _, changedPolicy := range []bool{false, true} {
		t.Run(map[bool]string{false: "same policy", true: "revoked policy"}[changedPolicy], func(t *testing.T) {
			backend, objects, source, request := migrationRemoteFixture(t, false, false)
			var once sync.Once
			copyFile := func(ctx context.Context, dst io.Writer, src io.Reader, size int64) error {
				err := backupCopy(ctx, dst, src, size)
				once.Do(func() {
					// A separate writer publishes a complete valid replacement;
					// the migration itself remains read-only.
					writer := testObjectBackend(t, objects)
					tx, writeErr := writer.Begin(context.Background(), "events")
					if writeErr != nil {
						t.Fatal(writeErr)
					}
					defer tx.Abort()
					if writeErr := tx.(SchemaWriter).SetSchema(backupTestSchema()); writeErr != nil {
						t.Fatal(writeErr)
					}
					rows := backupTestParquet(t, tx.File(), false)
					fingerprint := request.SourceFingerprint
					if changedPolicy {
						fingerprint = strings.Repeat("c", 64)
					}
					if _, writeErr := tx.Commit(fingerprint, rows); writeErr != nil {
						t.Fatal(writeErr)
					}
				})
				return err
			}
			result, err := backend.migrateBackup(context.Background(), request, copyFile, nil)
			if changedPolicy {
				if !errors.Is(err, ErrFingerprintMismatch) || result.Snapshot.Generation != "" {
					t.Fatal("changed policy published")
				}
				backupTestMissing(t, request.Destination)
			} else if err != nil || result.Snapshot.Generation != source.Generation {
				t.Fatal("copy did not retain pinned source generation", err)
			}
		})
	}
}

func TestObjectMigrationRetainsStaleness(t *testing.T) {
	backend, objects, _, request := migrationRemoteFixture(t, false, false)
	old := time.Now().Add(-2 * time.Hour).UTC()
	objects.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) { m.Committed.RefreshedAt = old })
	result, err := backend.MigrateBackup(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Snapshot.RefreshedAt.Equal(old) {
		t.Fatal("migration refreshed old data")
	}
	local, err := OpenStore(request.Destination, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.Acquire(context.Background(), "events", request.TargetFingerprint, time.Hour); !errors.Is(err, ErrStale) {
		t.Fatal("old remote data became fresh")
	}
}

type migrationVersionChange struct {
	objectstore.Client
	objects *fakeSnapshotObjects
	once    sync.Once
}

func (c *migrationVersionChange) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	if strings.HasSuffix(key, ".parquet") {
		c.once.Do(func() {
			c.objects.mu.Lock()
			defer c.objects.mu.Unlock()
			changed := c.objects.objects[key]
			changed.info.Version = "changed-after-head"
			c.objects.objects[key] = changed
		})
	}
	return c.Client.Get(ctx, key, version)
}

func TestObjectMigrationPinsVersionAcrossMetadataAndPayloadReads(t *testing.T) {
	backend, objects, _, request := migrationRemoteFixture(t, false, false)
	backend.reader = &migrationVersionChange{Client: objects, objects: objects}
	result, err := backend.MigrateBackup(context.Background(), request)
	if !errors.Is(err, objectstore.ErrConflict) || result.Snapshot.Generation != "" {
		t.Fatal("changed object version was accepted", err)
	}
	backupTestMissing(t, request.Destination)
}
