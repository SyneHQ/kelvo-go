//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	// macOS commonly places the temporary directory beneath the /var symlink.
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir = filepath.Join(dir, "snapshots")
	store, err := OpenStore(dir, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	return store, dir
}

func commitTestSnapshot(t *testing.T, store *Store, payload string) Snapshot {
	t.Helper()
	tx, err := store.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	if _, err := io.WriteString(tx.File(), payload); err != nil {
		t.Fatal(err)
	}
	snapshot, err := tx.Commit("config-v1", 7)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func requireCurrentGeneration(t *testing.T, store *Store, generation string) {
	t.Helper()
	snapshot, err := store.Verify(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Generation != generation {
		t.Fatalf("current generation = %q, want %q", snapshot.Generation, generation)
	}
}

func TestStorePersistsImmutableGenerationAndTenantIsolation(t *testing.T) {
	store, dir := newTestStore(t)
	payload := "PAR1initial payloadPAR1"
	snapshot := commitTestSnapshot(t, store, payload)
	digest := sha256.Sum256([]byte(payload))
	if snapshot.SHA256 != hex.EncodeToString(digest[:]) || snapshot.Bytes != int64(len(payload)) || snapshot.Rows != 7 {
		t.Fatalf("unexpected snapshot metadata: %+v", snapshot)
	}
	if snapshot.RefreshedAt.IsZero() || !filepath.IsAbs(snapshot.Path) {
		t.Fatalf("incomplete snapshot: %+v", snapshot)
	}
	info, err := os.Stat(snapshot.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0400 {
		t.Fatalf("payload permissions = %o", info.Mode().Perm())
	}
	restarted, err := OpenStore(dir, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := restarted.Acquire(context.Background(), "events", "config-v1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	got, err := os.ReadFile(lease.Snapshot.Path)
	if err != nil || string(got) != payload {
		t.Fatalf("persisted payload = %q, error = %v", got, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	otherTenant, err := OpenStore(dir, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := otherTenant.Status("events"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tenant crossed namespace: %v", err)
	}
	other := commitTestSnapshot(t, otherTenant, "other tenant")
	if other.Path == snapshot.Path {
		t.Fatal("tenant paths overlap")
	}
	requireCurrentGeneration(t, store, snapshot.Generation)
}

func TestStoreFailedRefreshPreservesPreviousSnapshot(t *testing.T) {
	for _, failure := range []string{"abort", "canceled", "closed-file", "manifest-write", "invalid-metadata", "generation-collision"} {
		t.Run(failure, func(t *testing.T) {
			store, _ := newTestStore(t)
			initial := commitTestSnapshot(t, store, "last good version")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tx, err := store.Begin(ctx, "events")
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Abort()
			if _, err := io.WriteString(tx.File(), "partial new version"); err != nil {
				t.Fatal(err)
			}
			fingerprint := "config-v2"
			switch failure {
			case "abort":
				err = tx.Abort()
			case "canceled":
				cancel()
			case "closed-file":
				if err := tx.File().Close(); err != nil {
					t.Fatal(err)
				}
			case "manifest-write":
				if err := os.Mkdir(filepath.Join(tx.dir.Name(), ".manifest-"+tx.generation+".yaml"), 0700); err != nil {
					t.Fatal(err)
				}
			case "invalid-metadata":
				fingerprint = ""
			case "generation-collision":
				if err := os.WriteFile(filepath.Join(tx.dir.Name(), tx.generation+".parquet"), []byte("existing"), 0400); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "abort" {
				if err != nil {
					t.Fatal(err)
				}
			} else if _, err := tx.Commit(fingerprint, 2); err == nil {
				t.Fatal("refresh unexpectedly succeeded")
			}
			if err := tx.Abort(); err != nil {
				t.Fatalf("Abort after completion: %v", err)
			}
			requireCurrentGeneration(t, store, initial.Generation)
			entries, err := os.ReadDir(filepath.Dir(initial.Path))
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".stage-") {
					t.Fatalf("staging file leaked: %s", entry.Name())
				}
			}
			// Every failure must also release the writer, including Commit failures.
			nextCtx, nextCancel := context.WithTimeout(context.Background(), time.Second)
			defer nextCancel()
			// A deliberately injected manifest directory remains suspicious and
			// should not be removed by stage recovery; remove the test fixture.
			if failure == "manifest-write" {
				if err := os.Remove(filepath.Join(filepath.Dir(initial.Path), ".manifest-"+tx.generation+".yaml")); err != nil {
					t.Fatal(err)
				}
			}
			next, err := store.Begin(nextCtx, "events")
			if err != nil {
				t.Fatalf("writer was not released: %v", err)
			}
			if err := next.Abort(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStoreWriterLockCancellation(t *testing.T) {
	store, dir := newTestStore(t)
	first, err := store.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Abort()
	second, err := OpenStore(dir, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	if _, err := second.Begin(ctx, "events"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writer lock did not wait with cancellation: %v", err)
	}
	if err := first.Abort(); err != nil {
		t.Fatal(err)
	}
	next, err := second.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreLeasesSurviveRefreshAndPrune(t *testing.T) {
	store, _ := newTestStore(t)
	first := commitTestSnapshot(t, store, "first")
	lease, err := store.Acquire(context.Background(), "events", "config-v1", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	second := commitTestSnapshot(t, store, "second")
	third := commitTestSnapshot(t, store, "third")
	if err := store.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(lease.Snapshot.Path); err != nil || string(got) != "first" {
		t.Fatalf("leased generation deleted: %q %v", got, err)
	}
	if _, err := os.Stat(second.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unleased old generation was not collected: %v", err)
	}
	requireCurrentGeneration(t, store, third.Generation)
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(first.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released generation was not collected: %v", err)
	}
}

func TestStoreConcurrentAcquireRefreshAndPrune(t *testing.T) {
	store, _ := newTestStore(t)
	commitTestSnapshot(t, store, "payload-0")
	stop := make(chan struct{})
	errorsFound := make(chan error, 4)
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				lease, err := store.Acquire(context.Background(), "events", "config-v1", 0)
				if err != nil {
					errorsFound <- err
					return
				}
				runtime.Gosched()
				payload, err := os.ReadFile(lease.Snapshot.Path)
				_ = lease.Close()
				if err != nil {
					errorsFound <- err
					return
				}
				if !strings.HasPrefix(string(payload), "payload-") {
					errorsFound <- fmt.Errorf("partial payload: %q", payload)
					return
				}
			}
		}()
	}
	for i := 1; i <= 20; i++ {
		commitTestSnapshot(t, store, fmt.Sprintf("payload-%d", i))
		if err := store.Prune(context.Background(), "events", 2); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	readers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
}

func TestStorePolicyAndExplicitIntegrityAudit(t *testing.T) {
	store, _ := newTestStore(t)
	snapshot := commitTestSnapshot(t, store, "original")
	if _, err := store.Acquire(context.Background(), "events", "other-config", 0); !errors.Is(err, ErrFingerprintMismatch) {
		t.Fatalf("fingerprint: %v", err)
	}
	if _, err := store.Acquire(context.Background(), "events", "config-v1", time.Nanosecond); !errors.Is(err, ErrStale) {
		t.Fatalf("staleness: %v", err)
	}
	if _, err := store.Acquire(context.Background(), "events", "config-v1", -1); err == nil {
		t.Fatal("negative max age accepted")
	}
	if err := os.Chmod(snapshot.Path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapshot.Path, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(snapshot.Path, 0400); err != nil {
		t.Fatal(err)
	}
	// The regular query path deliberately avoids hashing the whole snapshot.
	lease, err := store.Acquire(context.Background(), "events", "config-v1", 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = lease.Close()
	if _, err := store.Verify(context.Background(), "events"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("audit did not detect tampering: %v", err)
	}
}

func TestStoreRejectsInvalidNamespaces(t *testing.T) {
	store, dir := newTestStore(t)
	for _, tenant := range []string{"", "../other", "Tenant", "a/b", strings.Repeat("a", 33)} {
		if _, err := OpenStore(dir, tenant); err == nil {
			t.Errorf("tenant %q accepted", tenant)
		}
	}
	for _, dataset := range []string{"", "../other", "a/b", "a-b", "1table", strings.Repeat("a", 64)} {
		if tx, err := store.Begin(context.Background(), dataset); err == nil {
			_ = tx.Abort()
			t.Errorf("dataset %q accepted", dataset)
		}
	}
	public := filepath.Join(dir, "public")
	if err := os.Mkdir(public, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(public, "tenant"); err == nil {
		t.Fatal("non-private directory accepted")
	}
	link := filepath.Join(dir, "linked")
	if err := os.Symlink(filepath.Join(dir, "tenant-a"), link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(link, "tenant"); err == nil {
		t.Fatal("symlink store root accepted")
	}
	if _, err := OpenStore(dir, "linked"); err == nil {
		t.Fatal("symlink tenant namespace accepted")
	}
	if err := os.Symlink(dir, filepath.Join(dir, "tenant-a", "events")); err != nil {
		t.Fatal(err)
	}
	if tx, err := store.Begin(context.Background(), "events"); err == nil {
		_ = tx.Abort()
		t.Fatal("symlink dataset namespace accepted")
	}
}

func TestStoreRejectsUnsafeAndMissingFiles(t *testing.T) {
	for _, kind := range []string{"payload-symlink", "manifest-symlink", "payload-hardlink", "payload-writable", "payload-missing", "payload-size", "manifest-writable", "manifest-oversized", "manifest-traversal", "manifest-unknown", "manifest-multiple-documents", "manifest-dataset"} {
		t.Run(kind, func(t *testing.T) {
			store, dir := newTestStore(t)
			snapshot := commitTestSnapshot(t, store, "snapshot")
			manifestPath := filepath.Join(filepath.Dir(snapshot.Path), storeManifestName)
			switch kind {
			case "payload-symlink", "manifest-symlink":
				path := snapshot.Path
				if kind == "manifest-symlink" {
					path = manifestPath
				}
				target := filepath.Join(dir, "outside")
				if err := os.Rename(path, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "payload-hardlink":
				if err := os.Link(snapshot.Path, filepath.Join(dir, "outside")); err != nil {
					t.Fatal(err)
				}
			case "payload-writable":
				if err := os.Chmod(snapshot.Path, 0600); err != nil {
					t.Fatal(err)
				}
			case "payload-missing":
				if err := os.Remove(snapshot.Path); err != nil {
					t.Fatal(err)
				}
			case "payload-size":
				if err := os.Chmod(snapshot.Path, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(snapshot.Path, 1); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(snapshot.Path, 0400); err != nil {
					t.Fatal(err)
				}
			case "manifest-writable":
				if err := os.Chmod(manifestPath, 0600); err != nil {
					t.Fatal(err)
				}
			default:
				data, err := os.ReadFile(manifestPath)
				if err != nil {
					t.Fatal(err)
				}
				var manifest storeManifest
				if err := yaml.Unmarshal(data, &manifest); err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "manifest-oversized":
					data = []byte(strings.Repeat("x", storeManifestLimit+1))
				case "manifest-traversal":
					manifest.Generation = "../../outside"
					data, err = yaml.Marshal(manifest)
				case "manifest-unknown":
					data = append(data, []byte("path: /outside\n")...)
				case "manifest-multiple-documents":
					data = append(data, []byte("---\nextra: true\n")...)
				case "manifest-dataset":
					manifest.Dataset = "other"
					data, err = yaml.Marshal(manifest)
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(manifestPath, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(manifestPath, data, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(manifestPath, 0400); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.Status("events"); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("unsafe snapshot was accepted: %v", err)
			}
		})
	}
}

func TestStorePruneFailsSafeWithoutHealthyCurrent(t *testing.T) {
	store, _ := newTestStore(t)
	old := commitTestSnapshot(t, store, "old")
	current := commitTestSnapshot(t, store, "new")
	if err := os.Remove(current.Path); err != nil {
		t.Fatal(err)
	}
	if err := store.Prune(context.Background(), "events", 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("prune ignored missing current generation: %v", err)
	}
	if _, err := os.Stat(old.Path); err != nil {
		t.Fatalf("last recoverable generation was deleted: %v", err)
	}
}

// This test also serves as a subprocess worker. flock correctness must be
// demonstrated across OS processes, not only separate Store instances.
func TestStoreProcessHelper(t *testing.T) {
	mode := os.Getenv("KELVO_STORE_TEST_MODE")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	store, err := OpenStore(os.Getenv("KELVO_STORE_TEST_DIR"), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if mode == "lease" {
		lease, err := store.Acquire(context.Background(), "events", "config-v1", 0)
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Close()
	} else {
		tx, err := store.Begin(context.Background(), "events")
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Abort()
		if _, err := io.WriteString(tx.File(), "abandoned partial snapshot"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fmt.Fprintln(os.Stdout, "STORE_READY"); err != nil {
		t.Fatal(err)
	}
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
}

func startStoreHelper(t *testing.T, dir, mode string) (*exec.Cmd, io.WriteCloser) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestStoreProcessHelper$")
	cmd.Env = append(os.Environ(), "KELVO_STORE_TEST_MODE="+mode, "KELVO_STORE_TEST_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "STORE_READY\n" {
		t.Fatalf("worker readiness = %q, %v", line, err)
	}
	return cmd, stdin
}

func TestStoreProcessWriterLockAndCrashRecovery(t *testing.T) {
	store, dir := newTestStore(t)
	initial := commitTestSnapshot(t, store, "last good snapshot")
	worker, _ := startStoreHelper(t, dir, "writer")
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	if _, err := store.Begin(ctx, "events"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cross-process writer lock failed: %v", err)
	}
	if err := worker.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = worker.Wait()
	restarted, err := OpenStore(dir, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	next, err := restarted.Begin(context.Background(), "events")
	if err != nil {
		t.Fatalf("crashed writer lock was not released: %v", err)
	}
	if err := next.Abort(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Dir(initial.Path))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".stage-") {
			t.Fatalf("abandoned stage survived recovery: %s", entry.Name())
		}
	}
	requireCurrentGeneration(t, restarted, initial.Generation)
}

func TestStoreProcessLeaseBlocksPruning(t *testing.T) {
	store, dir := newTestStore(t)
	old := commitTestSnapshot(t, store, "leased across processes")
	worker, stdin := startStoreHelper(t, dir, "lease")
	commitTestSnapshot(t, store, "new generation")
	if err := store.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old.Path); err != nil {
		t.Fatalf("leased payload pruned: %v", err)
	}
	if _, err := io.WriteString(stdin, "release\n"); err != nil {
		t.Fatal(err)
	}
	if err := worker.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := store.Prune(context.Background(), "events", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released payload retained: %v", err)
	}
}
