//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authstate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/sys/unix"
)

func initialized(t *testing.T) (string, Candidate) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "state")
	initial := testCandidate(1, "retired", "retained")
	if err := Initialize(context.Background(), directory, testScope(), initial); err != nil {
		t.Fatal(err)
	}
	return directory, initial
}

func openTest(t *testing.T, directory string) *Store {
	t.Helper()
	store, err := Open(context.Background(), directory, testScope())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	return store
}

func requireOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestStoreRestartPreservesRetiredOwnershipAndRevision(t *testing.T) {
	directory, initial := initialized(t)
	store := openTest(t, directory)
	second := testCandidate(2, "retained")
	if err := store.Admit(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	store = openTest(t, directory)
	if err := store.Admit(context.Background(), initial); err != ErrRejected {
		t.Fatal("rollback accepted", err)
	}
	if err := store.Admit(context.Background(), testCandidate(2, "different")); err != ErrRejected {
		t.Fatal("equivocation accepted", err)
	}
	for _, owner := range []Owner{{TenantID: "b", PrincipalID: "analyst"}, {TenantID: "a", PrincipalID: "other"}, {TenantID: "a"}} {
		candidate := testCandidate(3, "retired")
		candidate.Owners[sha256.Sum256([]byte("retired"))] = owner
		if err := store.Admit(context.Background(), candidate); err != ErrRejected {
			t.Fatal("retired owner changed after restart", err)
		}
	}
	if err := store.Admit(context.Background(), second); err != nil {
		t.Fatal("ordinary rejection poisoned valid state", err)
	}
	raw, err := os.ReadFile(filepath.Join(directory, stateName))
	if err != nil || bytes.Contains(raw, []byte("retired")) || bytes.Contains(raw, []byte("retained")) {
		t.Fatal("raw token persisted", err)
	}
}

func TestStoreNoopDoesNotWriteAndOpenReestablishesDurability(t *testing.T) {
	directory, initial := initialized(t)
	operations := defaultIO()
	var syncs, writes atomic.Int32
	operations.sync = func(file *os.File) error { syncs.Add(1); return file.Sync() }
	operations.write = func(file *os.File, raw []byte) (int, error) { writes.Add(1); return file.Write(raw) }
	store, err := openWithIO(context.Background(), directory, testScope(), nil, operations)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	if syncs.Load() < 2 {
		t.Fatal("loaded state and directory were not synced")
	}
	before := syncs.Load()
	for range 20 {
		if err := store.Admit(context.Background(), initial); err != nil {
			t.Fatal(err)
		}
	}
	if writes.Load() != 0 || syncs.Load() != before {
		t.Fatal("unchanged reload wrote state")
	}
}

func TestStoreStartupNeverResetsOrAdoptsStaging(t *testing.T) {
	for _, mode := range []string{"missing-state", "missing-lock", "corrupt", "foreign", "pending-without-state", "unsafe-pending"} {
		t.Run(mode, func(t *testing.T) {
			directory, initial := initialized(t)
			state := filepath.Join(directory, stateName)
			switch mode {
			case "missing-state":
				requireOK(t, os.Remove(state))
			case "missing-lock":
				requireOK(t, os.Remove(filepath.Join(directory, lockName)))
			case "corrupt":
				requireOK(t, os.WriteFile(state, []byte("invalid\n"), 0600))
			case "foreign":
				raw, err := encodeState(Scope{ID: "foreign", Tenants: []string{"a", "b"}}, initial)
				requireOK(t, err)
				requireOK(t, os.WriteFile(state, raw, 0600))
			case "pending-without-state":
				requireOK(t, os.Rename(state, filepath.Join(directory, pendingName)))
			case "unsafe-pending":
				requireOK(t, os.Symlink(state, filepath.Join(directory, pendingName)))
			}
			if store, err := Open(context.Background(), directory, testScope()); err != ErrUnavailable {
				if store != nil {
					_ = store.Close(context.Background())
				}
				t.Fatal("invalid startup accepted", err)
			}
			if mode == "missing-lock" {
				if _, err := os.Lstat(filepath.Join(directory, lockName)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("Open recreated missing lock")
				}
			}
			if mode == "missing-state" {
				if err := Initialize(context.Background(), directory, testScope(), testCandidate(1, "replacement")); err != ErrUnavailable {
					t.Fatal("existing lock permitted history reset", err)
				}
				if _, err := os.Lstat(state); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("initializer recreated lost history", err)
				}
			}
		})
	}
	directory, initial := initialized(t)
	if err := os.WriteFile(filepath.Join(directory, pendingName), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	store := openTest(t, directory)
	if _, err := os.Lstat(filepath.Join(directory, pendingName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stale staging was not retired", err)
	}
	if err := store.Admit(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	if err := Initialize(context.Background(), directory, testScope(), testCandidate(9, "other")); err != ErrUnavailable {
		t.Fatal("initializer overwrote state", err)
	}
}

func TestStoreRejectsUnsafeFilesystemObjectsAndReplacement(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "fifo", "mode", "directory", "directory-mode", "ancestor-symlink"} {
		t.Run(kind, func(t *testing.T) {
			directory, _ := initialized(t)
			state := filepath.Join(directory, stateName)
			switch kind {
			case "symlink":
				requireOK(t, os.Rename(state, state+".saved"))
				requireOK(t, os.Symlink(state+".saved", state))
			case "hardlink":
				requireOK(t, os.Link(state, state+".linked"))
			case "fifo":
				requireOK(t, os.Remove(state))
				requireOK(t, unix.Mkfifo(state, 0600))
			case "mode":
				requireOK(t, os.Chmod(state, 0640))
			case "directory":
				requireOK(t, os.Remove(state))
				requireOK(t, os.Mkdir(state, 0700))
			case "directory-mode":
				requireOK(t, os.Chmod(directory, 0750))
			case "ancestor-symlink":
				alias := filepath.Join(filepath.Dir(directory), "alias")
				requireOK(t, os.Symlink(directory, alias))
				directory = alias
			}
			if store, err := Open(context.Background(), directory, testScope()); err != ErrUnavailable {
				if store != nil {
					_ = store.Close(context.Background())
				}
				t.Fatal("unsafe object accepted", err)
			}
		})
	}
	for _, target := range []string{"lock", "state", "directory"} {
		t.Run("replace-"+target, func(t *testing.T) {
			directory, _ := initialized(t)
			store := openTest(t, directory)
			switch target {
			case "lock":
				requireOK(t, os.Rename(filepath.Join(directory, lockName), filepath.Join(directory, "old-lock")))
				requireOK(t, os.WriteFile(filepath.Join(directory, lockName), nil, 0600))
			case "state":
				raw, err := os.ReadFile(filepath.Join(directory, stateName))
				requireOK(t, err)
				requireOK(t, os.Rename(filepath.Join(directory, stateName), filepath.Join(directory, "old-state")))
				requireOK(t, os.WriteFile(filepath.Join(directory, stateName), raw, 0600))
			case "directory":
				requireOK(t, os.Rename(directory, directory+".old"))
				requireOK(t, os.Mkdir(directory, 0700))
			}
			if err := store.Admit(context.Background(), testCandidate(2, "new")); err != ErrUncertain {
				t.Fatal("replacement not fenced", err)
			}
		})
	}
}

func TestStoreInitializationRequiresSafeExistingParents(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "missing", "state")
	if err := Initialize(context.Background(), directory, testScope(), testCandidate(1)); err != ErrUnavailable {
		t.Fatal("created missing ancestry", err)
	}
	for _, path := range []string{"relative", "/", "/tmp/../state", "/tmp/state\x00suffix", strings.Repeat("x", 4097)} {
		if _, err := Open(context.Background(), path, testScope()); err != ErrUnavailable {
			t.Fatal("invalid path accepted", err)
		}
	}
}
