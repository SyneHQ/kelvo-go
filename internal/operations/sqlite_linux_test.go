//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	api "github.com/SYNEHQ/kelvo-go/operations"
)

func TestSQLiteCASReopenAndIdentity(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	dir := filepath.Join(t.TempDir(), "ledger")
	identity := strings.Repeat("a", 64)
	b, err := OpenSQLite(ctx, dir, f.store.Policy(), identity)
	if err != nil {
		t.Fatal(err)
	}
	key := f.store.key(0)
	if _, err := b.Get(ctx, key); !errors.Is(err, ErrMissing) {
		t.Fatal(err)
	}
	if rev, err := b.Create(ctx, key, []byte("one")); err != nil || rev != 1 {
		t.Fatal(rev, err)
	}
	if _, err := b.Create(ctx, key, []byte("other")); !errors.Is(err, ErrRevision) {
		t.Fatal(err)
	}
	var wins int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := b.Update(ctx, key, []byte("two"), 1)
			if err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			} else if !errors.Is(err, ErrRevision) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatal("CAS winners", wins)
	}
	if _, err := OpenSQLite(ctx, dir, f.store.Policy(), identity); !errors.Is(err, ErrUnavailable) {
		t.Fatal("second process lock", err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, key); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	b, err = OpenSQLite(ctx, dir, f.store.Policy(), identity)
	if err != nil {
		t.Fatal(err)
	}
	got, err := b.Get(ctx, key)
	if err != nil || got.Revision != 2 || string(got.Value) != "two" {
		t.Fatal(got, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLite(ctx, dir, f.store.Policy(), strings.Repeat("b", 64)); !errors.Is(err, ErrConflict) {
		t.Fatal("identity change", err)
	}
	policy := f.store.Policy()
	policy.Retention += time.Hour
	if _, err := OpenSQLite(ctx, dir, policy, identity); !errors.Is(err, ErrConflict) {
		t.Fatal("policy change", err)
	}
}

func TestSQLiteRejectsUnsafeOrCorruptState(t *testing.T) {
	for _, scenario := range []string{"symlink", "permissions", "foreign-file", "corrupt", "oversize", "foreign-key"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			root := t.TempDir()
			dir := filepath.Join(root, "ledger")
			identity := strings.Repeat("a", 64)
			if scenario == "symlink" {
				if err := os.Symlink(root, dir); err != nil {
					t.Fatal(err)
				}
			} else if scenario == "permissions" {
				if err := os.Mkdir(dir, 0755); err != nil {
					t.Fatal(err)
				}
			} else {
				b, err := OpenSQLite(ctx, dir, f.store.Policy(), identity)
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "oversize" {
					if _, err := b.Create(ctx, f.store.key(0), make([]byte, MaxDocumentBytes+1)); !errors.Is(err, ErrInvalid) {
						t.Fatal(err)
					}
					_ = b.Close()
					return
				}
				if scenario == "foreign-key" {
					if _, err := b.Create(ctx, "operation.other.0000", []byte("x")); !errors.Is(err, ErrInvalid) {
						t.Fatal(err)
					}
					_ = b.Close()
					return
				}
				if err := b.Close(); err != nil {
					t.Fatal(err)
				}
				name := filepath.Join(dir, "foreign")
				raw := []byte("foreign")
				if scenario == "corrupt" {
					name = filepath.Join(dir, "operations.sqlite")
					raw = []byte("invalid database")
				}
				if err := os.WriteFile(name, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if b, err := OpenSQLite(ctx, dir, f.store.Policy(), identity); err == nil {
				_ = b.Close()
				t.Fatal("unsafe state opened")
			}
		})
	}
}

func TestSQLiteCrashNeverReplaysRunningWrite(t *testing.T) {
	const env = "KELVO_SQLITE_CRASH_FIXTURE"
	f := newFixture(t)
	if dir := os.Getenv(env); dir != "" {
		b, err := OpenSQLite(context.Background(), dir, f.store.Policy(), strings.Repeat("a", 64))
		if err != nil {
			t.Fatal(err)
		}
		f.store, err = New(b, f.store.Policy())
		if err != nil {
			t.Fatal(err)
		}
		f.store.now = func() time.Time { return f.now }
		record := f.running(t)
		fmt.Fprintln(os.Stdout, record.Record.ID)
		os.Exit(17) // Bypass Close to exercise WAL recovery.
	}
	dir := filepath.Join(t.TempDir(), "ledger")
	child := exec.Command(os.Args[0], "-test.run=^TestSQLiteCrashNeverReplaysRunningWrite$")
	child.Env = append(os.Environ(), env+"="+dir)
	raw, err := child.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 17 {
		t.Fatalf("crash fixture failed: %v %s", err, raw)
	}
	id := strings.TrimSpace(string(raw))
	b, err := OpenSQLite(context.Background(), dir, f.store.Policy(), strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	f.store, err = New(b, f.store.Policy())
	if err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(11 * time.Second)
	f.store.now = func() time.Time { return f.now }
	if err := f.store.RecoverShard(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	current, err := f.store.Get(context.Background(), f.scope, id)
	if err != nil || current.Record.Receipt == nil || current.Record.Receipt.Outcome != api.OutcomeUnknown {
		t.Fatal(current, err)
	}
	retry, duplicate, err := f.store.Submit(context.Background(), f.input)
	if err != nil || !duplicate || retry.Record.ID != id || retry.Record.State != string(api.OutcomeUnknown) {
		t.Fatal(retry, duplicate, err)
	}
}
