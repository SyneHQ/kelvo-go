//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authstate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture did not reach its checkpoint")
	}
}

func waitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("bounded operation did not return")
		return nil
	}
}

func assertLockHeld(t *testing.T, directory string) {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(directory, lockName), os.O_RDWR, 0)
	requireOK(t, err)
	defer file.Close()
	err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
		t.Fatal("lifetime writer lock was released early", err)
	}
}

func waitReopen(t *testing.T, directory string) *Store {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		store, err := Open(context.Background(), directory, testScope())
		if err == nil {
			t.Cleanup(func() { _ = store.Close(context.Background()) })
			return store
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not autonomously release ownership", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStorePublicationFailuresPoisonAndPreserveRecoverableFloor(t *testing.T) {
	for _, stage := range []string{"short-write", "staging-sync", "rename", "directory-sync", "old-state-close"} {
		t.Run(stage, func(t *testing.T) {
			directory, initial := initialized(t)
			operations := defaultIO()
			var enabled atomic.Bool
			injected := errors.New("private fixture failure")
			operations.write = func(file *os.File, raw []byte) (int, error) {
				if enabled.Load() && stage == "short-write" {
					return file.Write(raw[:len(raw)-1])
				}
				return file.Write(raw)
			}
			operations.sync = func(file *os.File) error {
				if enabled.Load() && stage == "staging-sync" && file.Name() == pendingName {
					return injected
				}
				if enabled.Load() && stage == "directory-sync" {
					info, err := file.Stat()
					if err != nil {
						return err
					}
					if info.IsDir() {
						return injected
					}
				}
				return file.Sync()
			}
			operations.rename = func(dir *os.File, from, to string) error {
				if enabled.Load() && stage == "rename" {
					return injected
				}
				return unix.Renameat(int(dir.Fd()), from, int(dir.Fd()), to)
			}
			operations.close = func(file *os.File) error {
				err := file.Close()
				if enabled.Load() && stage == "old-state-close" && file.Name() == stateName {
					return injected
				}
				return err
			}
			store, err := openWithIO(context.Background(), directory, testScope(), nil, operations)
			requireOK(t, err)
			t.Cleanup(func() { _ = store.Close(context.Background()) })
			enabled.Store(true)
			next := testCandidate(2, "retained", "next")
			if err := store.Admit(context.Background(), next); err != ErrUncertain {
				t.Fatal("publication failure not fenced", err)
			}
			if err := store.Admit(context.Background(), initial); err != ErrUncertain {
				t.Fatal("failed store recovered authority", err)
			}
			if err := store.Close(context.Background()); err != ErrUncertain {
				t.Fatal("poisoned shutdown lost uncertainty", err)
			}
			reopened := waitReopen(t, directory)
			if stage == "directory-sync" || stage == "old-state-close" {
				if err := reopened.Admit(context.Background(), initial); err != ErrRejected {
					t.Fatal("visible newer floor was lost", err)
				}
				requireOK(t, reopened.Admit(context.Background(), next))
			} else {
				requireOK(t, reopened.Admit(context.Background(), initial))
				requireOK(t, reopened.Admit(context.Background(), next))
			}
		})
	}
}

func TestStoreOpenSyncFailuresReturnNoAuthority(t *testing.T) {
	for _, stage := range []string{"state", "directory"} {
		t.Run(stage, func(t *testing.T) {
			directory, initial := initialized(t)
			operations := defaultIO()
			operations.sync = func(file *os.File) error {
				info, err := file.Stat()
				if err != nil {
					return err
				}
				if (stage == "state" && file.Name() == stateName) || (stage == "directory" && info.IsDir()) {
					return errors.New("fixture sync failure")
				}
				return file.Sync()
			}
			if store, err := openWithIO(context.Background(), directory, testScope(), nil, operations); store != nil || err != ErrUncertain {
				t.Fatal("unsynced startup returned authority", err)
			}
			store := waitReopen(t, directory)
			requireOK(t, store.Admit(context.Background(), initial))
		})
	}
}

func TestStoreBlockedAdmissionRetainsOwnershipUntilQuiescence(t *testing.T) {
	directory, initial := initialized(t)
	entered, released := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	defer release()
	operations := defaultIO()
	var writes atomic.Int32
	operations.write = func(file *os.File, raw []byte) (int, error) {
		writes.Add(1)
		close(entered)
		<-released
		return file.Write(raw)
	}
	store, err := openWithIO(context.Background(), directory, testScope(), nil, operations)
	requireOK(t, err)
	t.Cleanup(func() { release(); _ = store.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	next := testCandidate(2, "retained", "next")
	result := make(chan error, 1)
	go func() { result <- store.Admit(ctx, next) }()
	waitSignal(t, entered)
	if err := store.Admit(context.Background(), initial); err != ErrRejected {
		t.Fatal("overlap was not rejected", err)
	}
	if err := waitResult(t, result); err != ErrUncertain {
		t.Fatal("blocked admission retained authority", err)
	}
	for range 20 {
		if err := store.Admit(context.Background(), initial); err != ErrUncertain {
			t.Fatal("late admission restarted I/O", err)
		}
	}
	assertLockHeld(t, directory)
	if other, err := Open(context.Background(), directory, testScope()); other != nil || err != ErrUnavailable {
		t.Fatal("second owner entered blocked I/O", err)
	}
	closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer closeCancel()
	if err := store.Close(closeContext); err != ErrUncertain {
		t.Fatal("blocked shutdown was not bounded", err)
	}
	assertLockHeld(t, directory)
	if writes.Load() != 1 {
		t.Fatal("replacement I/O workers started")
	}
	release()
	if err := store.Close(context.Background()); err != ErrUncertain {
		t.Fatal("late completion unpoisoned the store", err)
	}
	// Closing the old Store again must never close a descriptor reused by Go.
	probe, err := os.Create(filepath.Join(t.TempDir(), "unrelated"))
	requireOK(t, err)
	defer probe.Close()
	for range 20 {
		_ = store.Close(context.Background())
	}
	_, err = probe.WriteString("still open")
	requireOK(t, err)
	reopened := waitReopen(t, directory)
	if err := reopened.Admit(context.Background(), initial); err != ErrRejected {
		t.Fatal("late durable floor was discarded", err)
	}
	requireOK(t, reopened.Admit(context.Background(), next))
}

func TestStoreBlockedStartupCleansUpWithoutReturnedStore(t *testing.T) {
	for _, initialize := range []bool{false, true} {
		name := "open"
		if initialize {
			name = "initialize"
		}
		t.Run(name, func(t *testing.T) {
			var directory string
			initial := testCandidate(1, "retired", "retained")
			if initialize {
				directory = filepath.Join(t.TempDir(), "state")
			} else {
				directory, _ = initialized(t)
			}
			entered, released := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(released) }) }
			defer release()
			operations := defaultIO()
			operations.checkpoint = func(stage string) {
				if (initialize && stage == "before-write") || (!initialize && stage == "opened-durable") {
					close(entered)
					<-released
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				var candidate *Candidate
				if initialize {
					candidate = &initial
				}
				store, err := openWithIO(ctx, directory, testScope(), candidate, operations)
				if store != nil {
					_ = store.Close(context.Background())
					result <- errors.New("startup returned a store after expiry")
					return
				}
				result <- err
			}()
			waitSignal(t, entered)
			if err := waitResult(t, result); err != ErrUncertain {
				t.Fatal("blocked startup not fenced", err)
			}
			assertLockHeld(t, directory)
			release()
			store := waitReopen(t, directory)
			requireOK(t, store.Admit(context.Background(), initial))
		})
	}
}

func TestStoreBlockedCloseRetainsLock(t *testing.T) {
	directory, _ := initialized(t)
	entered, released := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	defer release()
	operations := defaultIO()
	operations.close = func(file *os.File) error {
		if file.Name() == lockName {
			close(entered)
			<-released
		}
		return file.Close()
	}
	store, err := openWithIO(context.Background(), directory, testScope(), nil, operations)
	requireOK(t, err)
	t.Cleanup(func() { release(); _ = store.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- store.Close(ctx) }()
	waitSignal(t, entered)
	if err := waitResult(t, result); err != ErrUncertain {
		t.Fatal("close did not respect caller bound", err)
	}
	assertLockHeld(t, directory)
	release()
	if err := store.Close(context.Background()); err != ErrUncertain {
		t.Fatal("timed out close lost uncertainty", err)
	}
	waitReopen(t, directory)
}

type delayedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (ctx delayedDeadlineContext) Deadline() (time.Time, bool) { return ctx.deadline, true }

func TestStoreDeadlineFencedWithoutCancellationDelivery(t *testing.T) {
	expired := delayedDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Second)}
	if store, err := Open(expired, filepath.Join(t.TempDir(), "absent"), testScope()); store != nil || err != ErrUncertain {
		t.Fatal("expired constructor accepted", err)
	}
	directory, initial := initialized(t)
	store := openTest(t, directory)
	if err := store.Admit(expired, initial); err != ErrUncertain {
		t.Fatal("past deadline admitted with nil Context.Err", err)
	}
	if err := store.Admit(context.Background(), initial); err != ErrUncertain {
		t.Fatal("expired admission failed to poison")
	}

	directory, _ = initialized(t)
	entered, released := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	defer release()
	operations := defaultIO()
	operations.write = func(file *os.File, raw []byte) (int, error) { close(entered); <-released; return file.Write(raw) }
	store, err := openWithIO(context.Background(), directory, testScope(), nil, operations)
	requireOK(t, err)
	t.Cleanup(func() { release(); _ = store.Close(context.Background()) })
	ctx := delayedDeadlineContext{Context: context.Background(), deadline: time.Now().Add(100 * time.Millisecond)}
	result := make(chan error, 1)
	go func() { result <- store.Admit(ctx, testCandidate(2, "next")) }()
	waitSignal(t, entered)
	if err := waitResult(t, result); err != ErrUncertain {
		t.Fatal("delayed Done bypassed deadline", err)
	}
	if ctx.Err() != nil {
		t.Fatal("fixture unexpectedly delivered cancellation")
	}
	assertLockHeld(t, directory)
	release()
	if err := store.Close(context.Background()); err != ErrUncertain {
		t.Fatal("late completion restored authority", err)
	}
}
