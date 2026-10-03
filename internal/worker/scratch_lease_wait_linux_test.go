//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bufio"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// The leader really exits while its descendant retains the passed lease. The
// caller controls the descendant's exit through EOF, independently of cleanup.
func scratchOrphanLease(t *testing.T, workspace *scratchWorkspace) *os.File {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestScratchInheritedLeaseHelper$")
	// The race runtime's default one-second exit sleep would itself outlive
	// this test's grace. Keep race detection, but let EOF control actual exit.
	command.Env = append(os.Environ(), "KELVO_SCRATCH_TEST_CHILD=leader", "GORACE=atexit_sleep_ms=0")
	workspace.attach(command)
	childInput, input, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { input.Close(); childInput.Close() })
	command.Stdin = childInput
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	childInput.Close()
	t.Cleanup(func() {
		if command.ProcessState == nil {
			command.Process.Kill()
			command.Wait()
		}
	})
	if line, err := bufio.NewReader(output).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("descendant readiness: %q %v", line, err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	return input
}

func TestScratchLeaseGraceWaitsForExitedLeaderDescendant(t *testing.T) {
	root := scratchFixture(t)
	workspace, err := root.allocate()
	if err != nil {
		t.Fatal(err)
	}
	input := scratchOrphanLease(t, workspace)
	released := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		input.Close()
		close(released)
	}()
	started := time.Now()
	if err := workspace.cleanup(); err != nil {
		t.Fatal("transient inherited reference quarantined", err)
	}
	<-released
	if elapsed := time.Since(started); elapsed < 40*time.Millisecond || elapsed >= scratchLeaseGrace {
		t.Fatal("cleanup did not wait within its bounded grace", elapsed)
	}
	if _, err := os.Stat(workspace.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("completed workspace remains", err)
	}
	if count, err := root.Reclaim(); err != nil || count != 0 {
		t.Fatal("cleanup left ownership", count, err)
	}
}

func TestScratchLeaseGracePreservesIndefinitelyLiveDescendant(t *testing.T) {
	root := scratchFixture(t)
	workspace, err := root.allocate()
	if err != nil {
		t.Fatal(err)
	}
	input := scratchOrphanLease(t, workspace)
	started := time.Now()
	err = workspace.cleanup()
	var failure *scratchCleanupError
	if !errors.As(err, &failure) || failure.stage != "child_lease_held" {
		t.Fatal("live descendant passed cleanup", err)
	}
	if elapsed := time.Since(started); elapsed < scratchLeaseGrace || elapsed > 2*scratchLeaseGrace {
		t.Fatal("contention exceeded fixed grace", elapsed)
	}
	id := strings.TrimPrefix(filepath.Base(workspace.path), scratchPrefix)
	for _, path := range []string{workspace.path, filepath.Join(root.path, scratchLeasePrefix+id)} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("uncertain ownership removed", err)
		}
	}
	input.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		count, err := root.Reclaim()
		if err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("released descendant lease did not recover")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestScratchLeaseGraceRejectsReplacedAndCorruptRecords(t *testing.T) {
	for _, kind := range []string{"replaced", "corrupt"} {
		t.Run(kind, func(t *testing.T) {
			root := scratchFixture(t)
			workspace, err := root.allocate()
			if err != nil {
				t.Fatal(err)
			}
			input := scratchOrphanLease(t, workspace)
			id := strings.TrimPrefix(filepath.Base(workspace.path), scratchPrefix)
			record := filepath.Join(root.path, scratchLeasePrefix+id)
			changed := make(chan error, 1)
			go func() {
				time.Sleep(50 * time.Millisecond)
				var err error
				if kind == "replaced" {
					err = os.Rename(record, filepath.Join(root.path, "preserved-original-record"))
					if err == nil {
						err = os.WriteFile(record, []byte(leaseRecord(id)), 0o600)
					}
				} else {
					err = os.WriteFile(record, []byte("invalid-record"), 0o600)
				}
				input.Close()
				changed <- err
			}()
			err = workspace.cleanup()
			if changedErr := <-changed; changedErr != nil {
				t.Fatal(changedErr)
			}
			if !errors.Is(err, errScratchUnsafe) {
				t.Fatal("changed lease accepted", err)
			}
			for _, path := range []string{record, workspace.path} {
				if _, err := os.Stat(path); err != nil {
					t.Fatal("changed ownership was deleted", err)
				}
			}
		})
	}
}

func TestScratchLeaseGraceDoesNotRetryNonContentionErrors(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	started := time.Now()
	waited, err := acquireScratchLease(file)
	if waited || !errors.Is(err, unix.EBADF) || time.Since(started) >= scratchLeaseGrace/2 {
		t.Fatal("non-contention failure retried", waited, err)
	}
}
