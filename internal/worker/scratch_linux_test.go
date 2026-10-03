//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func scratchFixture(t *testing.T) *ScratchRoot {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := OpenScratchRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	return root
}

// Simulate losing only the parent's description. Deliberately do not invoke
// workspace.cleanup: a killed parent cannot execute a deferred function.
func abandonScratch(t *testing.T, root *ScratchRoot, workspace *scratchWorkspace) {
	t.Helper()
	if err := workspace.lease.Close(); err != nil {
		t.Fatal(err)
	}
	root.mu.Lock()
	root.active--
	root.mu.Unlock()
}

func TestScratchPrivateRootAndSymlinkAncestors(t *testing.T) {
	parent := t.TempDir()
	private := filepath.Join(parent, "private")
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(private, alias); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"relative", "/", alias, alias + "/../private"} {
		if root, err := OpenScratchRoot(path); err == nil {
			root.Close()
			t.Fatalf("accepted unsafe root %q", path)
		}
	}
	if err := os.Chmod(private, 0o755); err != nil {
		t.Fatal(err)
	}
	if root, err := OpenScratchRoot(private); err == nil {
		root.Close()
		t.Fatal("accepted non-private root")
	}
}

func TestScratchLiveWorkspaceCannotBeReclaimed(t *testing.T) {
	root := scratchFixture(t)
	workspace, err := root.allocate()
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.cleanup()
	other, err := OpenScratchRoot(root.path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if count, err := other.Reclaim(); err != nil || count != 0 {
		t.Fatalf("live reclaim = %d, %v", count, err)
	}
	if _, err := os.Stat(workspace.path); err != nil {
		t.Fatal("live workspace removed", err)
	}
	if err := root.Close(); err == nil {
		t.Fatal("closed root with active executor")
	}
}

func TestScratchInheritedLeaseHelper(t *testing.T) {
	mode := os.Getenv("KELVO_SCRATCH_TEST_CHILD")
	if mode == "" {
		return
	}
	file := os.NewFile(3, "inherited-lease")
	if _, err := file.Stat(); err != nil {
		os.Exit(3)
	}
	if mode == "leader" {
		child := exec.Command(os.Args[0], "-test.run=^TestScratchInheritedLeaseHelper$")
		child.Env = append(os.Environ(), "KELVO_SCRATCH_TEST_CHILD=1")
		child.ExtraFiles = []*os.File{file}
		child.Stdin, child.Stdout, child.Stderr = os.Stdin, os.Stdout, os.Stderr
		if child.Start() != nil {
			os.Exit(4)
		}
		os.Exit(0)
	}
	fmt.Println("ready")
	var b [1]byte
	_, _ = os.Stdin.Read(b[:])
	os.Exit(0)
}

func TestScratchChildKeepsLeaseAfterParentDescriptionCloses(t *testing.T) {
	root := scratchFixture(t)
	workspace, err := root.allocate()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestScratchInheritedLeaseHelper$")
	cmd.Env = append(os.Environ(), "KELVO_SCRATCH_TEST_CHILD=1")
	workspace.attach(cmd)
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		input.Close()
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	line, err := bufio.NewReader(output).ReadString('\n')
	if err != nil || line != "ready\n" {
		t.Fatalf("child ready = %q, %v", line, err)
	}
	abandonScratch(t, root, workspace)
	other, err := OpenScratchRoot(root.path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if count, err := other.Reclaim(); err != nil || count != 0 {
		t.Fatalf("inherited lease lost: %d %v", count, err)
	}
	if _, err := os.Stat(workspace.path); err != nil {
		t.Fatal("child workspace removed")
	}
	input.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if count, err := other.Reclaim(); err != nil || count != 1 {
		t.Fatalf("orphan not reclaimed: %d %v", count, err)
	}
	if _, err := os.Stat(workspace.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("orphan workspace remains")
	}
}

func TestScratchReclaimDoesNotFollowNestedSymlink(t *testing.T) {
	root := scratchFixture(t)
	workspace, err := root.allocate()
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "preserve")
	if err := os.WriteFile(outside, []byte("untouched"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Dir(outside), filepath.Join(workspace.path, "outside")); err != nil {
		t.Fatal(err)
	}
	abandonScratch(t, root, workspace)
	if count, err := root.Reclaim(); err != nil || count != 1 {
		t.Fatalf("reclaim = %d %v", count, err)
	}
	if body, err := os.ReadFile(outside); err != nil || string(body) != "untouched" {
		t.Fatal("followed nested symlink")
	}
}

func TestScratchUnknownAndCorruptOwnershipIsPreserved(t *testing.T) {
	for _, kind := range []string{"missing", "partial", "hardlink", "symlink", "directory_symlink"} {
		t.Run(kind, func(t *testing.T) {
			root := scratchFixture(t)
			workspace, err := root.allocate()
			if err != nil {
				t.Fatal(err)
			}
			abandonScratch(t, root, workspace)
			id := filepath.Base(workspace.path)[len(scratchPrefix):]
			record := filepath.Join(root.path, scratchLeasePrefix+id)
			switch kind {
			case "missing":
				err = os.Remove(record)
			case "partial":
				err = os.WriteFile(record, []byte("partial"), 0o600)
			case "hardlink":
				err = os.Link(record, filepath.Join(root.path, "other"))
			case "symlink":
				if err = os.Rename(record, record+".other"); err == nil {
					err = os.Symlink(record+".other", record)
				}
			case "directory_symlink":
				if err = os.Remove(workspace.path); err == nil {
					err = os.Symlink(t.TempDir(), workspace.path)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = root.Reclaim(); err == nil {
				t.Fatal("accepted corrupt ownership")
			}
			if _, err := os.Lstat(workspace.path); err != nil {
				t.Fatal("unknown workspace removed")
			}
		})
	}
}

func TestScratchRootReplacementFailsClosed(t *testing.T) {
	root := scratchFixture(t)
	workspace, err := root.allocate()
	if err != nil {
		t.Fatal(err)
	}
	abandonScratch(t, root, workspace)
	old := root.path + "-moved"
	if err := os.Rename(root.path, old); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(old) })
	if err := os.Mkdir(root.path, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Reclaim(); err == nil {
		t.Fatal("accepted replaced root")
	}
	if _, err := os.Stat(filepath.Join(old, filepath.Base(workspace.path))); err != nil {
		t.Fatal("moved data removed")
	}
}

func TestScratchMutationBusyFailsWithoutDeletion(t *testing.T) {
	root := scratchFixture(t)
	lock, err := os.OpenFile(filepath.Join(root.path, scratchMutationName), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Reclaim(); err == nil {
		t.Fatal("ignored root maintenance lock")
	}
}

func TestScratchConcurrentAllocationAndCleanup(t *testing.T) {
	root := scratchFixture(t)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workspace, err := root.allocate()
			if err != nil {
				t.Error(err)
				return
			}
			if err := os.WriteFile(filepath.Join(workspace.path, "data"), []byte("temporary"), 0o600); err != nil {
				t.Error(err)
			}
			if err := workspace.cleanup(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if count, err := root.Reclaim(); err != nil || count != 0 {
		t.Fatalf("cleanup = %d %v", count, err)
	}
}

func TestScratchInventoryIsBoundedAndUnknownFilesPreserved(t *testing.T) {
	root := scratchFixture(t)
	for i := range scratchInventoryLimit {
		if err := os.WriteFile(filepath.Join(root.path, fmt.Sprintf("unknown-%d", i)), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := root.Reclaim(); err == nil {
		t.Fatal("unbounded inventory accepted")
	}
	if _, err := os.Stat(filepath.Join(root.path, "unknown-0")); err != nil {
		t.Fatal("unknown file removed")
	}
}

func TestScratchNormalCleanupPreservesDescendantLeaseAfterLeaderExit(t *testing.T) {
	root := scratchFixture(t)
	workspace, err := root.allocate()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestScratchInheritedLeaseHelper$")
	cmd.Env = append(os.Environ(), "KELVO_SCRATCH_TEST_CHILD=leader")
	workspace.attach(cmd)
	childInput, input, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdin = childInput
	defer childInput.Close()
	defer input.Close()
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	childInput.Close()
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	if line, err := bufio.NewReader(output).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("descendant ready = %q %v", line, err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := workspace.cleanup(); err == nil {
		t.Fatal("cleanup accepted live descendant lease")
	}
	if _, err := os.Stat(workspace.path); err != nil {
		t.Fatal("cleanup deleted descendant workspace")
	}
	if count, err := root.Reclaim(); err != nil || count != 0 {
		t.Fatalf("live descendant reclaim = %d %v", count, err)
	}
	input.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		count, err := root.Reclaim()
		if err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("exited descendant workspace was not reclaimable")
}

func TestScratchUntrustedWritableAncestorRejected(t *testing.T) {
	ancestor := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(ancestor, 0o700); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(ancestor, "private")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(ancestor, 0o777); err != nil {
		t.Fatal(err)
	}
	if owned, err := OpenScratchRoot(root); err == nil {
		owned.Close()
		t.Fatal("accepted world-writable ancestor")
	}
	if err := os.Chmod(ancestor, 0o770); err != nil {
		t.Fatal(err)
	}
	if owned, err := OpenScratchRoot(root); err == nil {
		owned.Close()
		t.Fatal("accepted group-writable ancestor")
	}
}
