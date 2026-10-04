//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exports

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestCustodyCrashReleasesOnlyProcessLock(t *testing.T) {
	if directory := os.Getenv("KELVO_TEST_CUSTODY_CHILD"); directory != "" {
		c, err := OpenCustody(context.Background(), directory, "tenant", "worker")
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(c.ID())
		for {
			time.Sleep(time.Hour)
		}
	}
	directory := filepath.Join(t.TempDir(), "runtime")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCustodyCrashReleasesOnlyProcessLock$")
	child.Env = append(os.Environ(), "KELVO_TEST_CUSTODY_CHILD="+directory)
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	scanner := bufio.NewScanner(output)
	if !scanner.Scan() {
		t.Fatal("child did not acquire custody")
	}
	id := scanner.Text()
	if !idPattern.MatchString(id) {
		t.Fatal("invalid child custody identity", id)
	}
	locked, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	if other, err := OpenCustody(locked, directory, "tenant", "worker"); !errors.Is(err, context.DeadlineExceeded) || other != nil {
		stop()
		t.Fatal("live process custody stolen", err)
	}
	stop()
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = child.Wait(); err == nil {
		t.Fatal("crash control exited successfully")
	}
	reopened, err := OpenCustody(context.Background(), directory, "tenant", "worker")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.ID() != id {
		t.Fatal("crash changed persisted identity")
	}
}

func TestCustodyPersistsIdentityAndExcludesConcurrentRuntime(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "runtime")
	c, err := OpenCustody(context.Background(), dir, "tenant", "worker")
	if err != nil {
		t.Fatal(err)
	}
	id := c.ID()
	if !idPattern.MatchString(id) {
		t.Fatal("invalid persistent identity")
	}
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	if other, err := OpenCustody(ctx, dir, "tenant", "worker"); !errors.Is(err, context.DeadlineExceeded) || other != nil {
		t.Fatalf("second owner: %v", err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCustody(context.Background(), dir, "tenant", "worker")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.ID() != id || reopened.DataDirectory() != filepath.Join(dir, "data") {
		t.Fatal("restart changed custody")
	}
}

func TestCustodyRejectsForeignAuthorityAndNeverAdoptsData(t *testing.T) {
	for _, test := range []struct{ name, tenant, worker string }{
		{"tenant", "other", "worker"}, {"worker", "tenant", "other"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "runtime")
			c, err := OpenCustody(context.Background(), dir, "tenant", "worker")
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := OpenCustody(context.Background(), dir, test.tenant, test.worker); !errors.Is(err, ErrCorrupt) || got != nil {
				t.Fatalf("foreign custody accepted: %v", err)
			}
		})
	}
	dir := filepath.Join(t.TempDir(), "runtime")
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0700); err != nil {
		t.Fatal(err)
	}
	if got, err := OpenCustody(context.Background(), dir, "tenant", "worker"); !errors.Is(err, ErrCorrupt) || got != nil {
		t.Fatalf("orphan data adopted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "data")); err != nil {
		t.Fatal("orphan data was removed")
	}
}

func TestCustodyRejectsUnsafeMetadataAndPreservesUnknownFiles(t *testing.T) {
	for _, kind := range []string{"unknown", "symlink", "hardlink", "writable", "unknown-version"} {
		t.Run(kind, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "runtime")
			c, err := OpenCustody(context.Background(), dir, "tenant", "worker")
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Close(); err != nil {
				t.Fatal(err)
			}
			metadata := filepath.Join(dir, "custody.yml")
			switch kind {
			case "unknown":
				err = os.WriteFile(filepath.Join(dir, "unowned"), []byte("keep"), 0600)
			case "symlink":
				if err = os.Rename(metadata, filepath.Join(t.TempDir(), "metadata")); err != nil {
					t.Fatal(err)
				}
				err = os.Symlink("missing", metadata)
			case "hardlink":
				err = os.Link(metadata, filepath.Join(t.TempDir(), "linked"))
			case "writable":
				err = os.Chmod(metadata, 0600)
			case "unknown-version":
				if err = os.Remove(metadata); err != nil {
					t.Fatal(err)
				}
				err = os.WriteFile(metadata, []byte("version: 2\ntenant: tenant\nworker: worker\nid: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"), 0400)
			}
			if err != nil {
				t.Fatal(err)
			}
			if got, err := OpenCustody(context.Background(), dir, "tenant", "worker"); err == nil || got != nil {
				t.Fatal("unsafe custody accepted")
			}
			if kind == "unknown" {
				if raw, err := os.ReadFile(filepath.Join(dir, "unowned")); err != nil || string(raw) != "keep" {
					t.Fatal("unknown file modified")
				}
			}
		})
	}
}
