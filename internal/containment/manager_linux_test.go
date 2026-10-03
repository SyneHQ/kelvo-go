//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testGroup struct {
	killErr, populationErr, usageErr, removeErr error
	populations                                 []bool
	removed, closed                             bool
	kills                                       int
}

func (g *testGroup) attach(*exec.Cmd) error { return nil }
func (g *testGroup) kill() error            { g.kills++; return g.killErr }
func (g *testGroup) populated() (bool, error) {
	if g.populationErr != nil {
		return false, g.populationErr
	}
	if len(g.populations) == 0 {
		return false, nil
	}
	value := g.populations[0]
	if len(g.populations) > 1 {
		g.populations = g.populations[1:]
	}
	return value, nil
}
func (g *testGroup) usage() (Usage, error) {
	return Usage{MemoryPeakBytes: 123456, OOMKills: 1}, g.usageErr
}
func (g *testGroup) remove() error {
	if g.removeErr != nil {
		return g.removeErr
	}
	g.removed = true
	return nil
}
func (g *testGroup) close() error { g.closed = true; return nil }

func jobFixture(t *testing.T, g *testGroup, release func()) (*Manager, *Job) {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	state, err := openTree(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.close() })
	name := groupPrefix + "0123456789abcdef0123456789abcdef"
	m := &Manager{config: Config{MaxGroups: 8, CleanupTimeout: 100 * time.Millisecond}, state: state, jobs: map[string]*Job{}, quarantined: map[string]bool{}}
	j := &Job{manager: m, name: name, group: g, release: release}
	m.jobs[name] = j
	return m, j
}

func TestFinishReleasesOnlyAfterVerifiedEmptyAndCleanup(t *testing.T) {
	g := &testGroup{populations: []bool{true, false}}
	var released atomic.Int32
	m, j := jobFixture(t, g, func() {
		if !g.removed || !g.closed {
			t.Error("released before group cleanup")
		}
		released.Add(1)
	})
	usage, err := j.Finish(context.Background())
	if err != nil || usage.MemoryPeakBytes != 123456 || released.Load() != 1 || m.Status().Active != 0 {
		t.Fatal("completion failed", usage, err)
	}
	if _, err := j.Finish(context.Background()); err != nil || released.Load() != 1 {
		t.Fatal("duplicate completion released twice")
	}
}

func TestCleanupFailuresQuarantineAndRetainCustody(t *testing.T) {
	for _, kind := range []string{"kill", "observation", "populated", "usage", "remove"} {
		t.Run(kind, func(t *testing.T) {
			g := &testGroup{}
			switch kind {
			case "kill":
				g.killErr = errors.New("injected")
			case "observation":
				g.populationErr = errors.New("injected")
			case "populated":
				g.populations = []bool{true}
			case "usage":
				g.usageErr = errors.New("injected")
			case "remove":
				g.removeErr = errors.New("injected")
			}
			var released, notified atomic.Int32
			custody, _ := NewCustody(func() { released.Add(1) })
			hold, _ := custody.Hold()
			m, j := jobFixture(t, g, hold)
			m.SetOnQuarantine(func(err error) {
				if !errors.Is(err, ErrQuarantined) || !m.Status().Draining {
					t.Error("notification before drain")
				}
				notified.Add(1)
			})
			custody.Complete()
			if _, err := j.Finish(context.Background()); !errors.Is(err, ErrQuarantined) {
				t.Fatal("uncertain cleanup passed", err)
			}
			if released.Load() != 0 || custody.State().Held != 1 || notified.Load() != 1 || m.Healthy() || m.Status().Quarantined != 1 {
				t.Fatal("quarantine lost custody or readiness")
			}
			if _, err := m.Prepare(Limits{MemoryBytes: 32 << 20, MaxProcesses: 32, CPUQuotaMicros: 10000, CPUPeriodMicros: 100000}, func() {}); !errors.Is(err, ErrDraining) {
				t.Fatal("admitted work after quarantine")
			}
			g.killErr = nil
			g.populationErr = nil
			g.populations = nil
			g.usageErr = nil
			g.removeErr = nil
			if _, err := j.Finish(context.Background()); err != nil {
				t.Fatal(err)
			}
			if released.Load() != 1 || !m.Status().Draining || m.Status().Quarantined != 0 {
				t.Fatal("retry lost custody or silently resumed admission")
			}
		})
	}
}

func TestPreparedJobRefusesSecondOrPostQuarantineAttachment(t *testing.T) {
	m, j := jobFixture(t, &testGroup{}, func() {})
	command := exec.Command("/bin/true")
	if err := j.Start(command); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := j.Start(exec.Command("/bin/true")); !errors.Is(err, ErrInvalid) {
		t.Fatal("attached job twice")
	}
	m.quarantine(j.name)
	j.attached = false
	if err := j.Start(exec.Command("/bin/true")); !errors.Is(err, ErrDraining) {
		t.Fatal("attached while quarantined")
	}
}

func TestOwnershipRecordsAndCountersRejectAmbiguity(t *testing.T) {
	r := record{Name: groupPrefix + "0123456789abcdef0123456789abcdef", RootDevice: 1, RootInode: 2, GroupInode: 3}
	decoded, err := decodeRecord(r.encode())
	if err != nil || decoded != r {
		t.Fatal("valid ownership did not roundtrip")
	}
	for _, raw := range []string{r.encode() + "trailing", "", "kelvo-cgroup-v1\n../../unsafe\n1\n2\n3\n"} {
		if _, err := decodeRecord(raw); err == nil {
			t.Fatal("accepted malformed ownership")
		}
	}
	for _, raw := range []string{"populated 0\npopulated 1\n", "populated -1\n", "populated 1 trailing\n"} {
		if _, err := counters(raw); err == nil {
			t.Fatal("accepted ambiguous counters")
		}
	}
}

func TestMissingDelegationDoesNotCreateOrAdoptCgroups(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	os.Chmod(root, 0o700)
	os.Chmod(state, 0o700)
	if _, err := Open(Config{Root: root, StateDirectory: state}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("ordinary directory accepted as cgroup delegation", err)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 0 {
		t.Fatal("created entries in an unverified filesystem")
	}
}

func TestUnsafeAncestryAndStateHardlinksFailClosed(t *testing.T) {
	parent := t.TempDir()
	path := filepath.Join(parent, "private")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o777); err != nil {
		t.Fatal(err)
	}
	if tr, err := openTree(path, true, false); err == nil {
		tr.close()
		t.Fatal("accepted writable ancestor")
	}
	os.Chmod(parent, 0o700)
	tr, err := openTree(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.close()
	file := filepath.Join(path, "record")
	if err := os.WriteFile(file, []byte("private"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(file, filepath.Join(path, "other")); err != nil {
		t.Fatal(err)
	}
	if f, err := privateFile(tr, "record", os.O_RDONLY); err == nil {
		f.Close()
		t.Fatal("accepted hardlinked record")
	}
}

// Closing twice must not close a descriptor reused by an unrelated component.
func TestConcurrentAndRepeatedClosePreservesUnrelatedFD(t *testing.T) {
	m, _ := jobFixture(t, &testGroup{}, func() {})
	path := t.TempDir()
	os.Chmod(path, 0700)
	hierarchy, err := openTree(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	m.hierarchy = hierarchy
	oldFD := hierarchy.fd
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	unrelated, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(unrelated)
	// Pin an unrelated descriptor into the former hierarchy slot explicitly.
	if unrelated != oldFD {
		if err := unix.Dup3(unrelated, oldFD, unix.O_CLOEXEC); err != nil {
			t.Fatal(err)
		}
		defer unix.Close(oldFD)
	}
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := hierarchy.close(); err != nil {
		t.Fatal(err)
	}
	var st unix.Stat_t
	if unix.Fstat(oldFD, &st) != nil {
		t.Fatal("repeated close closed reused unrelated descriptor")
	}
}

func TestAnonymousOwnershipCrashPublication(t *testing.T) {
	for _, stage := range []string{"before_write", "after_write", "after_file_sync", "after_link", "after_directory_sync"} {
		t.Run(stage, func(t *testing.T) {
			path := t.TempDir()
			os.Chmod(path, 0700)
			command := exec.Command(os.Args[0], "-test.run=^TestOwnershipCrashHelper$")
			command.Env = append(os.Environ(), "KELVO_OWNERSHIP_CRASH="+stage, "KELVO_OWNERSHIP_STATE="+path)
			if err := command.Run(); err == nil {
				t.Fatal("helper did not stop at cutpoint")
			}
			entries, err := os.ReadDir(path)
			if err != nil {
				t.Fatal(err)
			}
			expected := 0
			if stage == "after_link" || stage == "after_directory_sync" {
				expected = 1
			}
			if len(entries) != expected {
				t.Fatalf("named partial record at %s: %d", stage, len(entries))
			}
			if expected == 1 {
				body, err := os.ReadFile(filepath.Join(path, "record"))
				if err != nil || string(body) != "complete ownership record\n" {
					t.Fatal("incomplete published record")
				}
			}
		})
	}
}
func TestOwnershipCrashHelper(t *testing.T) {
	stage := os.Getenv("KELVO_OWNERSHIP_CRASH")
	if stage == "" {
		return
	}
	tr, err := openTree(os.Getenv("KELVO_OWNERSHIP_STATE"), true, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	_, err = writeNewPrivateWithHook(tr, "record", "complete ownership record\n", func(at string) error {
		if at == stage {
			os.Exit(77)
		}
		return nil
	})
	fmt.Fprintln(os.Stderr, err)
	os.Exit(3)
}

func TestUnresolvedPreparationRetainsLocksDuringClose(t *testing.T) {
	m, j := jobFixture(t, &testGroup{}, func() {})
	if _, err := j.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.draining = true
	m.quarantined["preparation"] = true
	m.mu.Unlock()
	if err := m.Close(context.Background()); err != ErrQuarantined {
		t.Fatal("unresolved preparation reported successful close", err)
	}
	if m.Status().Closed || m.Status().Quarantined != 1 {
		t.Fatal("unresolved preparation lost state")
	}
	var st unix.Stat_t
	if unix.Fstat(m.state.fd, &st) != nil {
		t.Fatal("uncertain preparation released ownership locks")
	}
}
