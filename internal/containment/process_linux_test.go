//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type lifecycleGroup struct {
	mu       sync.Mutex
	command  *exec.Cmd
	complete func()
	fail     error
}

func (g *lifecycleGroup) Start(command *exec.Cmd) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := command.Start(); err != nil {
		return err
	}
	g.command = command
	return nil
}

func (*lifecycleGroup) Usage() (Usage, error) { return Usage{}, nil }

func (g *lifecycleGroup) Finish(context.Context) (Usage, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.command != nil {
		_ = g.command.Process.Kill()
	}
	if g.fail != nil {
		return Usage{}, g.fail
	}
	g.complete()
	return Usage{}, nil
}

func newLifecycleFixture(t *testing.T, release func()) (*delegatedProcess, *lifecycleGroup) {
	t.Helper()
	custody, err := NewCustody(release)
	if err != nil {
		t.Fatal(err)
	}
	reaped, err := custody.Hold()
	if err != nil {
		t.Fatal(err)
	}
	group := &lifecycleGroup{complete: custody.Complete}
	process := &delegatedProcess{group: group, releaseReap: reaped}
	t.Cleanup(func() {
		group.mu.Lock()
		group.fail = nil
		group.mu.Unlock()
		_, _ = process.Finish(context.Background())
	})
	return process, group
}

func lifecycleCommand(t *testing.T, mode string) *exec.Cmd {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^TestDelegatedProcessHelper$")
	command.Env = []string{"KELVO_TEST_PROCESS_HELPER=" + mode}
	command.WaitDelay = 100 * time.Millisecond
	return command
}

func TestDelegatedProcessHelper(t *testing.T) {
	switch os.Getenv("KELVO_TEST_PROCESS_HELPER") {
	case "hold":
		time.Sleep(time.Minute)
		os.Exit(0)
	case "exit":
		os.Exit(0)
	}
}

func TestProcessConcurrentWaitAndFinishOwnReaping(t *testing.T) {
	var releases atomic.Int32
	process, _ := newLifecycleFixture(t, func() { releases.Add(1) })
	command := lifecycleCommand(t, "hold")
	if err := process.Start(command); err != nil {
		t.Fatal(err)
	}
	const waiters = 16
	results := make(chan error, waiters)
	for range waiters {
		go func() { results <- process.Wait() }()
	}
	if _, err := process.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	var first error
	for range waiters {
		select {
		case err := <-results:
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatalf("wait did not return the killed child's exit: %v", err)
			}
			if first == nil {
				first = err
			} else if first != err {
				t.Fatalf("waiters received different outcomes: %v / %v", first, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("wait owner did not finish")
		}
	}
	if releases.Load() != 1 || command.ProcessState == nil {
		t.Fatal("custody did not follow the completed wait and cleanup")
	}
	var status unix.WaitStatus
	if _, err := unix.Wait4(command.Process.Pid, &status, unix.WNOHANG, nil); !errors.Is(err, unix.ECHILD) {
		t.Fatalf("child was not reaped: %v", err)
	}
}

func TestProcessWaitRetainsCustodyUntilCleanup(t *testing.T) {
	var releases atomic.Int32
	process, _ := newLifecycleFixture(t, func() { releases.Add(1) })
	if err := process.Start(lifecycleCommand(t, "exit")); err != nil {
		t.Fatal(err)
	}
	if err := process.Wait(); err != nil {
		t.Fatal(err)
	}
	if releases.Load() != 0 {
		t.Fatal("wait released custody before domain cleanup")
	}
	if _, err := process.Finish(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatalf("cleanup did not release custody once: %v", err)
	}
}

func TestProcessDomainCloseRetainsCustodyUntilReaping(t *testing.T) {
	var releases atomic.Int32
	process, group := newLifecycleFixture(t, func() { releases.Add(1) })
	if err := process.Start(lifecycleCommand(t, "hold")); err != nil {
		t.Fatal(err)
	}
	// Manager.Close can finish the underlying domain while the request still
	// owns the command. Its cleanup callback must not release that wait hold.
	if _, err := group.Finish(context.Background()); err != nil || releases.Load() != 0 {
		t.Fatalf("domain cleanup released an unreaped command: %v", err)
	}
	_ = process.Wait()
	if releases.Load() != 1 {
		t.Fatal("completed wait did not release the closed domain's custody")
	}
}

func TestProcessCleanupUncertaintyRetainsCustody(t *testing.T) {
	var releases atomic.Int32
	process, group := newLifecycleFixture(t, func() { releases.Add(1) })
	if err := process.Start(lifecycleCommand(t, "hold")); err != nil {
		t.Fatal(err)
	}
	group.fail = ErrQuarantined
	if _, err := process.Finish(context.Background()); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("cleanup uncertainty was lost: %v", err)
	}
	_ = process.Wait()
	if releases.Load() != 0 {
		t.Fatal("uncertain cleanup released custody after reaping")
	}
	group.fail = nil
	if _, err := process.Finish(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatalf("cleanup retry did not finish existing custody: %v", err)
	}
	if err := process.Start(lifecycleCommand(t, "exit")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cleanup reopened process admission: %v", err)
	}
}

func TestProcessNoChildRetainsCustodyUntilCleanup(t *testing.T) {
	for _, attempt := range []bool{false, true} {
		t.Run(map[bool]string{false: "unused", true: "failed-start"}[attempt], func(t *testing.T) {
			var releases atomic.Int32
			process, _ := newLifecycleFixture(t, func() { releases.Add(1) })
			if attempt {
				if err := process.Start(exec.Command(filepath.Join(t.TempDir(), "missing"))); err == nil {
					t.Fatal("missing executable started")
				}
				if err := process.Start(lifecycleCommand(t, "exit")); !errors.Is(err, ErrInvalid) {
					t.Fatalf("failed start was retried: %v", err)
				}
			}
			if err := process.Wait(); !errors.Is(err, ErrInvalid) || releases.Load() != 0 {
				t.Fatalf("unstarted wait changed custody: %v", err)
			}
			if _, err := process.Finish(context.Background()); err != nil || releases.Load() != 1 {
				t.Fatalf("unused group did not finish custody: %v", err)
			}
			if err := process.Start(lifecycleCommand(t, "exit")); !errors.Is(err, ErrInvalid) {
				t.Fatalf("finished group admitted a child: %v", err)
			}
		})
	}
}
