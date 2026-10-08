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
	"syscall"
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
	process := &delegatedProcess{group: group, releaseReap: reaped, cleanupTimeout: 3 * time.Second, quarantine: func() {}}
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
	case "output":
		_, _ = os.Stdout.Write([]byte("probe"))
		os.Exit(0)
	}
}

type blockedLifecycleWriter struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (w *blockedLifecycleWriter) Write(value []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(value), nil
}

func TestProcessFinishDeadlineRetainsBlockedWait(t *testing.T) {
	for _, callerDeadline := range []bool{true, false} {
		t.Run(map[bool]string{true: "caller-deadline", false: "manager-deadline"}[callerDeadline], func(t *testing.T) {
			var releases, drained atomic.Int32
			process, _ := newLifecycleFixture(t, func() { releases.Add(1) })
			process.quarantine = func() { drained.Add(1) }
			writer := &blockedLifecycleWriter{entered: make(chan struct{}), release: make(chan struct{})}
			var unblock sync.Once
			t.Cleanup(func() { unblock.Do(func() { close(writer.release) }) })
			command := lifecycleCommand(t, "output")
			command.Stdout = writer
			if err := process.Start(context.Background(), command); err != nil {
				t.Fatal(err)
			}
			select {
			case <-writer.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("child did not reach the blocked output writer")
			}
			ctx := context.Background()
			if callerDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 25*time.Millisecond)
				defer cancel()
			} else {
				process.cleanupTimeout = 25 * time.Millisecond
			}
			finished := make(chan error, 1)
			go func() { _, err := process.Finish(ctx); finished <- err }()
			select {
			case err := <-finished:
				if !errors.Is(err, ErrQuarantined) || releases.Load() != 0 || drained.Load() != 1 {
					t.Fatalf("blocked wait lost its custody or deadline: err=%v releases=%d drained=%d", err, releases.Load(), drained.Load())
				}
			case <-time.After(time.Second):
				t.Fatal("Finish ignored its cleanup deadline")
			}
			unblock.Do(func() { close(writer.release) })
			_ = process.Wait()
			if releases.Load() != 1 {
				t.Fatal("actual wait completion did not release existing custody")
			}
			if err := process.Start(context.Background(), lifecycleCommand(t, "exit")); !errors.Is(err, ErrInvalid) {
				t.Fatalf("deadline reopened process admission: %v", err)
			}
		})
	}
}

func TestProcessConcurrentWaitAndFinishOwnReaping(t *testing.T) {
	var releases atomic.Int32
	process, _ := newLifecycleFixture(t, func() { releases.Add(1) })
	command := lifecycleCommand(t, "hold")
	if err := process.Start(context.Background(), command); err != nil {
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
	if exit, done := process.Exit(); !done || exit.Code != -1 || exit.Signal != syscall.SIGKILL {
		t.Fatal("typed native termination was lost", exit, done)
	}
	var status unix.WaitStatus
	if _, err := unix.Wait4(command.Process.Pid, &status, unix.WNOHANG, nil); !errors.Is(err, unix.ECHILD) {
		t.Fatalf("child was not reaped: %v", err)
	}
}

func TestProcessCanceledStartRetainsReservationUntilCleanup(t *testing.T) {
	var releases atomic.Int32
	process, group := newLifecycleFixture(t, func() { releases.Add(1) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := process.Start(ctx, lifecycleCommand(t, "exit")); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled admission launched a process", err)
	}
	if group.command != nil || releases.Load() != 0 {
		t.Fatal("cancelled launch changed native ownership or freed an unclean group")
	}
	if _, done := process.Exit(); done {
		t.Fatal("failed launch invented a native exit")
	}
	if _, err := process.Finish(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("cancelled launch cleanup did not release once", err)
	}
}

func TestProcessWaitRetainsCustodyUntilCleanup(t *testing.T) {
	var releases atomic.Int32
	process, _ := newLifecycleFixture(t, func() { releases.Add(1) })
	if err := process.Start(context.Background(), lifecycleCommand(t, "exit")); err != nil {
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
	if err := process.Start(context.Background(), lifecycleCommand(t, "hold")); err != nil {
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
	if err := process.Start(context.Background(), lifecycleCommand(t, "hold")); err != nil {
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
	if err := process.Start(context.Background(), lifecycleCommand(t, "exit")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cleanup reopened process admission: %v", err)
	}
}

func TestProcessNoChildRetainsCustodyUntilCleanup(t *testing.T) {
	for _, attempt := range []bool{false, true} {
		t.Run(map[bool]string{false: "unused", true: "failed-start"}[attempt], func(t *testing.T) {
			var releases atomic.Int32
			process, _ := newLifecycleFixture(t, func() { releases.Add(1) })
			if attempt {
				if err := process.Start(context.Background(), exec.Command(filepath.Join(t.TempDir(), "missing"))); err == nil {
					t.Fatal("missing executable started")
				}
				if err := process.Start(context.Background(), lifecycleCommand(t, "exit")); !errors.Is(err, ErrInvalid) {
					t.Fatalf("failed start was retried: %v", err)
				}
			}
			if err := process.Wait(); !errors.Is(err, ErrInvalid) || releases.Load() != 0 {
				t.Fatalf("unstarted wait changed custody: %v", err)
			}
			if _, err := process.Finish(context.Background()); err != nil || releases.Load() != 1 {
				t.Fatalf("unused group did not finish custody: %v", err)
			}
			if err := process.Start(context.Background(), lifecycleCommand(t, "exit")); !errors.Is(err, ErrInvalid) {
				t.Fatalf("finished group admitted a child: %v", err)
			}
		})
	}
}
