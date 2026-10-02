//go:build linux

package worker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func startProcessTree(t *testing.T, ctx context.Context, mode string) (*exec.Cmd, int) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, exe, mode)
	configureProcess(cmd)
	cmd.Stderr = &bytes.Buffer{}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cleanupProcess(cmd)
		}
	})
	scanner := bufio.NewScanner(stdout)
	ready := make(chan int, 1)
	go func() {
		if scanner.Scan() {
			pid, _ := strconv.Atoi(scanner.Text())
			ready <- pid
		} else {
			ready <- 0
		}
	}()
	select {
	case pid := <-ready:
		if pid < 1 {
			t.Fatal("child PID unavailable")
		}
		group, err := syscall.Getpgid(pid)
		if err != nil || group != cmd.Process.Pid {
			t.Fatalf("descendant escaped initial process group: %v", err)
		}
		return cmd, pid
	case <-time.After(5 * time.Second):
		t.Fatal("process tree startup timed out")
	}
	return nil, 0
}

func requireProcessTerminated(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if os.IsNotExist(err) {
			return
		}
		if err == nil {
			end := strings.LastIndexByte(string(data), ')')
			if end >= 0 && len(data) > end+2 && (data[end+2] == 'Z' || data[end+2] == 'X') {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("descendant remained active after group termination")
}

func TestCancellationKillsWorkerProcessGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd, child := startProcessTree(t, ctx, "--group-parent")
	cancel()
	if err := finishProcess(cmd, true); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("cancelled leader succeeded")
	}
	requireProcessTerminated(t, child)
}

func TestCancellationAllowsCooperativeCleanupAndKillsDescendants(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd, child := startProcessTree(t, ctx, "--group-cooperative-parent")
	cancel()
	if err := finishProcess(cmd, true); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if !strings.Contains(cmd.Stderr.(*bytes.Buffer).String(), "cooperative-cleanup-complete") {
		t.Fatal("worker was killed before cooperative cleanup")
	}
	requireProcessTerminated(t, child)
}

func TestCancellationForcesUncooperativeLeaderAndDescendants(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd, child := startProcessTree(t, ctx, "--group-ignores-term-parent")
	started := time.Now()
	cancel()
	if err := finishProcess(cmd, true); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("uncooperative leader survived")
	}
	if elapsed := time.Since(started); elapsed < cancellationGrace || elapsed > 2*time.Second {
		t.Fatalf("cancellation grace was not bounded: %s", elapsed)
	}
	requireProcessTerminated(t, child)
}

func TestExecutorCancellationAllowsSourceCleanup(t *testing.T) {
	e, err := New(catalog.Config{}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	stats, err := e.Execute(ctx, query.Request{SQL: "SELECT cooperative_wait"}, &workerTestSink{})
	if err == nil || query.PublicError(err).Code != "DEADLINE_EXCEEDED" {
		t.Fatalf("cancellation: %v", err)
	}
	if stats.Backend != "cooperative-cleanup-complete" {
		t.Fatal("executor skipped source cleanup grace")
	}
	if time.Since(started) > 3*time.Second {
		t.Fatal("cooperative cancellation exceeded bound")
	}
}

func TestSuccessfulLeaderCannotLeaveBackgroundDescendant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd, child := startProcessTree(t, ctx, "--group-orphan-parent")
	// Observe the unreaped leader's exit instead of assuming a fixed delay.
	// Race-instrumented helpers deliberately pause during process teardown.
	// The zombie keeps its process-group ID reserved through descendant cleanup.
	requireProcessTerminated(t, cmd.Process.Pid)
	if err := cleanupProcess(cmd); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("completed leader exit changed: %v", err)
	}
	requireProcessTerminated(t, child)
}
