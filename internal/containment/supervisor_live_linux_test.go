//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// This gate requires the dedicated read-only PID-1 CRI fixture. A host test run
// must skip it; a qualification controller must reject that skip.
func TestNamespaceSupervisorLive(t *testing.T) {
	if os.Getenv("KELVO_TEST_NAMESPACE_SUPERVISOR") != "1" {
		t.Skip("requires the dedicated PID-1 supervisor fixture")
	}
	policy := Limits{MemoryBytes: 256 << 20, MaxProcesses: 64, CPUQuotaMicros: 100000, CPUPeriodMicros: 100000}
	s, err := OpenNamespaceSupervisor(policy, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(context.Background()); err != nil {
			t.Error("namespace cleanup remained uncertain", err)
		}
	})
	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer null.Close()
	spec := func(mode string) ChildSpec {
		return ChildSpec{Path: "/probe/supervisor-probe", Args: []string{"supervisor-probe", mode}, Dir: "/",
			Files: []*os.File{null, null, null}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Hold the registry lock until the exact child is already waitable. WNOWAIT
	// leaves its exit for the single supervisor reaper after registration.
	observed := false
	child, err := s.spawn(ctx, spec("exit"), func(pid int) {
		until := time.Now().Add(time.Second)
		for time.Now().Before(until) {
			var info unix.Siginfo
			if unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil) == nil && info.Signo != 0 {
				observed = true
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	if err != nil || !observed {
		t.Fatal("exit-before-registration boundary was not exercised", err)
	}
	if exit, err := child.Wait(ctx); err != nil || exit.Code != 17 || exit.Signal != 0 {
		t.Fatal("early exit lost its registered future", exit, err)
	}

	var parallel sync.WaitGroup
	failures := make(chan error, 16)
	for range 16 {
		parallel.Add(1)
		go func() {
			defer parallel.Done()
			child, err := s.Spawn(ctx, spec("exit"))
			if err != nil {
				failures <- err
				return
			}
			if exit, err := child.Wait(ctx); err != nil || exit.Code != 17 {
				failures <- fmt.Errorf("registered exit missing: %v", err)
			}
		}()
	}
	parallel.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	missing := spec("exit")
	missing.Path = "/probe/does-not-exist"
	if _, err := s.Spawn(ctx, missing); !errors.Is(err, ErrUnavailable) {
		t.Fatal("failed exec did not return its bounded launch failure", err)
	}

	ready := func(mode string, stdout *os.File) *SupervisedChild {
		t.Helper()
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer read.Close()
		defer write.Close()
		// This endpoint belongs only to the child; keep the parent read end
		// pollable so its readiness deadline remains effective.
		_ = write.Fd()
		request := spec(mode)
		request.Files = append(request.Files, write)
		if stdout != nil {
			request.Files[1] = stdout
		}
		child, err := s.Spawn(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		_ = write.Close()
		_ = read.SetReadDeadline(time.Now().Add(time.Second))
		var marker [1]byte
		if n, err := read.Read(marker[:]); err != nil || n != 1 || marker[0] != 'R' {
			t.Fatal("native workload did not reach its marker", err)
		}
		return child
	}
	held := ready("hold", nil)
	waitCtx, stopWait := context.WithTimeout(ctx, 25*time.Millisecond)
	if _, err := held.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("wait cancellation reported a false native exit", err)
	}
	stopWait()
	if err := held.Signal(syscall.SIGSTOP); !errors.Is(err, ErrInvalid) {
		t.Fatal("unsupported process-control signal was accepted", err)
	}
	if err := held.Signal(syscall.SIGTERM); err != nil {
		t.Fatal("pidfd did not signal a live child", err)
	}
	if exit, err := held.Wait(ctx); err != nil || exit.Signal != syscall.SIGTERM {
		t.Fatal("wait cancellation lost the live child", exit, err)
	}
	traced := ready("trace-stop", nil)
	until := time.Now().Add(time.Second)
	for s.State().Nonterminal == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if state := s.State(); state.Nonterminal != 1 || state.Registered != 1 {
		t.Fatal("traced stop was not retained as a live registered child", state)
	}
	waitCtx, stopWait = context.WithTimeout(ctx, 25*time.Millisecond)
	if _, err := traced.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("traced stop completed a native exit future", err)
	}
	stopWait()
	if err := traced.Signal(syscall.SIGKILL); err != nil {
		t.Fatal("traced child lost its pinned signal handle", err)
	}
	if exit, err := traced.Wait(ctx); err != nil || exit.Signal != syscall.SIGKILL {
		t.Fatal("traced child did not publish its final termination", exit, err)
	}
	clone := ready("clone-parent-zero-signal", nil)
	if exit, err := clone.Wait(ctx); err != nil || exit.Code != 0 {
		t.Fatal("custom clone fixture failed", exit, err)
	}
	orphan := ready("double-fork", nil)
	if exit, err := orphan.Wait(ctx); err != nil || exit.Code != 0 {
		t.Fatal("detached adoption fixture failed", exit, err)
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer write.Close()
	_ = write.Fd()
	blocked := ready("block-output", write)
	_ = write.Close()
	waitCtx, stopWait = context.WithTimeout(ctx, 25*time.Millisecond)
	if _, err := blocked.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("native output did not stay blocked", err)
	}
	stopWait()
	// Hold an actual launched process at the registration boundary. Close
	// must return at its caller deadline while its sole owner keeps custody.
	releaseRegistration := make(chan struct{})
	atRegistration := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRegistration) }) }
	defer release()
	type launchResult struct {
		child *SupervisedChild
		err   error
	}
	launch := make(chan launchResult, 1)
	go func() {
		child, err := s.spawn(ctx, spec("exit"), func(int) {
			close(atRegistration)
			select {
			case <-releaseRegistration:
			case <-ctx.Done():
			}
		})
		launch <- launchResult{child, err}
	}()
	select {
	case <-atRegistration:
	case <-ctx.Done():
		t.Fatal("native spawn did not reach the held registration boundary")
	}
	closeCtx, stopClose := context.WithTimeout(ctx, 20*time.Millisecond)
	closeStarted := time.Now()
	if err := s.Close(closeCtx); !errors.Is(err, ErrQuarantined) {
		t.Fatal("Close did not retain uncertainty while a launch was unregistered", err)
	}
	closeElapsed := time.Since(closeStarted)
	if closeElapsed > 250*time.Millisecond {
		t.Fatal("Close did not honor its caller deadline while registration was held", closeElapsed)
	}
	stopClose()
	release()
	select {
	case result := <-launch:
		if result.err != nil {
			t.Fatal("shutdown lost the in-flight launch", result.err)
		}
		if _, err := result.child.Wait(ctx); err != nil {
			t.Fatal("in-flight launch did not publish its final exit", err)
		}
	case <-ctx.Done():
		t.Fatal("in-flight launch did not resolve after registration resumed")
	}
	if err := s.Close(ctx); err != nil {
		t.Fatal("namespace shutdown failed", err)
	}
	if exit, err := blocked.Wait(ctx); err != nil || exit.Signal != unix.SIGKILL {
		t.Fatal("blocked output child did not terminate", exit, err)
	}
	state := s.State()
	if state.Registered != 0 || state.Adopted < 3 || !state.Draining || !state.Closed {
		t.Fatal("namespace did not join registered and adopted children", state)
	}
	if _, err := s.Spawn(ctx, spec("exit")); !errors.Is(err, ErrDraining) {
		t.Fatal("closed namespace admitted a process", err)
	}
	t.Logf("SUPERVISOR_ACCEPTED early_registration=true reaped=%d adopted=%d traced_stop=true blocked_output=true close_deadline_ms=%d remaining_registered=0", state.Reaped, state.Adopted, closeElapsed.Milliseconds())
}
