//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestNamespaceSupervisorCloseDoesNotWaitBehindSpawn(t *testing.T) {
	s := &NamespaceSupervisor{limits: &ContainerLimits{}, policy: kernelLimits(),
		cleanupTimeout: time.Second, closeDone: make(chan struct{})}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.Close(ctx) }()
	select {
	case err := <-result:
		if !errors.Is(err, ErrQuarantined) || !s.closeRequested.Load() {
			t.Fatal("blocked spawn did not retain closed admission and uncertainty", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Close waited for the spawn mutex instead of its caller deadline")
	}
}

func TestNamespaceSupervisorShutdownLockHasDeadline(t *testing.T) {
	s := &NamespaceSupervisor{}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if s.lockBefore(ctx) {
		t.Fatal("shutdown acquired a lock still held by process creation")
	}
}

func TestSupervisedChildRejectsMalformedHandle(t *testing.T) {
	for _, child := range []*SupervisedChild{nil, {}, {owner: &NamespaceSupervisor{}}} {
		if _, err := child.Wait(context.Background()); !errors.Is(err, ErrInvalid) {
			t.Fatal("malformed child wait was accepted", err)
		}
		if err := child.Signal(syscall.SIGKILL); !errors.Is(err, ErrInvalid) {
			t.Fatal("malformed child signal was accepted", err)
		}
	}
}

func TestSupervisedChildCopiesShareFinalExitAndClosedSignalHandle(t *testing.T) {
	state := &supervisedChildState{pidfd: -1, done: make(chan struct{})}
	child := &SupervisedChild{owner: &NamespaceSupervisor{}, pid: 2, state: state}
	copy := *child
	state.exit = ChildExit{Code: -1, Signal: syscall.SIGKILL}
	close(state.done)
	if exit, err := copy.Wait(context.Background()); err != nil || exit != state.exit {
		t.Fatal("copied handle observed its pre-exit zero value", exit, err)
	}
	if err := copy.Signal(syscall.SIGTERM); !errors.Is(err, ErrUnavailable) {
		t.Fatal("copied handle retained a closed signal descriptor", err)
	}
}

func TestNamespaceSupervisorRejectsAmbientProcess(t *testing.T) {
	if os.Getpid() == 1 {
		t.Skip("ordinary host rejection requires a non-init process")
	}
	for range 2 {
		if s, err := OpenNamespaceSupervisor(kernelLimits(), time.Second); err == nil || s != nil {
			t.Fatal("ambient host process acquired namespace-wide reap ownership")
		}
	}
}

func TestChildFilesStayExplicitAndCallerOwned(t *testing.T) {
	file, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	copies, err := duplicateChildFiles([]*os.File{file, file, file})
	if err != nil || len(copies) != 3 {
		t.Fatal("explicit descriptor copies failed", err)
	}
	for _, fd := range copies {
		flags, err := unix.FcntlInt(fd, unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC == 0 || fd == file.Fd() {
			t.Fatal("a supervisor copy is borrowed or inheritable", err)
		}
	}
	closeChildFiles(copies)
	for _, fd := range copies {
		if _, err := unix.FcntlInt(fd, unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
			t.Fatal("supervisor descriptor remains open", err)
		}
	}
	if _, err := file.Write([]byte("caller still owns this descriptor")); err != nil {
		t.Fatal("supervisor closed a caller descriptor", err)
	}
	if _, err := duplicateChildFiles([]*os.File{file, nil, file}); !errors.Is(err, ErrInvalid) {
		t.Fatal("an implicit descriptor was accepted", err)
	}
}

func TestChildFileDuplicationPreservesStatusFlags(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer write.Close()
	raw, err := write.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	status := func() int {
		t.Helper()
		var flags int
		var statusErr error
		if err := raw.Control(func(fd uintptr) { flags, statusErr = unix.FcntlInt(fd, unix.F_GETFL, 0) }); err != nil || statusErr != nil {
			t.Fatal("cannot inspect caller descriptor", err, statusErr)
		}
		return flags
	}
	before := status()
	if before&unix.O_NONBLOCK == 0 {
		t.Fatal("test pipe must start in Go's pollable nonblocking mode")
	}
	files, err := duplicateChildFiles([]*os.File{write})
	if err != nil {
		t.Fatal(err)
	}
	defer closeChildFiles(files)
	flags, err := unix.FcntlInt(files[0], unix.F_GETFL, 0)
	if err != nil || flags != before || status() != before {
		t.Fatal("duplication changed inherited or caller status flags", err)
	}
	// The caller prepares its solely owned child endpoint explicitly. The
	// opposite parent endpoint must remain pollable for bounded output reads.
	_ = write.Fd()
	if status()&unix.O_NONBLOCK != 0 {
		t.Fatal("explicit child endpoint preparation did not set blocking mode")
	}
	if err := read.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal("preparing child output disabled parent deadlines", err)
	}
}

func TestChildFilesPreserveClosedOptionalDescriptorSlots(t *testing.T) {
	file, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	files, err := duplicateChildFiles([]*os.File{file, file, file, nil, file})
	if err != nil || len(files) != 5 || files[3] != ^uintptr(0) {
		t.Fatal("closed fd3 shifted the later descriptor", files, err)
	}
	if _, err := unix.FcntlInt(files[4], unix.F_GETFD, 0); err != nil {
		t.Fatal("fd4 did not receive its explicit file", err)
	}
	closeChildFiles(files)
	if _, err := file.Write([]byte("caller still owns this descriptor")); err != nil {
		t.Fatal("optional-slot cleanup closed the caller's file", err)
	}
}
