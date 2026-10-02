//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const cancellationGrace = 750 * time.Millisecond

func configureProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL, Setpgid: true}
	cmd.Cancel = func() error { return cancelProcess(cmd) }
}

func cancelProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	return cmd.Process.Signal(syscall.SIGTERM)
}

// Give source drivers a bounded opportunity to cancel remote work. Waitid
// observes exit without reaping: the leader must keep the process-group ID
// reserved until all descendants have received the final SIGKILL.
func finishProcess(cmd *exec.Cmd, cancelled bool) error {
	if cancelled && cmd.Process != nil {
		_ = cancelProcess(cmd)
		deadline := time.Now().Add(cancellationGrace)
		for time.Now().Before(deadline) {
			var info unix.Siginfo
			err := unix.Waitid(unix.P_PID, cmd.Process.Pid, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil)
			if (err != nil && !errors.Is(err, syscall.EINTR)) || info.Signo != 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	return cleanupProcess(cmd)
}

// The sandbox forbids process-group/session/namespace changes after startup.
// The container PID/cgroup boundary remains necessary for native-code faults.
func cleanupProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}
