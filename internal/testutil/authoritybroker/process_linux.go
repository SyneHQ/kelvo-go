//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authoritybroker

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type BrokerOutcome struct {
	Name            string `json:"name"`
	PID             int    `json:"pid"`
	StartTicks      string `json:"start_ticks"`
	ProcessGroup    int    `json:"process_group"`
	AliveBeforeStop bool   `json:"alive_before_stop"`
	ForcedKill      bool   `json:"forced_kill"`
	Reaped          bool   `json:"reaped"`
	ExitCode        int    `json:"exit_code"`
	WaitOutcome     string `json:"wait_outcome"`
	GroupAbsent     bool   `json:"process_group_absent"`
	LogSHA256       string `json:"log_sha256"`
	LogTruncated    bool   `json:"log_truncated"`
}

type node struct {
	name, client, route, monitor, logPath string
	cmd                                   *exec.Cmd
	done                                  chan error
	log                                   *boundedLog
	receipt                               BrokerOutcome
}

func (n *node) start(binary, config, logPath string) error {
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("private broker log creation failed")
	}
	n.log, n.logPath = &boundedLog{file: f}, logPath
	cmd := exec.Command(binary, "-c", config)
	cmd.Env = []string{"GOMAXPROCS=1", "GOMEMLIMIT=96MiB"}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Stdout, cmd.Stderr = n.log, n.log
	if err = cmd.Start(); err != nil {
		_ = f.Close()
		return errors.New("owned broker start failed")
	}
	n.cmd, n.done = cmd, make(chan error, 1)
	go func() { err := cmd.Wait(); n.done <- errors.Join(err, f.Close()) }()
	ticks, group, state, err := processIdentity(cmd.Process.Pid)
	n.receipt = BrokerOutcome{Name: n.name, PID: cmd.Process.Pid, StartTicks: ticks, ProcessGroup: group, ExitCode: -1}
	if err != nil || group != cmd.Process.Pid || state == "Z" || state == "X" {
		return errors.New("owned broker identity unverified")
	}
	return nil
}

func (n *node) stop() error {
	var result error
	fail := func() { result = errors.New("broker shutdown or process ownership unproven") }
	ticks, group, state, err := processIdentity(n.receipt.PID)
	n.receipt.AliveBeforeStop = err == nil && ticks == n.receipt.StartTicks && group == n.receipt.ProcessGroup && group == n.receipt.PID && state != "Z" && state != "X"
	if !n.receipt.AliveBeforeStop {
		fail()
	} else if err := n.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		fail()
	}
	var waitErr error
	select {
	case waitErr = <-n.done:
	case <-time.After(3 * time.Second):
		n.receipt.ForcedKill = true
		fail()
		ticks, group, _, identityErr := processIdentity(n.receipt.PID)
		if identityErr == nil && ticks == n.receipt.StartTicks && group == n.receipt.ProcessGroup {
			_ = n.cmd.Process.Kill()
		}
		select {
		case waitErr = <-n.done:
		case <-time.After(2 * time.Second):
			return result
		}
	}
	n.receipt.Reaped = true
	n.receipt.WaitOutcome = "exit_zero"
	if n.cmd.ProcessState != nil {
		n.receipt.ExitCode = n.cmd.ProcessState.ExitCode()
	}
	if waitErr != nil || n.receipt.ExitCode != 0 {
		fail()
		n.receipt.WaitOutcome = "exit_nonzero"
		if n.receipt.ExitCode < 0 {
			n.receipt.WaitOutcome = "signal_or_wait_error"
		}
	}
	n.receipt.GroupAbsent = groupAbsent(n.receipt.PID)
	if !n.receipt.GroupAbsent {
		fail()
	}
	raw, err := os.ReadFile(n.logPath)
	if err != nil || len(raw) > 1<<20 || n.log.failed {
		fail()
	} else {
		n.receipt.LogSHA256 = fmt.Sprintf("%x", sha256.Sum256(raw))
	}
	n.receipt.LogTruncated = n.log.truncated
	return result
}

type boundedLog struct {
	mu                sync.Mutex
	file              *os.File
	written           int
	truncated, failed bool
}

func (w *boundedLog) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	part := p[:min(n, (1<<20)-w.written)]
	if len(part) < n {
		w.truncated = true
	}
	if len(part) > 0 {
		written, err := w.file.Write(part)
		w.written += written
		if err != nil {
			w.failed = true
			return written, err
		}
	}
	return n, nil
}

func processIdentity(pid int) (string, int, string, error) {
	bad := errors.New("process identity unavailable")
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil || len(raw) > 8192 {
		return "", 0, "", bad
	}
	end := strings.LastIndexByte(string(raw), ')')
	if end < 0 {
		return "", 0, "", bad
	}
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) < 20 {
		return "", 0, "", bad
	}
	group, e1 := strconv.Atoi(fields[2])
	_, e2 := strconv.ParseUint(fields[19], 10, 64)
	if e1 != nil || e2 != nil || group < 0 {
		return "", 0, "", bad
	}
	return fields[19], group, fields[0], nil
}

func groupAbsent(group int) bool {
	entries, err := os.ReadDir("/proc")
	if err != nil || len(entries) > 65536 {
		return false
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		_, actual, _, err := processIdentity(pid)
		if err == nil && actual == group {
			return false
		}
		if err != nil {
			if _, err := os.Stat("/proc/" + entry.Name()); !errors.Is(err, os.ErrNotExist) {
				return false
			}
		}
	}
	return true
}

func socketAbsent(address string) bool {
	conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return false
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}
