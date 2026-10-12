//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type linuxWorkerProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	job    containment.Job
}

func (p *linuxWorkerProcess) Stdin() io.Writer { return p.stdin }
func (p *linuxWorkerProcess) Stdout() io.Reader { return p.stdout }
func (p *linuxWorkerProcess) Kill() error {
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	return nil
}
func (p *linuxWorkerProcess) Wait() error {
	if p.stdin != nil {
		_ = p.stdin.Close()
	}
	if p.stdout != nil {
		_ = p.stdout.Close()
	}
	if p.job != nil {
		return p.job.Wait()
	}
	return nil
}

func (l *defaultWorkerLauncher) Launch(ctx context.Context, e *Executor, cfg OperationProcessConfig, limits query.Limits) (WorkerProcess, func() error, error) {
	if e == nil || e.ResourcePool == nil || e.ScratchRoot == nil || e.Containment == nil {
		return nil, nil, errors.New("warm_pool: executor runtime unavailable")
	}

	memoryBytes := int64(limits.MemoryMB) << 20
	reservation, err := e.ResourcePool.Acquire(ctx, admission.Request{
		MemoryBytes:  memoryBytes + e.ResourceOverheadBytes,
		ScratchBytes: int64(limits.MaxTempMB) << 20,
	})
	if err != nil {
		return nil, nil, err
	}

	custody, _ := containment.NewCustody(reservation.Release)
	workspace, err := newScratchWorkspace(e.ScratchRoot)
	if err != nil {
		custody.Complete()
		return nil, nil, err
	}

	binary, _, err := openOperationBinary(ctx, cfg)
	if err != nil {
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	args, err := SandboxCommand(cfg.Binary, workspace.path, catalog.Config{}, limits)
	if err != nil {
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	// Target serve subcommand instead of worker
	args[len(args)-2] = "/proc/self/fd/5"
	args[len(args)-1] = "serve"

	processLimits, err := e.ContainmentBudget.ProcessLimits(int64(limits.MemoryMB), limits.Threads)
	if err != nil {
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	holdProcess, err := custody.Hold()
	if err != nil {
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	job, err := e.Containment.PrepareProcess(processLimits, holdProcess)
	if err != nil {
		holdProcess()
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	command := exec.CommandContext(ctx, e.SandboxPath, args...)
	configureProcess(command)
	command.Dir = workspace.path
	command.Env = operationEnvironment(workspace.path, limits.Threads)
	command.WaitDelay = query.WorkerWaitDelay
	workspace.attach(command)
	command.ExtraFiles = append(command.ExtraFiles, binary) // fd5 executable

	stdinPipe, err := command.StdinPipe()
	if err != nil {
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	stdoutPipe, err := command.StdoutPipe()
	if err != nil {
		_ = stdinPipe.Close()
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	if err := job.Start(command); err != nil {
		_ = stdinPipe.Close()
		_ = stdoutPipe.Close()
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	cleanup := func() error {
		_ = binary.Close()
		finishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, jobErr := job.Finish(finishCtx)
		wsErr := workspace.cleanup()
		custody.Complete()
		return errors.Join(jobErr, wsErr)
	}

	proc := &linuxWorkerProcess{
		cmd:    command,
		stdin:  stdinPipe,
		stdout: stdoutPipe,
		job:    job,
	}

	return proc, cleanup, nil
}
