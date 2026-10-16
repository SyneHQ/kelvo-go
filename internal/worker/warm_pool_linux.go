//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type linuxWorkerProcess struct {
	cancel context.CancelFunc
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	job    containment.Process
}

func (p *linuxWorkerProcess) Stdin() io.Writer  { return p.stdin }
func (p *linuxWorkerProcess) Stdout() io.Reader { return p.stdout }
func (p *linuxWorkerProcess) Kill() error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.job != nil {
		// Kills the whole process group, not only the launcher's leader.
		return p.job.Terminate(0)
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

// reserveWorker takes a worker's node reservation. Short of capacity it
// refuses at once, unless the pool let this launch wait (withCapacityWait);
// then it waits in the node's FIFO until that deadline. Either way a lack of
// capacity is errWarmUnavailable, never a launch failure.
func reserveWorker(ctx context.Context, pool *admission.Pool, request admission.Request) (*admission.Reservation, error) {
	deadline, wait := capacityWait(ctx)
	for {
		changed := pool.Changed()
		// A warm worker keeps its reservation while idle. Never let it take the
		// node's last execution slot: one-shot operations must stay admissible.
		// Separate classes already keep background capacity out of its reach.
		snapshot := pool.Snapshot()
		if snapshot.Limits.SeparateClasses || snapshot.Limits.MaxConcurrent-snapshot.Active >= 2 {
			break
		}
		if !wait {
			return nil, errWarmUnavailable
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-changed:
			timer.Stop()
		case <-timer.C:
			return nil, errWarmUnavailable
		case <-ctx.Done():
			// The launch's own deadline (launchTimeout) ended a capacity wait:
			// still capacity, not a failed launch.
			timer.Stop()
			return nil, errWarmUnavailable
		}
	}
	if !wait {
		reservation, err := pool.TryAcquire(request)
		if errors.Is(err, admission.ErrBusy) {
			return nil, errWarmUnavailable
		}
		return reservation, err
	}
	reserveCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	reservation, err := pool.Acquire(reserveCtx, request)
	// Either deadline, the pool's or the launch's, ending this wait is capacity.
	if err != nil && reserveCtx.Err() != nil {
		return nil, errWarmUnavailable
	}
	return reservation, err
}

func (l *defaultWorkerLauncher) Launch(ctx context.Context, e *Executor, cfg OperationProcessConfig, limits query.Limits) (WorkerProcess, func() error, error) {
	return launchServe(ctx, e, cfg, limits, 0)
}

// serveLauncher starts adapters with an explicit source-connection pool size.
type serveLauncher struct{ poolSize int }

// NewServeLauncher returns the default launcher, passing `--pool-size N` to the
// adapter's serve command. An adapter that does not know the flag exits at
// once, the hello handshake fails, and the pool falls back to one-shot.
func NewServeLauncher(poolSize int) WorkerLauncher { return &serveLauncher{poolSize: poolSize} }

func (l *serveLauncher) Launch(ctx context.Context, e *Executor, cfg OperationProcessConfig, limits query.Limits) (WorkerProcess, func() error, error) {
	return launchServe(ctx, e, cfg, limits, l.poolSize)
}

// launchServe starts one serve process; poolSize 0 leaves the adapter's default.
func launchServe(ctx context.Context, e *Executor, cfg OperationProcessConfig, limits query.Limits, poolSize int) (WorkerProcess, func() error, error) {
	if e == nil || e.ResourcePool == nil || e.ScratchRoot == nil || e.Containment == nil {
		return nil, nil, errors.New("warm_pool: executor runtime unavailable")
	}

	// The node buffers the worker's streams inside its reservation's overhead.
	if m := e.WarmPool; m != nil && m.cfg.FitsOverhead(e.ResourceOverheadBytes) != nil {
		return nil, nil, errWarmUnavailable
	}
	memoryBytes := int64(limits.MemoryMB) << 20
	reservation, err := reserveWorker(ctx, e.ResourcePool, admission.Request{
		Class:        admission.ClassInteractive,
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
	if poolSize > 0 {
		args = append(args, "--pool-size", strconv.Itoa(poolSize))
	}

	processLimits, err := e.ContainmentBudget.ProcessLimits(int64(limits.MemoryMB), limits.Threads)
	if err != nil {
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}
	processLimits.CPUWeight = containment.CPUWeightReadWorker

	holdProcess, err := custody.Hold()
	if err != nil {
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	job, err := e.Containment.PrepareProcess(custody, processLimits, holdProcess)
	if err != nil {
		holdProcess()
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	// The worker outlives the request that spawned it, so it owns its context.
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	placeholder, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		cancelWorker()
		holdProcess()
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}
	defer placeholder.Close()
	command := exec.CommandContext(workerCtx, e.SandboxPath, args...)
	configureProcess(command)
	command.Dir = workspace.path
	command.Env = operationEnvironment(workspace.path, limits.Threads)
	command.WaitDelay = query.WorkerWaitDelay
	// Same descriptor layout as a one-shot operation: fd3 is the receipt pipe
	// there and unused here, fd4 the scratch lease, fd5 the pinned executable.
	command.ExtraFiles = []*os.File{placeholder}
	workspace.attach(command)
	if len(command.ExtraFiles) != 2 {
		cancelWorker()
		holdProcess()
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, errWarmUnavailable
	}
	command.ExtraFiles = append(command.ExtraFiles, binary)

	stdinPipe, err := command.StdinPipe()
	if err != nil {
		cancelWorker()
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	stdoutPipe, err := command.StdoutPipe()
	if err != nil {
		cancelWorker()
		_ = stdinPipe.Close()
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	if err := job.Start(workerCtx, command); err != nil {
		cancelWorker()
		_ = stdinPipe.Close()
		_ = stdoutPipe.Close()
		_ = binary.Close()
		_ = workspace.cleanup()
		custody.Complete()
		return nil, nil, err
	}

	cleanup := func() error {
		cancelWorker()
		_ = binary.Close()
		finishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, jobErr := job.Finish(finishCtx)
		wsErr := workspace.cleanup()
		custody.Complete()
		return errors.Join(jobErr, wsErr)
	}

	proc := &linuxWorkerProcess{
		cancel: cancelWorker,
		cmd:    command,
		stdin:  stdinPipe,
		stdout: stdoutPipe,
		job:    job,
	}

	return proc, cleanup, nil
}

// OperationBinaryUsable reports whether cfg names a binary the launcher would
// accept now: present, owner-only writable, executable and matching its pinned
// digest. The node uses it to choose between adapter binaries at startup; the
// launch itself still verifies the descriptor it executes.
func OperationBinaryUsable(ctx context.Context, cfg OperationProcessConfig) bool {
	file, _, err := openOperationBinary(ctx, cfg)
	if err != nil {
		return false
	}
	_ = file.Close()
	return true
}
