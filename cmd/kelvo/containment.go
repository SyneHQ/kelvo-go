// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"io"
	"os/exec"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

// Controls being writable does not prove clone3's common-ancestor permission.
// Before binding/advertising a node, run only our known version entrypoint in a
// disposable group. No shell, credentials, or query data enter this probe.
func verifyContainmentStartup(parent context.Context, executor *worker.Executor) (resultErr error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	reservation, err := executor.ResourcePool.Acquire(ctx, admission.Request{MemoryBytes: (int64(executor.Limits.MemoryMB) << 20) + executor.ResourceOverheadBytes})
	if err != nil {
		return err
	}
	custody, _ := containment.NewCustody(reservation.Release)
	defer custody.Complete()
	hold, _ := custody.Hold()
	limits, err := executor.ContainmentBudget.ProcessLimits(int64(executor.Limits.MemoryMB), executor.Limits.Threads)
	if err != nil {
		hold()
		return err
	}
	job, err := executor.Containment.PrepareProcess(limits, hold)
	if err != nil {
		hold()
		return query.NewError("CONFIGURATION_ERROR", "Worker containment startup probe could not prepare")
	}
	defer func() {
		if _, err := job.Finish(context.Background()); err != nil {
			resultErr = query.NewError("CONFIGURATION_ERROR", "Worker containment startup cleanup is uncertain")
		}
	}()
	command := exec.CommandContext(ctx, executor.Binary, "version")
	command.Env = []string{"GOMAXPROCS=1"}
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := job.Start(command); err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Worker containment requires usable pre-start cgroup placement inside the delegated hierarchy")
	}
	if err := job.Wait(); err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Worker containment startup probe failed")
	}
	return nil
}
