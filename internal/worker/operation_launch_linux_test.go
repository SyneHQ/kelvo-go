//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"os/exec"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type uncertainLaunchDomain struct{ containment.Domain }

func (d uncertainLaunchDomain) PrepareProcess(custody *containment.Custody, limits containment.Limits, release func()) (containment.Process, error) {
	process, err := d.Domain.PrepareProcess(custody, limits, release)
	if err != nil {
		return nil, err
	}
	return uncertainLaunchProcess{process}, nil
}

type uncertainLaunchProcess struct{ containment.Process }

func (p uncertainLaunchProcess) Start(ctx context.Context, command *exec.Cmd) error {
	if err := p.Process.Start(ctx, command); err != nil {
		return err
	}
	// The child really executed before this synthetic post-launch failure.
	// The boundary must remain uncertain even if cleanup later reaps it.
	return containment.ErrLaunchUncertain
}

func TestContainedOperationUncertainLaunchKeepsSourceEffectsUnknown(t *testing.T) {
	executor, manager, pool := containedExecutor(t)
	executor.Containment = uncertainLaunchDomain{manager}
	cfg := operationExecutable(t)
	for _, kind := range []operations.Kind{operations.QueryRead, operations.StatementExecute} {
		t.Run(string(kind), func(t *testing.T) {
			input := operationProcessInput(t, kind, "fixture")
			receipt, err := executor.ExecuteOperation(context.Background(), cfg, input, &workerTestSink{})
			if err == nil || receipt.Outcome == operations.Rejected {
				t.Fatal("a child that started was reported as never executed", receipt, err)
			}
			if kind.Mutating() && (receipt.Outcome != operations.OutcomeUnknown || receipt.Effect != operations.EffectUnknown || receipt.ErrorCode != "OUTCOME_UNKNOWN") {
				t.Fatal("post-launch failure falsely authorized mutation replay", receipt)
			}
			if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
				t.Fatal("known native cleanup did not release physical custody")
			}
		})
	}
}
