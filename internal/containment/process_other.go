//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"os/exec"
	"time"
)

func commandExit(command *exec.Cmd) (ChildExit, bool) {
	if command.ProcessState == nil {
		return ChildExit{}, false
	}
	return ChildExit{Code: command.ProcessState.ExitCode()}, true
}

func (p *delegatedProcess) Terminate(grace time.Duration) error {
	if grace < 0 || grace > 30*time.Second {
		return ErrInvalid
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.cleanupTimeout)
	defer cancel()
	_, err := p.group.Finish(ctx)
	return err
}

func (*Manager) PrepareProcess(*Custody, Limits, func()) (Process, error) {
	return nil, ErrUnsupported
}
