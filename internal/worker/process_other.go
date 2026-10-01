//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import "os/exec"

// Non-Linux deployments need an external supervisor for parent-death cleanup.
func configureProcess(cmd *exec.Cmd) {}
