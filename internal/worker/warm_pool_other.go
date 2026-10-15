//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func (l *defaultWorkerLauncher) Launch(ctx context.Context, e *Executor, cfg OperationProcessConfig, limits query.Limits) (WorkerProcess, func() error, error) {
	return nil, nil, errors.New("warm_pool: default launcher requires Linux environment")
}

// NewServeLauncher exists for portability; the serve launcher requires Linux.
func NewServeLauncher(poolSize int) WorkerLauncher { return &defaultWorkerLauncher{} }

// OperationBinaryUsable is false off Linux: warm workers need the Linux launcher.
func OperationBinaryUsable(context.Context, OperationProcessConfig) bool { return false }
