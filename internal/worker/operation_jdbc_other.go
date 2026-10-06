//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"os"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

type operationJDBCRuntime struct{ binding string }

func openOperationJDBC(_ context.Context, cfg *OperationJDBCConfig) (*operationJDBCRuntime, error) {
	if cfg == nil {
		return nil, nil
	}
	return nil, operationFailure("CONFIGURATION_ERROR")
}
func (*operationJDBCRuntime) close()         {}
func (*operationJDBCRuntime) current() error { return operationFailure("CONFIGURATION_ERROR") }
func (*operationJDBCRuntime) selectProfile(string, int) (*adapter.JDBCRuntime, []*os.File, error) {
	return nil, nil, operationFailure("CONFIGURATION_ERROR")
}
