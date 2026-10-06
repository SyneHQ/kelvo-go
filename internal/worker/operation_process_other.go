//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"os"
)

type operationBinaryIdentity struct{}

func openOperationBinary(context.Context, OperationProcessConfig) (*os.File, operationBinaryIdentity, error) {
	return nil, operationBinaryIdentity{}, operationFailure("CONFIGURATION_ERROR")
}
func operationBinaryCurrent(*os.File, operationBinaryIdentity) error {
	return operationFailure("CONFIGURATION_ERROR")
}
