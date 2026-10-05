// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"go.yaml.in/yaml/v3"
)

// Operator output certifies completed work. Keep it behind both owned cleanup
// paths, including when runtime construction failed after acquiring resources.
func finishAccelerationCommand(ctx context.Context, resultErr error, closeManager func() error, closeRuntime func(context.Context) error, publish func() error) error {
	if closeManager != nil {
		if err := closeManager(); err != nil {
			resultErr = errors.Join(resultErr, query.NewError("UNAVAILABLE", "Acceleration storage shutdown remains uncertain"))
		}
	}
	if closeRuntime != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := closeRuntime(cleanup)
		cancel()
		if err != nil {
			resultErr = errors.Join(resultErr, query.NewError("UNAVAILABLE", "Protected object runtime shutdown remains uncertain"))
		}
	}
	if resultErr != nil || publish == nil {
		return resultErr
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return publish()
}

func encodeAccelerationResult(output io.Writer, value any) error {
	encoder := yaml.NewEncoder(output)
	return errors.Join(encoder.Encode(value), encoder.Close())
}
