// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"os"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// SecretResolver belongs to the trusted parent and sees selected references only.
// Configured failures must never silently fall back to an older environment value.
type SecretResolver interface {
	Resolve(context.Context, string) (string, bool, error)
}

func (e *Executor) resolveSecret(ctx context.Context, key string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if e.Secrets != nil {
		value, configured, err := e.Secrets.Resolve(ctx, key)
		if err != nil {
			if ctx.Err() != nil {
				return "", false, ctx.Err()
			}
			return "", false, query.NewError("CONFIGURATION_ERROR", "Selected source credential is unavailable")
		}
		if configured {
			return value, true, nil
		}
	}
	value, ok := os.LookupEnv(key)
	return value, ok, nil
}
