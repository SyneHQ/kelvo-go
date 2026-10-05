// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// Keep the backend result until command cleanup finishes. A candidate snapshot
// accompanying an error does not establish which pointer storage accepted.
type accelerationRestoreResult struct {
	snapshot acceleration.Snapshot
	err      error
}

func (r *accelerationRestoreResult) outcome() acceleration.RestoreOutcome {
	if r == nil {
		return acceleration.RestoreNotAttempted
	}
	if r.err != nil {
		return acceleration.RestoreOutcomeOf(r.err)
	}
	if r.snapshot.Generation == "" {
		return acceleration.RestoreUnknown
	}
	// Successful restore proves this observation, including for a no-op. It
	// does not prove this command wrote the pointer or that it remains current.
	return acceleration.RestoreTargetObserved
}

func finishRestoreCommand(ctx context.Context, operationErr error, result *accelerationRestoreResult, closeManager func() error, closeRuntime func(context.Context) error, publish func() error) error {
	err := finishAccelerationCommand(ctx, operationErr, closeManager, closeRuntime, publish)
	if err == nil {
		return nil
	}
	outcome := result.outcome()
	code, message := "RESTORE_FAILED", "Restore outcome: not_attempted; the pointer swap was not attempted"
	switch outcome {
	case acceleration.RestoreNotPublished:
		code, message = "RESTORE_CONFLICT", "Restore outcome: not_published; the pointer swap conflicted; inspect the current generation before retrying"
	case acceleration.RestoreVerifiedNoOp:
		code, message = "RESTORE_FINALIZATION_FAILED", "Restore outcome: verified_noop; the target was already current, but command finalization failed"
	case acceleration.RestoreTargetObserved:
		code, message = "RESTORE_FINALIZATION_FAILED", "Restore outcome: target_observed; the target pointer was observed, but the command did not finish successfully; inspect the current generation before retrying"
	case acceleration.RestoreUnknown:
		code, message = "RESTORE_PUBLICATION_UNCERTAIN", "Restore outcome: unknown; inspect the current generation before retrying"
	default:
		if errors.Is(err, context.DeadlineExceeded) {
			code = "DEADLINE_EXCEEDED"
		} else if errors.Is(err, context.Canceled) {
			code = "CANCELLED"
		}
	}
	return &restoreCommandError{public: &query.Error{Code: code, Message: message}, cause: acceleration.WithRestoreOutcome(err, outcome)}
}

// Both direct printing and PublicError select the sanitized message. The cause
// remains available to errors.Is/As without printing provider keys or responses.
type restoreCommandError struct {
	public *query.Error
	cause  error
}

func (e *restoreCommandError) Error() string   { return e.public.Message }
func (e *restoreCommandError) Unwrap() []error { return []error{e.public, e.cause} }
