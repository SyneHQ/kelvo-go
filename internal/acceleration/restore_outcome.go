// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import "errors"

// RestoreOutcome describes the pointer observation, not cleanup success or
// continuing authority to read the returned snapshot.
type RestoreOutcome string

const (
	RestoreNotAttempted   RestoreOutcome = "not_attempted"
	RestoreNotPublished   RestoreOutcome = "not_published"
	RestoreVerifiedNoOp   RestoreOutcome = "verified_noop"
	RestoreTargetObserved RestoreOutcome = "target_observed"
	RestoreUnknown        RestoreOutcome = "unknown"
)

type restoreOutcomeError struct {
	outcome RestoreOutcome
	cause   error
}

func (e *restoreOutcomeError) Error() string {
	return "protected restore failed (outcome: " + string(e.outcome) + ")"
}
func (e *restoreOutcomeError) Unwrap() error { return e.cause }

// RestoreOutcomeOf follows joined cleanup errors without treating a populated
// candidate snapshot as evidence of publication. Nil has no error outcome.
func RestoreOutcomeOf(err error) RestoreOutcome {
	var failure *restoreOutcomeError
	if errors.As(err, &failure) {
		return failure.outcome
	}
	return RestoreNotAttempted
}

// WithRestoreOutcome keeps provider diagnostics out of printed errors while
// retaining errors.Is/As classification. Apply it after all cleanup errors join.
func WithRestoreOutcome(err error, outcome RestoreOutcome) error {
	if err == nil {
		return nil
	}
	switch outcome {
	case RestoreNotAttempted, RestoreNotPublished, RestoreVerifiedNoOp, RestoreTargetObserved, RestoreUnknown:
	default:
		outcome = RestoreUnknown
	}
	return &restoreOutcomeError{outcome: outcome, cause: err}
}
