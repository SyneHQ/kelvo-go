// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// CapacityError is a RESOURCE_EXHAUSTED refusal because the node was out of
// worker capacity right now: the same read can succeed when it is retried. Its
// public error is unchanged; the type is the marker that tells a node and
// gateway to answer retryably. Limits a read hits (rows, bytes, memory, a slow
// consumer) are plain RESOURCE_EXHAUSTED errors and are never retryable.
type CapacityError struct {
	public *query.Error
	after  time.Duration
}

func (e *CapacityError) Error() string { return e.public.Error() }
func (e *CapacityError) Unwrap() error { return e.public }

// RetryAfter is the wait the refusal advises.
func (e *CapacityError) RetryAfter() time.Duration { return e.after }

// NewCapacityError builds the refusal for a read the node had no capacity for.
func NewCapacityError(message string, after time.Duration) error {
	return &CapacityError{public: &query.Error{Code: "RESOURCE_EXHAUSTED", Message: message}, after: after}
}

// CapacityRetryAfter reports whether err is a capacity refusal and how long to
// wait before retrying.
func CapacityRetryAfter(err error) (time.Duration, bool) {
	var capacity *CapacityError
	if errors.As(err, &capacity) {
		return capacity.after, true
	}
	return 0, false
}
