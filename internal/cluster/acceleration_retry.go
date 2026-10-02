// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

const refreshMaxAttempts = uint64(5)
const refreshMaxRetryDelay = 5 * time.Minute

// RefreshFailure contains only a fixed category. Raw driver messages, SQL and
// connection details are deliberately neither retained nor unwrapped.
type RefreshFailure struct {
	Category  string
	Permanent bool
}

func (e *RefreshFailure) Error() string { return "refresh failed: " + e.Category }

func classifyRefreshFailure(err error) *RefreshFailure {
	category, permanent := "unknown", false
	var qe *query.Error
	if errors.As(err, &qe) {
		switch qe.Code {
		case "SCHEMA_MISMATCH":
			category, permanent = "schema", true
		case "INVALID_ARGUMENT", "CONFIGURATION_ERROR", "UNIMPLEMENTED", "NOT_SUPPORTED":
			category, permanent = "configuration", true
		case "UNAUTHENTICATED", "PERMISSION_DENIED", "FORBIDDEN", "UNAUTHORIZED":
			category, permanent = "access", true
		case "RESOURCE_EXHAUSTED":
			category, permanent = "resource", true
		case "CANCELLED", "DEADLINE_EXCEEDED":
			category = "timeout"
		case "UNAVAILABLE", "DATASET_UNAVAILABLE":
			category = "unavailable"
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		category = "timeout"
	}
	if errors.Is(err, admission.ErrDraining) || errors.Is(err, admission.ErrBusy) {
		category = "capacity"
	}
	if errors.Is(err, admission.ErrOversize) || errors.Is(err, admission.ErrInvalid) {
		category, permanent = "configuration", true
	}
	var network net.Error
	if errors.As(err, &network) {
		category = "network"
	}
	return &RefreshFailure{Category: category, Permanent: permanent}
}

// Optional provider errors can communicate a parsed delay without exposing HTTP
// headers or payloads. A value above our cap is a permanent retry-policy failure:
// we must not silently retry earlier than a provider's Retry-After requirement.
type refreshRetryAfter interface{ RetryAfter() time.Duration }

func refreshBackoff(attempt uint64, err error) (time.Duration, bool) {
	delay := refreshRetryDelay
	for n := uint64(1); n < attempt && delay < refreshMaxRetryDelay; n++ {
		delay *= 2
		if delay > refreshMaxRetryDelay {
			delay = refreshMaxRetryDelay
		}
	}
	// Equal jitter: [half the capped exponential delay, full delay].
	delay = delay/2 + time.Duration(rand.Int64N(int64(delay-delay/2)+1))
	var provider refreshRetryAfter
	if errors.As(err, &provider) {
		retryAfter := provider.RetryAfter()
		if retryAfter > refreshMaxRetryDelay {
			return 0, false
		}
		if retryAfter > delay {
			delay = retryAfter
		}
	}
	return delay, true
}
