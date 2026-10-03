// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"errors"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

// Observe only an actual native execution. Failures selected by parent timeout,
// source admission, IPC, sink or process cleanup must not blame the source. A
// typed child error must survive unchanged as the final result. Resource and
// configuration errors also do not prove source health. A query failure may be
// SQL-specific; diagnostics report the last operation, never a reachability SLA.
func recordSourceHealth(health *telemetry.SourceHealth, request query.Request, childError *query.Error, resultErr error, decoded bool) {
	if health == nil || request.Mode != "native" || !decoded {
		return
	}
	if resultErr == nil {
		health.Observe(request.ConnectionID, telemetry.SourceSuccess)
		return
	}
	var returned *query.Error
	if childError == nil || !errors.As(resultErr, &returned) || returned != childError {
		return
	}
	var outcome telemetry.SourceOutcome
	switch childError.Code {
	case "UNAUTHENTICATED", "PERMISSION_DENIED", "FORBIDDEN", "UNAUTHORIZED":
		outcome = telemetry.SourceAccessFailure
	case "UNAVAILABLE":
		outcome = telemetry.SourceUnavailable
	case "QUERY_FAILED":
		outcome = telemetry.SourceQueryFailure
	default:
		return
	}
	health.Observe(request.ConnectionID, outcome)
}
