// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

func recordNodeHistory(history *telemetry.History, id string, started time.Time, err error) {
	if history == nil {
		return
	}
	entry := telemetry.HistoryEntry{QueryID: id, Outcome: "success", Category: "none", StartedAt: started, FinishedAt: time.Now()}
	if err != nil {
		entry.Outcome, entry.Category = "error", "unknown"
		var qe *query.Error
		if errors.As(err, &qe) {
			switch qe.Code {
			case "SCHEMA_MISMATCH":
				entry.Category = "schema"
			case "CONFIGURATION_ERROR", "INVALID_ARGUMENT", "UNIMPLEMENTED", "NOT_SUPPORTED", "UNSUPPORTED":
				entry.Category = "configuration"
			case "PERMISSION_DENIED", "UNAUTHENTICATED", "FORBIDDEN", "UNAUTHORIZED":
				entry.Category = "access"
			case "RESOURCE_EXHAUSTED":
				entry.Category = "resource"
			case "UNAVAILABLE", "DATASET_UNAVAILABLE":
				entry.Category = "unavailable"
			case "INTERNAL":
				entry.Category = "internal"
			case "CANCELLED":
				entry.Outcome, entry.Category = "canceled", "canceled"
			case "DEADLINE_EXCEEDED":
				entry.Outcome, entry.Category = "canceled", "timeout"
			}
		}
		if errors.Is(err, context.Canceled) {
			entry.Outcome, entry.Category = "canceled", "canceled"
		}
		if errors.Is(err, context.DeadlineExceeded) {
			entry.Outcome, entry.Category = "canceled", "timeout"
		}
	}
	history.Append(entry)
}
