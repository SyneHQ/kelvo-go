// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// incrementalRequest wraps the dataset query as
//
//	SELECT * FROM (<query>) AS kelvo_incr WHERE "<column>" > <literal>
//
// The literal is rendered only from the snapshot's own covered_until (a decimal
// integer or RFC3339Nano time), never from caller input. A non-empty reason
// means the request cannot be wrapped safely and the caller must refresh fully.
func incrementalRequest(request query.Request, column, coveredUntil string) (query.Request, string) {
	if request.Mode != "federated" || request.Mongo != nil || len(request.Parameters) != 0 {
		return request, "only federated SQL refreshes can be wrapped"
	}
	sql := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(request.SQL), ";"))
	if sql == "" || strings.Contains(sql, ";") {
		return request, "refresh SQL is not a single statement"
	}
	literal, reason := incrementalLiteral(coveredUntil)
	if reason != "" {
		return request, reason
	}
	quoted := `"` + strings.ReplaceAll(column, `"`, `""`) + `"`
	request.SQL = "SELECT * FROM (\n" + sql + "\n) AS kelvo_incr WHERE " + quoted + " > " + literal
	if err := query.ValidateRequest(request); err != nil {
		return request, "wrapped refresh query is not valid"
	}
	return request, ""
}

func incrementalLiteral(coveredUntil string) (string, string) {
	if v, err := strconv.ParseInt(coveredUntil, 10, 64); err == nil {
		return strconv.FormatInt(v, 10), ""
	}
	t, err := time.Parse(time.RFC3339Nano, coveredUntil)
	if err != nil {
		return "", "covered_until has no recognised type"
	}
	if t.Nanosecond()%1000 != 0 {
		return "", "covered_until is finer than microseconds"
	}
	// ponytail: naive TIMESTAMP literal assumes a UTC session for tz-aware columns.
	return "TIMESTAMP '" + t.UTC().Format("2006-01-02 15:04:05.000000") + "'", ""
}

// incrementalDecision reports whether this refresh may append to previous. A
// non-empty reason explains the fall back to a full refresh. have is false when
// there is no previous generation; lastFullAt is that generation's last_full_at.
func incrementalDecision(d catalog.Dataset, previous Snapshot, have bool, lastFullAt, fingerprint string, now time.Time) (bool, string) {
	switch {
	case d.Incremental == nil:
		return false, ""
	case !have:
		return false, "no previous generation"
	case previous.Fingerprint != fingerprint:
		return false, "dataset fingerprint changed"
	case previous.CoveredUntil == "":
		return false, "previous generation has no covered_until"
	case lastFullAt == "":
		return false, "previous generation has no last_full_at"
	}
	last, err := time.Parse(time.RFC3339Nano, lastFullAt)
	if err != nil || last.After(now) {
		return false, "last_full_at is unusable"
	}
	if now.Sub(last) >= d.Incremental.FullEvery {
		return false, "full_every elapsed"
	}
	return true, ""
}
