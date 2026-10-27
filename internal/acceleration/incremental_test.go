// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestIncrementalRequestBuildsTypedPredicate(t *testing.T) {
	base := query.Request{SQL: "SELECT id, ts FROM events;\n", Mode: "federated", Sources: []string{"pg"}}
	got, reason := incrementalRequest(base, `t"s`, "42", 0)
	if reason != "" || got.SQL != "SELECT * FROM (\nSELECT id, ts FROM events\n) AS kelvo_incr WHERE \"t\"\"s\" >= 42" {
		t.Fatalf("int: %q %q", got.SQL, reason)
	}
	got, reason = incrementalRequest(base, "ts", "2026-03-01T10:20:30.123456Z", 0)
	if reason != "" || !strings.HasSuffix(got.SQL, `"ts" >= TIMESTAMP '2026-03-01 10:20:30.123456'`) {
		t.Fatalf("time: %q %q", got.SQL, reason)
	}
	got, reason = incrementalRequest(base, "ts", "2026-03-01T10:20:30.123456Z", 10*time.Minute)
	if reason != "" || !strings.HasSuffix(got.SQL, `"ts" >= TIMESTAMP '2026-03-01 10:10:30.123456'`) {
		t.Fatalf("late window: %q %q", got.SQL, reason)
	}
	if _, reason := incrementalRequest(base, "id", "42", time.Minute); reason == "" {
		t.Fatal("late_window on an integer watermark must fall back")
	}
	for name, c := range map[string]struct {
		r  query.Request
		cu string
	}{
		"nanos":    {base, "2026-03-01T10:20:30.1234567Z"},
		"garbage":  {base, "1; DROP TABLE x"},
		"native":   {query.Request{SQL: "SELECT 1", Mode: "native", ConnectionID: "pg"}, "1"},
		"mongo":    {query.Request{Mode: "federated", Mongo: &query.MongoRequest{}}, "1"},
		"multi":    {query.Request{SQL: "SELECT 1; SELECT 2", Mode: "federated", Sources: []string{"pg"}}, "1"},
		"emptysql": {query.Request{SQL: " ; ", Mode: "federated"}, "1"},
	} {
		if _, reason := incrementalRequest(c.r, "ts", c.cu, 0); reason == "" {
			t.Errorf("%s must fall back", name)
		}
	}
}

func TestIncrementalDecision(t *testing.T) {
	now := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	d := catalog.Dataset{Incremental: &catalog.Incremental{Column: "ts", FullEvery: 24 * time.Hour}}
	prev := Snapshot{Fingerprint: "f", CoveredUntil: "5"}
	last := now.Add(-time.Hour).Format(time.RFC3339Nano)
	if ok, why := incrementalDecision(d, prev, true, last, "f", now); !ok {
		t.Fatalf("expected incremental: %s", why)
	}
	noCovered := prev
	noCovered.CoveredUntil = ""
	cases := map[string]func() (bool, string){
		"none":        func() (bool, string) { return incrementalDecision(d, prev, false, last, "f", now) },
		"fingerprint": func() (bool, string) { return incrementalDecision(d, prev, true, last, "g", now) },
		"covered":     func() (bool, string) { return incrementalDecision(d, noCovered, true, last, "f", now) },
		"lastfull":    func() (bool, string) { return incrementalDecision(d, prev, true, "", "f", now) },
		"elapsed":     func() (bool, string) { return incrementalDecision(d, prev, true, last, "f", now.Add(24*time.Hour)) },
	}
	for name, f := range cases {
		if ok, why := f(); ok || why == "" {
			t.Errorf("%s must force full (%v %q)", name, ok, why)
		}
	}
}
