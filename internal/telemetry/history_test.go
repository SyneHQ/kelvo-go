// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func historyFixtureEntry(id string) HistoryEntry {
	return HistoryEntry{QueryID: id, Outcome: "success", Category: "none", StartedAt: time.Unix(1, 0), FinishedAt: time.Unix(3, 0)}
}
func TestHistoryBoundsExpiryAndSnapshot(t *testing.T) {
	h, err := NewHistory(HistoryConfig{MaxEntries: 2, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(10, 0)
	h.now = func() time.Time { return now }
	for _, id := range []string{"a", "b", "c"} {
		if !h.Append(historyFixtureEntry(id)) {
			t.Fatal("valid entry rejected")
		}
	}
	entries := h.Entries()
	if len(entries) != 2 || entries[0].QueryID != "b" || entries[1].QueryID != "c" || entries[1].DurationNS != 2*time.Second.Nanoseconds() {
		t.Fatalf("bad ring: %+v", entries)
	}
	entries[0].QueryID = "mutated"
	if h.Entries()[0].QueryID != "b" {
		t.Fatal("snapshot aliases ring")
	}
	now = now.Add(time.Minute)
	if len(h.Entries()) != 0 {
		t.Fatal("TTL boundary did not prune")
	}
	for _, slot := range h.slots {
		if slot.entry.QueryID != "" {
			t.Fatal("prune retained identifier")
		}
	}
	h.Append(historyFixtureEntry("d"))
	now = now.Add(time.Minute)
	h.Append(historyFixtureEntry("e"))
	if entries := h.Entries(); len(entries) != 1 || entries[0].QueryID != "e" {
		t.Fatal("append did not prune")
	}
}
func TestHistoryValidation(t *testing.T) {
	for _, cfg := range []HistoryConfig{{}, {MaxEntries: 1025, TTL: time.Hour}, {MaxEntries: 1, TTL: 25 * time.Hour}, {MaxEntries: 1, TTL: -1}} {
		if _, err := NewHistory(cfg); err == nil {
			t.Fatal("unbounded config accepted")
		}
	}
	h, _ := NewHistory(HistoryConfig{MaxEntries: 1, TTL: time.Hour})
	for _, edit := range []func(*HistoryEntry){func(e *HistoryEntry) { e.QueryID = strings.Repeat("x", 129) }, func(e *HistoryEntry) { e.QueryID = "SELECT secret" }, func(e *HistoryEntry) { e.Outcome = "raw error" }, func(e *HistoryEntry) { e.Category = "password" }, func(e *HistoryEntry) { e.StartedAt = time.Time{} }, func(e *HistoryEntry) { e.FinishedAt = e.StartedAt.Add(-time.Second) }, func(e *HistoryEntry) { e.Outcome = "error" }} {
		e := historyFixtureEntry("query-id")
		edit(&e)
		if h.Append(e) {
			t.Fatalf("invalid entry accepted: %+v", e)
		}
	}
	if len(h.Entries()) != 0 {
		t.Fatal("invalid records retained")
	}
	var disabled *History
	if disabled.Append(historyFixtureEntry("q")) || len(disabled.Entries()) != 0 {
		t.Fatal("nil history enabled")
	}
}
func TestHistoryConcurrentBounds(t *testing.T) {
	h, _ := NewHistory(HistoryConfig{MaxEntries: 32, TTL: time.Hour})
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Go(func() {
			for i := 0; i < 100; i++ {
				h.Append(historyFixtureEntry(fmt.Sprint("q-", i)))
				if len(h.Entries()) > 32 {
					t.Error("capacity exceeded")
				}
			}
		})
	}
	wg.Wait()
	if len(h.Entries()) != 32 {
		t.Fatal("unexpected final capacity")
	}
}
