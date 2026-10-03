// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSourceHealthBoundedConfigurationAndNoRequestGrowth(t *testing.T) {
	cfg := SourceHealthConfig{ObservationTTL: time.Minute}
	for _, ttl := range []time.Duration{0, -time.Second, time.Second - time.Nanosecond, 24*time.Hour + time.Nanosecond} {
		if _, err := NewSourceHealth(SourceHealthConfig{ObservationTTL: ttl}, nil); err == nil {
			t.Fatalf("accepted ttl %s", ttl)
		}
	}
	for _, ids := range [][]string{{"bad.source"}, {""}, {strings.Repeat("a", 64)}, {"ok", "ok"}} {
		if _, err := NewSourceHealth(cfg, ids); err == nil {
			t.Fatal("accepted invalid identities")
		}
	}
	ids := make([]string, MaxHealthSources+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("source_%d", i)
	}
	if _, err := NewSourceHealth(cfg, ids); err == nil {
		t.Fatal("unbounded source registry")
	}
	h, err := NewSourceHealth(cfg, ids[:MaxHealthSources])
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		if h.Observe(fmt.Sprintf("unconfigured_%d", i), SourceSuccess) {
			t.Fatal("request grew registry")
		}
	}
	if h.Observe(ids[0], SourceOutcome(255)) {
		t.Fatal("invalid enum accepted")
	}
	if len(h.entries) != MaxHealthSources || len(h.Entries()) != MaxHealthSources {
		t.Fatal("registry size changed")
	}
	var disabled *SourceHealth
	if disabled.Observe("source", SourceSuccess) || len(disabled.Entries()) != 0 {
		t.Fatal("nil registry not disabled")
	}
}

func TestSourceHealthExpiryAndDetachedSnapshots(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	ids := []string{"zeta", "alpha"}
	h, err := NewSourceHealth(SourceHealthConfig{ObservationTTL: time.Minute}, ids)
	if err != nil {
		t.Fatal(err)
	}
	h.now = func() time.Time { return now }
	ids[0] = "mutated"
	entries := h.Entries()
	if entries[0].ID != "alpha" || entries[1].ID != "zeta" || entries[0].State != "unknown" || entries[0].LastObserved != nil {
		t.Fatalf("initial state: %+v", entries)
	}
	h.Observe("alpha", SourceSuccess)
	now = now.Add(30 * time.Second)
	entries = h.Entries()
	if entries[0].State != "succeeded" || entries[0].Category != "none" || entries[0].AgeNS != int64(30*time.Second) {
		t.Fatalf("fresh state: %+v", entries[0])
	}
	*entries[0].LastObserved = time.Time{}
	entries[0].State = "mutated"
	if h.Entries()[0].LastObserved.IsZero() || h.Entries()[0].State != "succeeded" {
		t.Fatal("snapshot mutated registry")
	}
	now = now.Add(30 * time.Second)
	entries = h.Entries()
	if entries[0].State != "unknown" || entries[0].Category != "none" || entries[0].LastObserved == nil {
		t.Fatalf("expiry boundary: %+v", entries[0])
	}
	h.Observe("alpha", SourceAccessFailure)
	if h.Entries()[0].Category != "access" {
		t.Fatal("new observation did not replace stale state")
	}
	now = now.Add(-time.Second)
	if h.Entries()[0].AgeNS != 0 {
		t.Fatal("negative observation age")
	}
}

func TestSourceHealthFixedCategoriesAndConcurrentSnapshots(t *testing.T) {
	h, err := NewSourceHealth(SourceHealthConfig{ObservationTTL: time.Hour}, []string{"warehouse"})
	if err != nil {
		t.Fatal(err)
	}
	for outcome, category := range map[SourceOutcome]string{SourceSuccess: "none", SourceAccessFailure: "access", SourceUnavailable: "unavailable", SourceQueryFailure: "query"} {
		h.Observe("warehouse", outcome)
		entry := h.Entries()[0]
		if entry.Category != category || (entry.State == "succeeded") != (outcome == SourceSuccess) {
			t.Fatalf("outcome %d: %+v", outcome, entry)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				h.Observe("warehouse", SourceOutcome(i%4))
				_ = h.Entries()
			}
		}(i)
	}
	wg.Wait()
	data, err := json.Marshal(h.Entries())
	if err != nil || len(data) > 512 {
		t.Fatalf("unbounded diagnostic: %d, %v", len(data), err)
	}
	if strings.Contains(string(data), "sql") || strings.Contains(string(data), "error_message") {
		t.Fatal("unexpected diagnostic fields")
	}
}
