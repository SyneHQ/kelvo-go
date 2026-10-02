// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import (
	"errors"
	"strings"
	"sync"
	"time"
)

// HistoryConfig is opt-in. An absent configuration disables allocation; an
// explicitly present configuration must provide both bounded limits.
type HistoryConfig struct {
	MaxEntries int           `yaml:"max_entries"`
	TTL        time.Duration `yaml:"ttl"`
}

func (c HistoryConfig) Validate() error {
	if c.MaxEntries < 1 || c.MaxEntries > 1024 || c.TTL <= 0 || c.TTL > 24*time.Hour {
		return errors.New("history requires max_entries 1..1024 and ttl greater than zero through 24h")
	}
	return nil
}

// HistoryEntry records one completed node execution/transfer. Success means
// result-ready at the node, not durable gateway/client receipt. It is not an
// audit log or a complete distributed query lifecycle. Only generated query IDs
// and fixed outcomes/categories may be stored; error text and SQL are absent.
type HistoryEntry struct {
	QueryID    string    `json:"query_id"`
	Outcome    string    `json:"outcome"`
	Category   string    `json:"category"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	DurationNS int64     `json:"duration_ns"`
}
type historySlot struct {
	entry      HistoryEntry
	recordedAt time.Time
}
type History struct {
	mu           sync.Mutex
	slots        []historySlot
	start, count int
	ttl          time.Duration
	now          func() time.Time
}

func NewHistory(cfg HistoryConfig) (*History, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &History{slots: make([]historySlot, cfg.MaxEntries), ttl: cfg.TTL, now: time.Now}, nil
}
func validHistoryEntry(e HistoryEntry) bool {
	if len(e.QueryID) == 0 || len(e.QueryID) > 128 {
		return false
	}
	for _, c := range e.QueryID {
		if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '-' && c != '_' {
			return false
		}
	}
	switch e.Outcome {
	case "success", "error", "canceled":
	default:
		return false
	}
	switch e.Category {
	case "none", "unknown", "schema", "configuration", "access", "resource", "timeout", "canceled", "unavailable", "internal":
	default:
		return false
	}
	if (e.Outcome == "success") != (e.Category == "none") {
		return false
	}
	return !e.StartedAt.IsZero() && !e.FinishedAt.IsZero() && !e.FinishedAt.Before(e.StartedAt)
}
func (h *History) prune(now time.Time) {
	for h.count > 0 && !h.slots[h.start].recordedAt.Add(h.ttl).After(now) {
		h.slots[h.start] = historySlot{}
		h.start = (h.start + 1) % len(h.slots)
		h.count--
	}
}

// Append drops invalid entries and bounds both retention and query ID storage.
// It derives duration from timestamps and never retains a caller's large string
// backing allocation. Nil receivers disable recording.
func (h *History) Append(entry HistoryEntry) bool {
	if h == nil || !validHistoryEntry(entry) {
		return false
	}
	entry.QueryID = strings.Clone(entry.QueryID)
	entry.Outcome = strings.Clone(entry.Outcome)
	entry.Category = strings.Clone(entry.Category)
	entry.StartedAt = entry.StartedAt.UTC()
	entry.FinishedAt = entry.FinishedAt.UTC()
	entry.DurationNS = entry.FinishedAt.Sub(entry.StartedAt).Nanoseconds()
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	h.prune(now)
	if h.count == len(h.slots) {
		h.slots[h.start] = historySlot{}
		h.start = (h.start + 1) % len(h.slots)
		h.count--
	}
	index := (h.start + h.count) % len(h.slots)
	h.slots[index] = historySlot{entry: entry, recordedAt: now}
	h.count++
	return true
}

// Entries returns oldest-to-newest copies after TTL pruning. Response rendering
// occurs outside the lock. Empty histories are an empty JSON array, not null.
func (h *History) Entries() []HistoryEntry {
	if h == nil {
		return []HistoryEntry{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.prune(h.now())
	entries := make([]HistoryEntry, h.count)
	for i := range entries {
		entries[i] = h.slots[(h.start+i)%len(h.slots)].entry
	}
	return entries
}
