// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import (
	"errors"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const MaxHealthSources = 256

// SourceHealthConfig enables passive, process-local observations. This TTL is
// observation age, not a guarantee that a source remains reachable until expiry.
type SourceHealthConfig struct {
	ObservationTTL time.Duration `yaml:"observation_ttl"`
}

func (c SourceHealthConfig) Validate() error {
	if c.ObservationTTL < time.Second || c.ObservationTTL > 24*time.Hour {
		return errors.New("source health observation_ttl must be between 1s and 24h")
	}
	return nil
}

type SourceOutcome uint8

const (
	SourceSuccess SourceOutcome = iota
	SourceAccessFailure
	SourceUnavailable
	SourceQueryFailure
	sourceOutcomeCount
)

// SourceStatus exposes only configured identity and fixed outcome categories.
// LastObserved and AgeNS refer to the last eligible native operation. Unknown
// means never observed or expired; it must not be interpreted as healthy.
type SourceStatus struct {
	ID           string     `json:"id"`
	State        string     `json:"state"`
	Category     string     `json:"category"`
	LastObserved *time.Time `json:"last_observed,omitempty"`
	AgeNS        int64      `json:"age_ns"`
}

type sourceHealthEntry struct {
	observed time.Time
	outcome  SourceOutcome
}

// SourceHealth has one fixed-size record per configured source. It never grows
// from request identifiers, owns no goroutine, and retains no error or SQL text.
// Every instance belongs to one tenant-bound worker node. It is not a metric
// registry; source identities must remain behind authorized node diagnostics.
type SourceHealth struct {
	mu      sync.Mutex
	entries map[string]sourceHealthEntry
	ids     []string
	ttl     time.Duration
	now     func() time.Time
}

var healthSourceID = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

func NewSourceHealth(cfg SourceHealthConfig, ids []string) (*SourceHealth, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if len(ids) > MaxHealthSources {
		return nil, errors.New("source health supports at most 256 configured sources")
	}
	h := &SourceHealth{entries: make(map[string]sourceHealthEntry, len(ids)), ttl: cfg.ObservationTTL, now: time.Now}
	for _, id := range ids {
		if !healthSourceID.MatchString(id) {
			return nil, errors.New("invalid source health identity")
		}
		if _, ok := h.entries[id]; ok {
			return nil, errors.New("duplicate source health identity")
		}
		// Do not retain a caller's large backing string or mutate their slice.
		id = strings.Clone(id)
		h.entries[id] = sourceHealthEntry{}
		h.ids = append(h.ids, id)
	}
	sort.Strings(h.ids)
	return h, nil
}

// Observe ignores unknown identities and enum values without retaining them.
// It records completion order, not start order, and never runs a health query.
func (h *SourceHealth) Observe(id string, outcome SourceOutcome) bool {
	if h == nil || outcome >= sourceOutcomeCount {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.entries[id]; !ok {
		return false
	}
	h.entries[id] = sourceHealthEntry{observed: h.now(), outcome: outcome}
	return true
}

// Entries returns bounded copies in identity order. Rendering happens after
// releasing the lock; a slow diagnostic reader cannot block query completion.
func (h *SourceHealth) Entries() []SourceStatus {
	if h == nil {
		return []SourceStatus{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	out := make([]SourceStatus, 0, len(h.ids))
	for _, id := range h.ids {
		record := h.entries[id]
		status := SourceStatus{ID: id, State: "unknown", Category: "none"}
		if !record.observed.IsZero() {
			observed := record.observed.UTC()
			status.LastObserved = &observed
			age := now.Sub(record.observed)
			if age < 0 {
				age = 0
			}
			status.AgeNS = age.Nanoseconds()
			if age < h.ttl {
				status.State = "failed"
				switch record.outcome {
				case SourceSuccess:
					status.State = "succeeded"
				case SourceAccessFailure:
					status.Category = "access"
				case SourceUnavailable:
					status.Category = "unavailable"
				case SourceQueryFailure:
					status.Category = "query"
				}
			}
		}
		out = append(out, status)
	}
	return out
}
