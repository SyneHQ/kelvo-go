// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

func TestMetricsCollectionDefaultsAndExplicitDisable(t *testing.T) {
	yes, no := true, false
	for _, test := range []struct {
		name    string
		config  *MetricsConfig
		enabled bool
	}{
		{name: "absent", enabled: true},
		{name: "empty", config: &MetricsConfig{}, enabled: true},
		{name: "enabled", config: &MetricsConfig{Enabled: &yes}, enabled: true},
		{name: "disabled", config: &MetricsConfig{Enabled: &no}},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := test.config.NewRegistry()
			if (registry != nil) != test.enabled {
				t.Fatalf("registry presence = %v, want %v", registry != nil, test.enabled)
			}
			// Refresh/query lifecycle calls remain safe without a registry and
			// must not silently create collection when explicitly disabled.
			registry.Observe(telemetry.KindQuery, telemetry.OutcomeSuccess, time.Millisecond, time.Second)
			registry.Reject(telemetry.KindRefresh, telemetry.RejectionCapacity)
			registry.ObservePhases(telemetry.KindQuery, telemetry.PhaseTimings{}, time.Second)
			snapshot := registry.Snapshot()
			if !test.enabled {
				if snapshot != (telemetry.Snapshot{}) {
					t.Fatal("disabled collection returned observations")
				}
				return
			}
			if snapshot.Outcomes[telemetry.KindQuery][telemetry.OutcomeSuccess] != 1 || snapshot.Rejections[telemetry.KindRefresh][telemetry.RejectionCapacity] != 1 {
				t.Fatal("enabled collection lost lifecycle observations")
			}
		})
	}
}

func TestNodeMetricsYAMLControlsAuthenticatedRoute(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sandbox"), []byte("fixture"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "node.yml")
	for _, tc := range []struct {
		name, block    string
		valid, enabled bool
	}{
		{"absent", "", true, true},
		{"empty", "metrics: {}\n", true, true},
		{"enabled", "metrics: {enabled: true}\n", true, true},
		{"disabled", "metrics: {enabled: false}\n", true, false},
		{"unknown", "metrics: {labels: true}\n", false, false},
		{"invalid boolean", "metrics: {enabled: 4}\n", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(resourceNodeYAML+tc.block), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadNode(path)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
			if err != nil {
				return
			}
			cfg.RuntimeMetrics = cfg.Metrics.NewRegistry()
			node := diagnosticsNode(t)
			node.cfg.RuntimeMetrics = cfg.RuntimeMetrics
			node.cfg.RuntimeMetrics.Observe(telemetry.KindQuery, telemetry.OutcomeSuccess, time.Millisecond, time.Second)
			response := httptest.NewRecorder()
			node.ServeHTTP(response, nodeRequest(http.MethodGet, "/metrics"))
			expected := http.StatusNotFound
			if tc.enabled {
				expected = http.StatusOK
			}
			if response.Code != expected {
				t.Fatalf("metrics route=%d want %d", response.Code, expected)
			}
			response = httptest.NewRecorder()
			node.ServeHTTP(response, nodeRequest(http.MethodGet, "/resources"))
			if response.Code != http.StatusOK {
				t.Fatal("metrics toggle disabled resource diagnostics")
			}
		})
	}
}
