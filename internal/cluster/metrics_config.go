// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import "github.com/SYNEHQ/kelvo-go/internal/telemetry"

// MetricsConfig controls aggregate node lifecycle collection. A missing block
// or enabled field preserves the existing enabled default. Tracing, history,
// source health and security audit configuration remain independent.
type MetricsConfig struct {
	Enabled *bool `yaml:"enabled,omitempty"`
}

// NewRegistry returns nil only for an explicit enabled: false. Registry methods
// accept a nil receiver, and node metrics routing is already conditional on a
// non-nil runtime registry.
func (c *MetricsConfig) NewRegistry() *telemetry.Registry {
	if c != nil && c.Enabled != nil && !*c.Enabled {
		return nil
	}
	return telemetry.New()
}
