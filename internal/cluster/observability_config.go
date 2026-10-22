// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"errors"
	"path/filepath"
	"time"
)

// ObservabilityConfig is opt-in; an absent block changes nothing.
type ObservabilityConfig struct {
	StallDump         *StallDumpConfig `yaml:"stall_dump,omitempty"`
	SlowReadThreshold time.Duration    `yaml:"slow_read_threshold,omitempty"`
}

// StallDumpConfig writes a goroutine dump when reads are in flight and none
// completed for After.
type StallDumpConfig struct {
	Dir   string        `yaml:"dir"`
	After time.Duration `yaml:"after"`
}

func (c *ObservabilityConfig) Validate() error {
	if c == nil {
		return nil
	}
	if c.SlowReadThreshold < 0 || c.SlowReadThreshold > 10*time.Minute {
		return errors.New("observability slow_read_threshold must be 0 through 10m")
	}
	if s := c.StallDump; s != nil {
		if s.After < time.Second || s.After > time.Hour {
			return errors.New("observability stall_dump after must be 1s through 1h")
		}
		if !filepath.IsAbs(s.Dir) || filepath.Clean(s.Dir) != s.Dir || s.Dir == string(filepath.Separator) || len(s.Dir) > 4096 {
			return errors.New("observability stall_dump dir must be a clean absolute directory")
		}
	}
	return nil
}
