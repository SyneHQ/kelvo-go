// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"fmt"
	"path/filepath"
)

// SheetsCacheConfig is a node-owned, persistent directory where Google Sheets
// adapter processes keep version-checked spreadsheet snapshots. It is not
// operation scratch: it is not reserved per operation and never wiped by
// workspace cleanup or quarantine. The adapter bounds it with MaxMB.
type SheetsCacheConfig struct {
	Directory string `yaml:"directory"`
	MaxMB     int    `yaml:"max_mb"`
}

func (c SheetsCacheConfig) Validate() error {
	if !filepath.IsAbs(c.Directory) || filepath.Clean(c.Directory) != c.Directory || c.Directory == "/" || len(c.Directory) > 4096 || c.MaxMB < 64 || c.MaxMB > 1<<20 {
		return fmt.Errorf("invalid sheets cache configuration")
	}
	return nil
}

// sheetsCacheGrant returns the launcher arguments and environment that give
// one Google Sheets operation the cache. Other engines get nothing.
func sheetsCacheGrant(cache *SheetsCacheConfig, engine string) (args, env []string) {
	if cache == nil || engine != "google_sheets" {
		return nil, nil
	}
	return []string{"--cache", cache.Directory}, []string{"KELVO_SHEETS_CACHE_DIR=" + cache.Directory, fmt.Sprintf("KELVO_SHEETS_CACHE_MAX_MB=%d", cache.MaxMB)}
}
