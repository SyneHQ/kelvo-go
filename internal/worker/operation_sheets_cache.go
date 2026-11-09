// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"time"
)

// SheetsCacheConfig is a node-owned, persistent directory where Google Sheets
// adapter processes keep version-checked spreadsheet snapshots. It is not
// operation scratch: it is not reserved per operation and never wiped by
// workspace cleanup or quarantine. Each team gets its own subdirectory.
type SheetsCacheConfig struct {
	Directory string `yaml:"directory"`
	MaxMB     int    `yaml:"max_mb"`
}

const (
	sheetsCacheTeamMaxMB = 1024
	sheetsCacheStale     = 7 * 24 * time.Hour
	sheetsCacheBusy      = 10 * time.Minute
)

var sheetsCacheSubdir = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (c SheetsCacheConfig) Validate() error {
	if !filepath.IsAbs(c.Directory) || filepath.Clean(c.Directory) != c.Directory || c.Directory == "/" || len(c.Directory) > 4096 || c.MaxMB < 64 || c.MaxMB > 1<<20 {
		return fmt.Errorf("invalid sheets cache configuration")
	}
	return nil
}

func sheetsCacheTeamDirectory(root, team string) string {
	sum := sha256.Sum256([]byte("kelvo.sheets.cache.team.v1\x00" + team))
	return filepath.Join(root, hex.EncodeToString(sum[:]))
}

// sheetsCacheGrant returns the launcher arguments, environment and team
// directory for one Google Sheets operation. Other engines, and operations
// without a team, get nothing.
func sheetsCacheGrant(cache *SheetsCacheConfig, engine, team string) (args, env []string, directory string) {
	if cache == nil || engine != "google_sheets" || team == "" {
		return nil, nil, ""
	}
	directory = sheetsCacheTeamDirectory(cache.Directory, team)
	return []string{"--cache", directory}, []string{"KELVO_SHEETS_CACHE_DIR=" + directory, fmt.Sprintf("KELVO_SHEETS_CACHE_MAX_MB=%d", min(cache.MaxMB, sheetsCacheTeamMaxMB))}, directory
}

// SweepSheetsCache removes team subdirectories untouched for 7 days, then the
// least recently modified ones until the total fits max_mb. Subdirectories
// modified in the last 10 minutes are kept: an operation may be using them.
func SweepSheetsCache(cache SheetsCacheConfig, now time.Time) error {
	entries, err := os.ReadDir(cache.Directory)
	if err != nil {
		return err
	}
	type team struct {
		path   string
		newest time.Time
		bytes  int64
	}
	var teams []team
	var total int64
	var errs []error
	for _, entry := range entries {
		if !entry.IsDir() || !sheetsCacheSubdir.MatchString(entry.Name()) {
			continue
		}
		t := team{path: filepath.Join(cache.Directory, entry.Name())}
		// ponytail: full walk per sweep; fine at hourly cadence and 1 GiB per team.
		_ = filepath.WalkDir(t.path, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return nil
			}
			if relDepth(t.path, path) <= 2 && info.ModTime().After(t.newest) {
				t.newest = info.ModTime()
			}
			if !d.IsDir() {
				t.bytes += info.Size()
			}
			return nil
		})
		if now.Sub(t.newest) > sheetsCacheStale {
			errs = append(errs, os.RemoveAll(t.path))
			continue
		}
		teams = append(teams, t)
		total += t.bytes
	}
	slices.SortFunc(teams, func(a, b team) int { return a.newest.Compare(b.newest) })
	for _, t := range teams {
		if total <= int64(cache.MaxMB)<<20 {
			break
		}
		if now.Sub(t.newest) < sheetsCacheBusy {
			continue
		}
		if err := os.RemoveAll(t.path); err != nil {
			errs = append(errs, err)
			continue
		}
		total -= t.bytes
	}
	return errors.Join(errs...)
}

func relDepth(root, path string) int {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." {
		return 0
	}
	n := 1
	for _, r := range rel {
		if r == filepath.Separator {
			n++
		}
	}
	return n
}
