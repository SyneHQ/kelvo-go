// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestSheetsCacheGrantIsPerTeamAndSheetsOnly(t *testing.T) {
	cache := &SheetsCacheConfig{Directory: "/var/lib/kelvo/sheets-cache", MaxMB: 4096}
	args, env, dir := sheetsCacheGrant(cache, "google_sheets", "team-a")
	if !sheetsCacheSubdir.MatchString(filepath.Base(dir)) || filepath.Dir(dir) != cache.Directory ||
		!slices.Equal(args, []string{"--cache", dir}) ||
		!slices.Equal(env, []string{"KELVO_SHEETS_CACHE_DIR=" + dir, "KELVO_SHEETS_CACHE_MAX_MB=1024"}) {
		t.Fatalf("sheets grant = %v %v %q", args, env, dir)
	}
	if _, _, other := sheetsCacheGrant(cache, "google_sheets", "team-b"); other == dir {
		t.Fatal("two teams share a cache directory")
	}
	for _, engine := range []string{"postgres", "mysql", "duckdb", ""} {
		if args, env, dir := sheetsCacheGrant(cache, engine, "team-a"); args != nil || env != nil || dir != "" {
			t.Fatalf("%s got cache grant %v %v", engine, args, env)
		}
	}
	if args, _, _ := sheetsCacheGrant(nil, "google_sheets", "team-a"); args != nil {
		t.Fatal("unconfigured cache granted")
	}
	if args, _, _ := sheetsCacheGrant(cache, "google_sheets", ""); args != nil {
		t.Fatal("teamless operation granted")
	}
	if _, env, _ := sheetsCacheGrant(&SheetsCacheConfig{Directory: "/c", MaxMB: 256}, "google_sheets", "t"); env[1] != "KELVO_SHEETS_CACHE_MAX_MB=256" {
		t.Fatalf("per-team cap = %v", env)
	}
}

func TestSweepSheetsCache(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	team := func(name string, age time.Duration, mb int) string {
		dir := sheetsCacheTeamDirectory(root, name)
		if err := os.MkdirAll(filepath.Join(dir, "sheet"), 0700); err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(dir, "sheet", "snapshot")
		if err := os.WriteFile(file, make([]byte, mb<<20), 0600); err != nil {
			t.Fatal(err)
		}
		for _, p := range []string{file, filepath.Join(dir, "sheet"), dir} {
			_ = os.Chtimes(p, now.Add(-age), now.Add(-age))
		}
		return dir
	}
	stale := team("stale", 8*24*time.Hour, 1)
	oldest := team("oldest", 3*time.Hour, 40)
	older := team("older", 2*time.Hour, 40)
	busy := team("busy", time.Minute, 40)
	stranger := filepath.Join(root, "not-a-team")
	_ = os.Mkdir(stranger, 0700)
	_ = os.Chtimes(stranger, now.Add(-30*24*time.Hour), now.Add(-30*24*time.Hour))

	// 120 MiB live against a 64 MiB budget: drop least recent until it fits.
	if err := SweepSheetsCache(SheetsCacheConfig{Directory: root, MaxMB: 64}, now); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]bool{stale: false, oldest: false, older: false, busy: true, stranger: true} {
		if _, err := os.Stat(path); (err == nil) != want {
			t.Errorf("%s exists=%v want %v", path, err == nil, want)
		}
	}
}
