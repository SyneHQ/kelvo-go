// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"slices"
	"testing"
)

func TestSheetsCacheGrantOnlyForGoogleSheets(t *testing.T) {
	cache := &SheetsCacheConfig{Directory: "/var/lib/kelvo/sheets-cache", MaxMB: 4096}
	args, env := sheetsCacheGrant(cache, "google_sheets")
	if !slices.Equal(args, []string{"--cache", "/var/lib/kelvo/sheets-cache"}) ||
		!slices.Equal(env, []string{"KELVO_SHEETS_CACHE_DIR=/var/lib/kelvo/sheets-cache", "KELVO_SHEETS_CACHE_MAX_MB=4096"}) {
		t.Fatalf("sheets grant = %v %v", args, env)
	}
	for _, engine := range []string{"postgres", "mysql", "duckdb", ""} {
		if args, env := sheetsCacheGrant(cache, engine); args != nil || env != nil {
			t.Fatalf("%s got cache grant %v %v", engine, args, env)
		}
	}
	if args, env := sheetsCacheGrant(nil, "google_sheets"); args != nil || env != nil {
		t.Fatal("unconfigured cache granted")
	}
}
