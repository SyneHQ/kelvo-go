// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestCSVOptionsBoundsAndStrictDecimalPair(t *testing.T) {
	for _, pair := range [][2]string{{"262144", "1"}, {"1048576", "262144"}, {"268435456", "67108864"}} {
		s := Source{Type: "csv", Options: map[string]string{"buffer_size": pair[0], "maximum_line_size": pair[1]}}
		if options, err := s.CSVOptions(); err != nil || options == nil {
			t.Fatalf("valid CSV options rejected: %v", err)
		}
	}
	for name, options := range map[string]map[string]string{
		"missing_line": {"buffer_size": "1048576"},
		"unknown":      {"buffer_size": "1048576", "max_line_size": "262144"},
		"skip_errors":  {"buffer_size": "1048576", "maximum_line_size": "262144", "ignore_errors": "true"},
		"too_small":    {"buffer_size": "262143", "maximum_line_size": "1"},
		"too_large":    {"buffer_size": "268435457", "maximum_line_size": "1"},
		"zero_line":    {"buffer_size": "1048576", "maximum_line_size": "0"},
		"negative":     {"buffer_size": "-1048576", "maximum_line_size": "1"},
		"relationship": {"buffer_size": "1048576", "maximum_line_size": "262145"},
		"overflow":     {"buffer_size": "18446744073709551616", "maximum_line_size": "1"},
		"units":        {"buffer_size": "1MiB", "maximum_line_size": "1"},
		"plus":         {"buffer_size": "+1048576", "maximum_line_size": "1"},
		"leading_zero": {"buffer_size": "01048576", "maximum_line_size": "1"},
		"space":        {"buffer_size": "1048576 ", "maximum_line_size": "1"},
		"sql":          {"buffer_size": "1048576); SELECT private_value", "maximum_line_size": "1"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := (Source{Type: "csv", Options: options}).CSVOptions()
			if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" || strings.Contains(err.Error(), "private_value") {
				t.Fatalf("invalid CSV options were not safely rejected: %v", err)
			}
		})
	}
	for _, source := range []Source{{Type: "csv"}, {Type: "csv", Options: map[string]string{}}, {Type: "mysql", Options: map[string]string{"vendor": "value"}}, {Type: "csv", Adapter: "custom", Options: map[string]string{"vendor": "value"}}} {
		if options, err := source.CSVOptions(); err != nil || options != nil {
			t.Fatalf("omitted or external options changed: %v", err)
		}
	}
}

func TestCSVOptionsLoadAndFingerprint(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "events.csv"), []byte("n\n3\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "kelvo.yml")
	base := "sources:\n  - id: events\n    type: csv\n    path: events.csv\n"
	for _, suffix := range []string{"", "    options:\n      buffer_size: '1048576'\n      maximum_line_size: '262144'\n"} {
		if err := os.WriteFile(path, []byte(base+suffix), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(path, []byte(base+"    options:\n      buffer_size: '1048576'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" {
		t.Fatalf("Load accepted unpaired CSV options: %v", err)
	}
	c := Config{Sources: []Source{{ID: "events", Type: "csv", Path: filepath.Join(dir, "events.csv")}}, Acceleration: &AccelerationConfig{TenantID: "tenant-a", Datasets: []Dataset{{ID: "snapshot", AuthorizationVersion: "v1", Query: query.Request{Mode: "federated", Sources: []string{"events"}, SQL: "SELECT * FROM events"}}}}}
	original, err := c.DatasetFingerprint("snapshot")
	if err != nil {
		t.Fatal(err)
	}
	c.Sources[0].Options = map[string]string{"buffer_size": "1048576", "maximum_line_size": "262144"}
	changed, err := c.DatasetFingerprint("snapshot")
	if err != nil || changed == original {
		t.Fatalf("CSV parsing options did not invalidate snapshot identity: %v", err)
	}
}
