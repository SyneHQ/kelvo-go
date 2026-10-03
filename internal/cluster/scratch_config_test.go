// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadNodeManagedScratchPath(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sandbox"), []byte("fixture"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "node.yml")
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{
		{"legacy", "", true},
		{"explicit", filepath.Join(dir, "private-scratch"), true},
		{"relative", "private-scratch", false},
		{"parent escape", "/var/tmp/../private-scratch", false},
		{"redundant separator", "/var//tmp/private-scratch", false},
		{"filesystem root", "/", false},
		{"oversize", "/" + strings.Repeat("x", 4096), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := resourceNodeYAML
			if tc.value != "" {
				text += fmt.Sprintf("scratch_directory: %q\n", tc.value)
			}
			if err := os.WriteFile(path, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadNode(path)
			if (err == nil) != tc.valid {
				t.Fatalf("accepted=%v expected=%v: %v", err == nil, tc.valid, err)
			}
			if err == nil && cfg.ScratchDirectory != tc.value {
				t.Fatal("configured scratch root was silently resolved or replaced")
			}
		})
	}
}
