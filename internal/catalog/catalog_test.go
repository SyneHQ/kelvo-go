// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRelativeConfigurationSurvivesWorkerDirectoryChange(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "examples")
	if err := os.MkdirAll(filepath.Join(configDir, "extensions"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "sales.csv"), []byte("region,amount\neast,10\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := `{"extension_directory":"extensions","sources":[{"id":"sales","type":"csv","path":"sales.csv"}]}`
	if err := os.WriteFile(filepath.Join(configDir, "kelvo.json"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	loaded, err := Load(filepath.Join("examples", "kelvo.json"))
	if err != nil {
		t.Fatal(err)
	}
	expectedSource, err := filepath.EvalSymlinks(filepath.Join(configDir, "sales.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Sources[0].Path != expectedSource || !filepath.IsAbs(loaded.Sources[0].Path) {
		t.Fatalf("source path did not resolve absolutely: %q", loaded.Sources[0].Path)
	}
	if loaded.ExtensionDirectory != filepath.Join(configDir, "extensions") || !filepath.IsAbs(loaded.ExtensionDirectory) {
		t.Fatalf("extension directory did not resolve absolutely: %q", loaded.ExtensionDirectory)
	}
	t.Chdir(t.TempDir())
	if _, err := os.ReadFile(loaded.Sources[0].Path); err != nil {
		t.Fatalf("source is unavailable from worker directory: %v", err)
	}
	if _, err := os.Stat(loaded.ExtensionDirectory); err != nil {
		t.Fatalf("extensions are unavailable from worker directory: %v", err)
	}
}

func TestConfigurationErrorsDoNotExposeContentsOrPaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "private-config-marker.json")
	if _, err := Load(path); err == nil || strings.Contains(err.Error(), "private-config-marker") {
		t.Fatalf("missing configuration error leaked its path: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"private-content-marker":"not-a-real-secret"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || strings.Contains(err.Error(), "private-content-marker") || strings.Contains(err.Error(), "not-a-real-secret") {
		t.Fatalf("invalid configuration error leaked its contents: %v", err)
	}
}
