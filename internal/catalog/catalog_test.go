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
	config := `# Paths are relative to this file, not the query worker.
extension_directory: extensions
sources:
  - id: sales
    type: csv
    path: sales.csv
`
	if err := os.WriteFile(filepath.Join(configDir, "kelvo.yml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	loaded, err := Load(filepath.Join("examples", "kelvo.yml"))
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
	path := filepath.Join(dir, "private-config-marker.yml")
	if _, err := Load(path); err == nil || strings.Contains(err.Error(), "private-config-marker") {
		t.Fatalf("missing configuration error leaked its path: %v", err)
	}
	if err := os.WriteFile(path, []byte("private-content-marker: not-a-real-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || strings.Contains(err.Error(), "private-content-marker") || strings.Contains(err.Error(), "not-a-real-secret") {
		t.Fatalf("invalid configuration error leaked its contents: %v", err)
	}
}

func TestYAMLConfigurationRejectsInvalidDocuments(t *testing.T) {
	tests := map[string]string{
		"empty":                "# only a comment\n",
		"null":                 "null\n",
		"sequence root":        "[]\n",
		"scalar root":          "configuration\n",
		"unknown field":        "soruces: []\n",
		"unknown source field": "sources:\n  - id: db\n    type: postgres\n    dsn_env: DB_DSN\n    typo: value\n",
		"duplicate field":      "sources: []\nsources: []\n",
		"multiple documents":   "sources: []\n---\nsources: []\n",
		"second null document": "sources: []\n---\nnull\n",
		"invalid syntax":       "sources: [\n",
		"oversized comments":   "sources: []\n#" + strings.Repeat("x", maxConfigBytes),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "kelvo.yml")
			if err := os.WriteFile(path, []byte(input), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestYAMLSourceEnvironmentReferences(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kelvo.yaml")
	input := `sources:
  - id: pg
    type: postgres
    dsn_env: KELVO_POSTGRES_DSN
  - id: events
    type: clickhouse
    url_env: KELVO_CLICKHOUSE_URL
    username_env: KELVO_CLICKHOUSE_USER
    password_env: KELVO_CLICKHOUSE_PASSWORD
`
	if err := os.WriteFile(path, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Sources) != 2 || c.Sources[0].DSNEnv != "KELVO_POSTGRES_DSN" ||
		c.Sources[1].URLEnv != "KELVO_CLICKHOUSE_URL" ||
		c.Sources[1].UsernameEnv != "KELVO_CLICKHOUSE_USER" ||
		c.Sources[1].PasswordEnv != "KELVO_CLICKHOUSE_PASSWORD" {
		t.Fatalf("source environment references were not preserved: %#v", c)
	}
}
