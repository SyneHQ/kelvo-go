// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func writeFingerprintCatalog(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "private-catalog-marker.yml")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCatalogFingerprintOutputsOnlyDigestWithoutResolvingSecrets(t *testing.T) {
	const name = "KELVO_SOURCE_FINGERPRINT_TEST_DSN"
	path := writeFingerprintCatalog(t, "sources:\n  - id: sales\n    type: postgres\n    dsn_env: "+name+"\n")
	t.Setenv(name, "")
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	var first, rotated bytes.Buffer
	if err := runCatalogFingerprint([]string{"--config", path}, &first); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}\n$`).Match(first.Bytes()) {
		t.Fatal("command did not emit exactly one bare SHA-256 digest")
	}
	t.Setenv(name, "private-secret-marker-never-a-valid-dsn")
	if err := runCatalogFingerprint([]string{"--config=" + path}, &rotated); err != nil {
		t.Fatal(err)
	}
	if first.String() != rotated.String() {
		t.Fatal("changing a secret value changed the catalog fingerprint")
	}
	config, err := catalog.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := catalog.AuthorityFingerprint(config)
	if err != nil || first.String() != expected+"\n" {
		t.Fatal("CLI fingerprint differs from the loaded catalog authority")
	}
}

func TestCatalogFingerprintUsesDefaultConfigAndNormalizedPaths(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	if err := os.WriteFile("input.csv", []byte("opaque-data-not-read-by-fingerprinting"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("kelvo.yml", []byte("sources:\n  - id: sales\n    type: csv\n    path: ./input.csv\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var relative, absolute bytes.Buffer
	if err := runCatalogFingerprint(nil, &relative); err != nil {
		t.Fatal(err)
	}
	if err := runCatalogFingerprint([]string{"--config", filepath.Join(directory, "kelvo.yml")}, &absolute); err != nil {
		t.Fatal(err)
	}
	if relative.String() != absolute.String() {
		t.Fatal("equivalent configuration paths produced different fingerprints")
	}
	if err := os.WriteFile("input.csv", []byte("different-data-still-not-read"), 0600); err != nil {
		t.Fatal(err)
	}
	var changedData bytes.Buffer
	if err := runCatalogFingerprint(nil, &changedData); err != nil || changedData.String() != relative.String() {
		t.Fatal("local file contents affected the catalog fingerprint", err)
	}
}

func TestCatalogFingerprintRejectsArgumentsBeforeConfiguration(t *testing.T) {
	for _, args := range [][]string{
		{"private-positional-marker"}, {"--", "private-positional-marker"},
		{"--private-flag-marker=value"}, {"--config"}, {"--config="}, {"--config", " "},
		{"--config", "private-path-marker", "--config", "private-path-marker"},
		{"--config", "private-path-marker", "extra"}, {"-h"}, {"--help"},
	} {
		var output bytes.Buffer
		err := runCatalogFingerprint(args, &output)
		if err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" || output.Len() != 0 {
			t.Fatal("invalid arguments reached configuration or emitted output")
		}
		if strings.Contains(err.Error(), "private-") {
			t.Fatal("argument diagnostics exposed caller input")
		}
	}
}

func TestCatalogFingerprintRejectsInvalidCatalogWithoutOutput(t *testing.T) {
	for name, text := range map[string]string{
		"unknown":            "private-field-marker: private-value-marker\n",
		"invalid source":     "sources:\n  - id: private-source-marker\n    type: postgres\n",
		"multiple documents": "sources: []\n---\nsources: []\n",
		"malformed":          "sources: [private-value-marker\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := writeFingerprintCatalog(t, text)
			var output bytes.Buffer
			err := runCatalogFingerprint([]string{"--config", path}, &output)
			if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" || output.Len() != 0 {
				t.Fatal("invalid catalog emitted a fingerprint")
			}
			if strings.Contains(err.Error(), "private-") {
				t.Fatal("catalog diagnostics exposed configuration data")
			}
		})
	}
	var output bytes.Buffer
	err := runCatalogFingerprint([]string{"--config", filepath.Join(t.TempDir(), "private-missing-marker.yml")}, &output)
	if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" || output.Len() != 0 || strings.Contains(err.Error(), "private-") {
		t.Fatal("missing catalog emitted output or disclosed its path")
	}
}

type fingerprintFailWriter struct{ short bool }

func (w fingerprintFailWriter) Write(data []byte) (int, error) {
	if w.short {
		return len(data) - 1, nil
	}
	return 0, errors.New("private-output-error-marker")
}

func TestCatalogFingerprintRedactsWriterFailures(t *testing.T) {
	path := writeFingerprintCatalog(t, "sources:\n  - id: sales\n    type: postgres\n    dsn_env: KELVO_SOURCE_FINGERPRINT_TEST_DSN\n")
	for _, short := range []bool{false, true} {
		err := runCatalogFingerprint([]string{"--config", path}, fingerprintFailWriter{short: short})
		if err == nil || query.PublicError(err).Code != "UNAVAILABLE" || strings.Contains(err.Error(), "private-") {
			t.Fatal("writer failure was lost or exposed private diagnostics")
		}
	}
	if err := runCatalogFingerprint([]string{"--config", path}, nil); err == nil {
		t.Fatal("nil output accepted")
	}
}

func TestCatalogFingerprintCommandRoutesBeforeEngineSetup(t *testing.T) {
	err := run([]string{"catalog-fingerprint", "--config", filepath.Join(t.TempDir(), "missing.yml")})
	if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" {
		t.Fatal("catalog-fingerprint command was not routed to catalog loading")
	}
}
