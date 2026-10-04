// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestAuthStateCLIArgumentsAndErrorsAreRedacted(t *testing.T) {
	for _, args := range [][]string{
		nil, {"--config"}, {"--config", ""}, {"--config", "   "},
		{"--config", "private-marker", "extra"},
		{"--config", "private-marker", "--config", "second-marker"},
		{"--reset=private-marker"}, {"--force"}, {"--overwrite"},
	} {
		var output bytes.Buffer
		err := runAuthStateInit(args, &output)
		if err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" || output.Len() != 0 || strings.Contains(err.Error(), "private-marker") {
			t.Fatalf("invalid setup arguments were accepted or disclosed: %v", args)
		}
	}
	var output bytes.Buffer
	err := runAuthStateInit([]string{"--config", filepath.Join(t.TempDir(), "private-path-marker")}, &output)
	if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" || output.Len() != 0 || strings.Contains(err.Error(), "private-path-marker") {
		t.Fatal("missing setup configuration leaked details or was accepted")
	}
	if err := runAuthStateInit([]string{"--config", "gateway.yml"}, nil); err == nil {
		t.Fatal("nil output accepted")
	}
	if err := runAuthStateInit([]string{"--help"}, &output); err != nil || !strings.Contains(output.String(), "gateway stopped") {
		t.Fatal("setup help is unavailable", err)
	}
}
