// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"errors"
	"flag"
	"io"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// runCatalogFingerprint resolves configuration only. It never starts an engine
// or resolves the values behind the catalog's secret references.
func runCatalogFingerprint(args []string, out io.Writer) error {
	invalid := query.NewError("INVALID_ARGUMENT", "Expected catalog-fingerprint with one optional --config path")
	if out == nil {
		return invalid
	}
	flags := flag.NewFlagSet("catalog-fingerprint", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath, configured := "kelvo.yml", false
	flags.Func("config", "Registered source configuration (YAML)", func(value string) error {
		if configured || strings.TrimSpace(value) == "" {
			return errors.New("invalid configuration argument")
		}
		configPath, configured = value, true
		return nil
	})
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return invalid
	}
	config, err := catalog.Load(configPath)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Catalog configuration is invalid or unavailable")
	}
	fingerprint, err := catalog.AuthorityFingerprint(config)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Catalog authority fingerprint could not be computed")
	}
	line := fingerprint + "\n"
	if n, err := io.WriteString(out, line); err != nil || n != len(line) {
		return query.NewError("UNAVAILABLE", "Catalog fingerprint could not be written")
	}
	return nil
}
