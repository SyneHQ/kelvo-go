//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func validateObjectSource(source catalog.Source) error {
	if source.Object != nil {
		return query.NewError("CONFIGURATION_ERROR", "Cloud object credentials are unavailable to the query engine")
	}
	if source.Range == nil {
		return nil
	}
	if source.Range.Validate() != nil || source.Type != "parquet" || source.Path != source.Range.URL || source.Federation != nil || source.Adapter != "" ||
		source.DSNEnv != "" || source.URLEnv != "" || source.UsernameEnv != "" ||
		source.PasswordEnv != "" || source.TokenEnv != "" || len(source.Options) != 0 {
		return query.NewError("CONFIGURATION_ERROR", "Object snapshots require one exact range capability without cloud credentials")
	}
	return nil
}

func validateObjectCombination(sources []catalog.Source) error {
	object, network := false, false
	for _, source := range sources {
		if err := validateObjectSource(source); err != nil {
			return err
		}
		object = object || source.Range != nil
		network = network || source.Type == "postgres" || source.Type == "mysql"
	}
	if object && network {
		// Those legacy database extensions need enable_external_access=true.
		// Never let their compatibility path disable exact range restrictions.
		return query.NewError("UNSUPPORTED", "Object snapshots cannot be combined with network database sources")
	}
	return nil
}

// prepareObjectRange only reads the parent-owned loopback capability. Cloud
// credentials, TLS, redirect refusal, version checks, and upstream byte ranges
// belong to the parent bridge. It never redirects or accepts whole-object GETs.
// HTTPFS receives no secret and lockSourceAccess supplies exact authorization.
func prepareObjectRange(ctx context.Context, exec driver.ExecerContext, source catalog.Source, extensionDir, tempDir string) error {
	if err := validateObjectSource(source); err != nil {
		return err
	}
	if source.Range == nil {
		return errors.New("object range capability is required")
	}
	if err := loadApprovedExtension(ctx, exec, "httpfs", extensionDir, tempDir); err != nil {
		return errors.New("approved object range extension is unavailable")
	}
	settings := []string{
		"SET force_download = false",
		"SET force_download_threshold = 0",
		"SET auto_fallback_to_full_download = false",
		"SET unsafe_disable_etag_checks = false",
		"SET enable_global_s3_configuration = false",
		"SET merge_http_secret_into_s3_request = false",
		"SET httpfs_enable_credential_refresh = false",
	}
	for _, setting := range settings {
		if _, err := exec.ExecContext(ctx, setting, nil); err != nil {
			return errors.New("object range restrictions could not be configured")
		}
	}
	return nil
}
