// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"go.yaml.in/yaml/v3"
)

func runMigrationBackup(args []string) error {
	f := flag.NewFlagSet("accelerate migrate-backup", flag.ContinueOnError)
	sourcePath := f.String("config", "", "Explicit remote source catalog (YAML)")
	targetPath := f.String("destination-config", "", "Explicit local target catalog with a new acceleration directory (YAML)")
	id := f.String("dataset", "", "Current dataset to migrate")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if f.NArg() != 0 || *sourcePath == "" || *targetPath == "" || *id == "" {
		return query.NewError("INVALID_ARGUMENT", "Migration requires config, destination-config and dataset")
	}
	source, err := catalog.Load(*sourcePath)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Migration source catalog cannot be loaded")
	}
	target, err := catalog.Load(*targetPath)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Migration target catalog cannot be loaded")
	}
	if _, err := acceleration.ValidateMigrationCatalogs(source, target, *id); err != nil {
		return err
	}
	manager, err := acceleration.NewManager(source, func(catalog.Config, query.Limits) (query.Executor, error) {
		return nil, query.NewError("INTERNAL", "Migration cannot execute source queries")
	})
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Migration object reader cannot be opened")
	}
	defer manager.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result, err := manager.MigrateBackup(ctx, *id, target)
	if err != nil {
		return backupCLIError(result.Snapshot, err)
	}
	return yaml.NewEncoder(os.Stdout).Encode(struct {
		Verified                     bool `yaml:"verified"`
		acceleration.MigrationResult `yaml:",inline"`
	}{true, result})
}
