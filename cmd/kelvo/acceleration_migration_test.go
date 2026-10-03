//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestCLIMigrationRequiresExplicitCatalogsAndRejectsOtherFlags(t *testing.T) {
	for _, args := range [][]string{
		{}, {"--config", "missing", "--dataset", "events"},
		{"--destination-config", "missing", "--dataset", "events"},
		{"--config", "missing", "--destination-config", "missing"},
		{"--config", "missing", "--destination-config", "missing", "--dataset", "events", "unexpected"},
		{"--sandbox", ""}, {"--destination", "/tmp/should-not-create"}, {"--generation", ""},
	} {
		if err := runMigrationBackup(args); err == nil {
			t.Fatalf("invalid migration flags accepted: %v", args)
		}
	}
}

func TestCLIMigrationPolicyMismatchPrecedesObjectCredentials(t *testing.T) {
	source, path := backupCLIConfig(t)
	source.Acceleration.ObjectStorage = &catalog.ObjectStorage{
		ObjectLocation:   catalog.ObjectLocation{Provider: "s3", Endpoint: "https://objects.example.com", Bucket: "snapshots", Prefix: "kelvo", Region: "us-east-1"},
		ReadCredentials:  catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_MIGRATION_READER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_MIGRATION_READER_SECRET"},
		WriteCredentials: catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_MIGRATION_WRITER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_MIGRATION_WRITER_SECRET"},
	}
	for _, name := range append(source.Acceleration.ObjectStorage.ReadCredentials.EnvironmentNames(), source.Acceleration.ObjectStorage.WriteCredentials.EnvironmentNames()...) {
		if name != "" {
			t.Setenv(name, "")
		}
	}
	writeBackupCLIConfig(t, path, source)
	target := source
	targetAcceleration := *source.Acceleration
	targetAcceleration.ObjectStorage = nil
	targetAcceleration.Directory = filepath.Join(filepath.Dir(path), "new-local-root")
	targetAcceleration.Datasets = append([]catalog.Dataset(nil), source.Acceleration.Datasets...)
	targetAcceleration.Datasets[0].AuthorizationVersion = "changed"
	target.Acceleration = &targetAcceleration
	targetPath := filepath.Join(filepath.Dir(path), "target.yml")
	writeBackupCLIConfig(t, targetPath, target)
	output, err := captureBackupCLI(t, "migrate-backup", "--config", path, "--destination-config", targetPath, "--dataset", "events")
	if err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" || len(output) != 0 {
		t.Fatalf("policy mismatch reached credentials or output: %v", err)
	}
	if _, err := os.Lstat(targetAcceleration.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid migration created target")
	}
	if _, err := os.Lstat(source.Acceleration.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid migration created source staging")
	}
}
