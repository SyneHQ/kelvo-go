//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.yaml.in/yaml/v3"
)

func backupCLIConfig(t *testing.T) (catalog.Config, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KELVO_SOURCE_BACKUP_UNUSED_DSN", "")
	c := catalog.Config{
		Sources: []catalog.Source{{ID: "source", Type: "postgres", DSNEnv: "KELVO_SOURCE_BACKUP_UNUSED_DSN"}},
		Acceleration: &catalog.AccelerationConfig{
			Directory: filepath.Join(root, "source-store"), TenantID: "tenant-backup",
			Datasets: []catalog.Dataset{{ID: "events", Query: query.Request{Mode: "native", ConnectionID: "source", SQL: "SELECT private_query_marker FROM unavailable"},
				AuthorizationVersion: "v1", MaxAge: time.Hour, Limits: query.DefaultLimits()}},
		},
	}
	file := filepath.Join(root, "catalog.yml")
	writeBackupCLIConfig(t, file, c)
	loaded, err := catalog.Load(file)
	if err != nil {
		t.Fatal(err)
	}
	return loaded, file
}

func writeBackupCLIConfig(t *testing.T, file string, c catalog.Config) {
	t.Helper()
	data, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func seedBackupCLI(t *testing.T, c catalog.Config) acceleration.Snapshot {
	t.Helper()
	store, err := acceleration.OpenStore(c.Acceleration.Directory, c.Acceleration.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	if err := tx.SetSchema(schema); err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(42)
	builder.Field(0).(*array.Int64Builder).AppendNull()
	record := builder.NewRecordBatch()
	defer record.Release()
	sink := acceleration.NewParquetSink(tx.File(), query.DefaultLimits())
	defer sink.Abort()
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := c.DatasetFingerprint("events")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tx.Commit(fingerprint, 2)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// Tests are intentionally serial: the CLI writes to process stdout.
func captureBackupCLI(t *testing.T, args ...string) ([]byte, error) {
	t.Helper()
	output, err := os.CreateTemp(t.TempDir(), "stdout-")
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = output
	defer func() { os.Stdout = old; output.Close() }()
	err = runAcceleration(args)
	data, readErr := os.ReadFile(output.Name())
	if readErr != nil {
		t.Fatal(readErr)
	}
	return data, err
}

func assertBackupCLIIdentity(t *testing.T, expected, got acceleration.Snapshot) {
	t.Helper()
	if expected.Dataset != got.Dataset || expected.Generation != got.Generation || expected.Fingerprint != got.Fingerprint ||
		expected.SHA256 != got.SHA256 || expected.SchemaHash != got.SchemaHash || expected.Rows != got.Rows || expected.Bytes != got.Bytes || !expected.RefreshedAt.Equal(got.RefreshedAt) {
		t.Fatal("backup changed generation identity, schema, content metadata or refresh time")
	}
}

func TestCLIBackupCreatesVerifiedPortableRootWithoutSourceExecution(t *testing.T) {
	c, file := backupCLIConfig(t)
	originalConfig, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	source := seedBackupCLI(t, c)
	backup := filepath.Join(filepath.Dir(file), "backup")
	output, err := captureBackupCLI(t, "backup", "--config", file, "--dataset", "events", "--destination", backup)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Verified bool                  `yaml:"verified"`
		Snapshot acceleration.Snapshot `yaml:"snapshot"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(output))
	decoder.KnownFields(true)
	if err := decoder.Decode(&result); err != nil || !result.Verified {
		t.Fatalf("backup did not report verified snapshot: %v", err)
	}
	assertBackupCLIIdentity(t, source, result.Snapshot)
	if !strings.HasPrefix(result.Snapshot.Path, backup+string(os.PathSeparator)) {
		t.Fatal("backup output did not identify the new root")
	}
	if bytes.Contains(output, []byte("private_query_marker")) || bytes.Contains(output, []byte("KELVO_SOURCE_BACKUP_UNUSED_DSN")) {
		t.Fatal("backup output exposed source query or credential references")
	}
	unchanged, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(originalConfig, unchanged) {
		t.Fatal("backup modified the active operator configuration")
	}
	store, err := acceleration.OpenBackend(*c.Acceleration)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	current, err := store.Verify(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	assertBackupCLIIdentity(t, source, current)

	// Recovery is the same verified copy operation with the backup as input.
	// Only the root changes; the operator retains the original source/query
	// catalog for the fingerprint and explicitly switches production afterward.
	c.Acceleration.Directory = backup
	recoveryConfig := filepath.Join(filepath.Dir(file), "recovery.yml")
	writeBackupCLIConfig(t, recoveryConfig, c)
	recovered := filepath.Join(filepath.Dir(file), "recovered")
	output, err = captureBackupCLI(t, "backup", "--config", recoveryConfig, "--dataset", "events", "--destination", recovered)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(output, &result); err != nil || !result.Verified {
		t.Fatalf("recovery copy failed: %v", err)
	}
	assertBackupCLIIdentity(t, source, result.Snapshot)
	if !strings.HasPrefix(result.Snapshot.Path, recovered+string(os.PathSeparator)) {
		t.Fatal("recovery did not create a separate root")
	}
	c.Acceleration.Directory = recovered
	verification, err := acceleration.OpenBackend(*c.Acceleration)
	if err != nil {
		t.Fatal(err)
	}
	defer verification.Close()
	verified, err := verification.Verify(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	assertBackupCLIIdentity(t, source, verified)
}

func TestCLIBackupRejectsInvalidFlagsBeforeLoadingCatalog(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yml")
	destination := filepath.Join(t.TempDir(), "destination")
	for _, args := range [][]string{
		{"backup", "--dataset", "events"},
		{"backup", "--dataset", "events", "--destination", "relative"},
		{"backup", "--destination", destination},
		{"backup", "--dataset", "events", "--destination", destination, "--sandbox", ""},
		{"backup", "--dataset", "events", "--destination", destination, "--generation", ""},
		{"backup", "--dataset", "events", "--destination", destination, "--expected-generation", ""},
		{"status", "--dataset", "events", "--destination", destination},
		{"verify", "--dataset", "events", "--destination", ""},
		{"watch", "--destination", destination},
	} {
		output, err := captureBackupCLI(t, append(args, "--config", missing)...)
		if err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" || len(output) != 0 {
			t.Fatalf("invalid backup flags reached catalog/store: %v", err)
		}
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid command created its destination")
	}
}

func TestCLIBackupRejectsRemoteStorageBeforeCredentials(t *testing.T) {
	c, file := backupCLIConfig(t)
	c.Acceleration.ObjectStorage = &catalog.ObjectStorage{
		ObjectLocation:   catalog.ObjectLocation{Provider: "s3", Endpoint: "https://objects.example.com", Bucket: "snapshots", Prefix: "kelvo", Region: "us-east-1"},
		ReadCredentials:  catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_BACKUP_READER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_BACKUP_READER_SECRET"},
		WriteCredentials: catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_BACKUP_WRITER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_BACKUP_WRITER_SECRET"},
	}
	for _, name := range append(c.Acceleration.ObjectStorage.ReadCredentials.EnvironmentNames(), c.Acceleration.ObjectStorage.WriteCredentials.EnvironmentNames()...) {
		if name != "" {
			t.Setenv(name, "")
		}
	}
	writeBackupCLIConfig(t, file, c)
	destination := filepath.Join(filepath.Dir(file), "remote-not-supported")
	output, err := captureBackupCLI(t, "backup", "--config", file, "--dataset", "events", "--destination", destination)
	if err == nil || query.PublicError(err).Code != "UNSUPPORTED" || len(output) != 0 {
		t.Fatalf("remote backup initialized credentials or claimed success: %v", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unsupported backup created destination")
	}
}

func TestCLIBackupRequiresCurrentCatalogFingerprintAndNeverOverwrites(t *testing.T) {
	c, file := backupCLIConfig(t)
	seedBackupCLI(t, c)
	destination := filepath.Join(filepath.Dir(file), "already-present")
	if err := os.Mkdir(destination, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(destination, "keep")
	if err := os.WriteFile(marker, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	output, err := captureBackupCLI(t, "backup", "--config", file, "--dataset", "events", "--destination", destination)
	if err == nil || len(output) != 0 {
		t.Fatal("existing destination was accepted or failure claimed success")
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "unchanged" {
		t.Fatal("backup modified existing destination")
	}
	c.Acceleration.Datasets[0].AuthorizationVersion = "revoked-v2"
	writeBackupCLIConfig(t, file, c)
	destination = filepath.Join(filepath.Dir(file), "fingerprint-rejected")
	output, err = captureBackupCLI(t, "backup", "--config", file, "--dataset", "events", "--destination", destination)
	if !errors.Is(err, acceleration.ErrFingerprintMismatch) || len(output) != 0 {
		t.Fatalf("backup ignored current catalog fingerprint: %v", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("rejected fingerprint published a destination")
	}
}

func TestCLIBackupErrorsPreserveCausesWithoutExposingDiagnostics(t *testing.T) {
	private := errors.New("private path /srv/customer/source and private query SELECT secret")
	for _, tc := range []struct {
		name     string
		snapshot acceleration.Snapshot
		cause    error
		code     string
	}{
		{"published storage failure", acceleration.Snapshot{Generation: "already-published"}, private, "BACKUP_DURABILITY_UNCERTAIN"},
		{"published cancellation", acceleration.Snapshot{Generation: "already-published"}, context.Canceled, "BACKUP_DURABILITY_UNCERTAIN"},
		{"unsupported", acceleration.Snapshot{}, acceleration.ErrBackupUnsupported, "UNSUPPORTED"},
		{"unsupported backend", acceleration.Snapshot{}, acceleration.ErrRecoveryUnsupported, "UNSUPPORTED"},
		{"no overwrite", acceleration.Snapshot{}, os.ErrExist, "ALREADY_EXISTS"},
		{"fingerprint", acceleration.Snapshot{}, acceleration.ErrFingerprintMismatch, "CONFIGURATION_ERROR"},
		{"corrupt", acceleration.Snapshot{}, acceleration.ErrCorrupt, "DATASET_UNAVAILABLE"},
		{"permission", acceleration.Snapshot{}, os.ErrPermission, "PERMISSION_DENIED"},
		{"canceled", acceleration.Snapshot{}, context.Canceled, "CANCELLED"},
		{"deadline", acceleration.Snapshot{}, context.DeadlineExceeded, "DEADLINE_EXCEEDED"},
		{"unknown", acceleration.Snapshot{}, private, "BACKUP_FAILED"},
		{"nested raw typed error", acceleration.Snapshot{}, query.NewError("INTERNAL", private.Error()), "BACKUP_FAILED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := errors.Join(tc.cause, private)
			got := backupCLIError(tc.snapshot, cause)
			if !errors.Is(got, cause) || !errors.Is(got, tc.cause) {
				t.Fatal("CLI mapping lost original error identity")
			}
			public := query.PublicError(got)
			if public.Code != tc.code || strings.Contains(public.Message, "private query") || strings.Contains(public.Message, "private path") || strings.Contains(public.Message, "/srv/") || strings.Contains(public.Message, "SELECT") {
				t.Fatalf("unsafe backup error mapping: %s", public.Code)
			}
			if tc.code == "BACKUP_DURABILITY_UNCERTAIN" && (!strings.Contains(public.Message, "published") || !strings.Contains(public.Message, "preserve") || !strings.Contains(public.Message, "verify")) {
				t.Fatal("published backup error lacks operator recovery action")
			}
		})
	}
	if err := backupCLIError(acceleration.Snapshot{Generation: "complete"}, nil); err != nil {
		t.Fatal("successful backup became uncertain")
	}
}
