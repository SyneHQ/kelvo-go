// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func migrationPolicyCatalogs(t *testing.T) (catalog.Config, catalog.Config) {
	t.Helper()
	source := backupPolicyCatalog()
	source.Acceleration.ObjectStorage = &catalog.ObjectStorage{ObjectLocation: catalog.ObjectLocation{Provider: "s3", Endpoint: "https://storage.example.test", Region: "us-east-1", Bucket: "snapshots"}}
	raw, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var target catalog.Config
	if err := json.Unmarshal(raw, &target); err != nil {
		t.Fatal(err)
	}
	target.Acceleration.ObjectStorage = nil
	target.Acceleration.Directory = "/private/new-target"
	return source, target
}

type migrationPolicyFixture struct {
	Backend
	calls   int
	request MigrationRequest
	result  MigrationResult
	err     error
}

func (f *migrationPolicyFixture) MigrateBackup(_ context.Context, r MigrationRequest) (MigrationResult, error) {
	f.calls++
	f.request = r
	return f.result, f.err
}

func TestMigrationCatalogPoliciesMustBeIdenticalExceptStorage(t *testing.T) {
	mutations := map[string]func(*catalog.Config){
		"source credential": func(c *catalog.Config) { c.Sources[0].DSNEnv = "KELVO_OTHER_SECRET" },
		"source type":       func(c *catalog.Config) { c.Sources[0].Type = "mysql" },
		"query":             func(c *catalog.Config) { c.Acceleration.Datasets[0].Query.SQL = "SELECT 1" },
		"authorization":     func(c *catalog.Config) { c.Acceleration.Datasets[0].AuthorizationVersion = "new-policy" },
		"memory limit":      func(c *catalog.Config) { c.Acceleration.Datasets[0].Limits.MemoryMB++ },
		"byte limit":        func(c *catalog.Config) { c.Acceleration.Datasets[0].Limits.MaxBytes++ },
		"freshness":         func(c *catalog.Config) { c.Acceleration.Datasets[0].MaxAge++ },
		"schema": func(c *catalog.Config) {
			c.Acceleration.Datasets[0].SchemaEvolution = &catalog.SchemaEvolution{AddNullableColumns: true}
		},
		"tenant":              func(c *catalog.Config) { c.Acceleration.TenantID = "other" },
		"dataset":             func(c *catalog.Config) { c.Acceleration.Datasets[0].ID = "other" },
		"extension directory": func(c *catalog.Config) { c.ExtensionDirectory = "/another-extension-root" },
		"remote target":       func(c *catalog.Config) { c.Acceleration.ObjectStorage = &catalog.ObjectStorage{} },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			source, target := migrationPolicyCatalogs(t)
			mutate(&target)
			if _, err := ValidateMigrationCatalogs(source, target, "daily"); err == nil {
				t.Fatal("migration changed policy")
			}
		})
	}
	source, target := migrationPolicyCatalogs(t)
	request, err := ValidateMigrationCatalogs(source, target, "daily")
	if err != nil {
		t.Fatal(err)
	}
	src, _ := source.DatasetFingerprint("daily")
	dst, _ := target.DatasetFingerprint("daily")
	if src == dst || request.SourceFingerprint != src || request.TargetFingerprint != dst || request.Destination != target.Acceleration.Directory {
		t.Fatal("migration fingerprint derivation changed")
	}
	if _, err := ValidateMigrationCatalogs(target, target, "daily"); err == nil {
		t.Fatal("local source accepted")
	}
	if _, err := ValidateMigrationCatalogs(source, target, "missing"); err == nil {
		t.Fatal("missing dataset accepted")
	}
}

func TestMigrationManagerUsesPolicyWithoutExecutingSource(t *testing.T) {
	source, target := migrationPolicyCatalogs(t)
	cause := errors.New("post-publication fixture failure")
	backend := &migrationPolicyFixture{result: MigrationResult{Snapshot: Snapshot{Generation: "published"}}, err: cause}
	manager := &Manager{config: source, store: backend, factory: func(catalog.Config, query.Limits) (query.Executor, error) {
		t.Fatal("migration executed SQL")
		return nil, nil
	}}
	result, err := manager.MigrateBackup(context.Background(), "daily", target)
	if !errors.Is(err, cause) || result.Snapshot.Generation != "published" || backend.calls != 1 {
		t.Fatal("migration lost uncertain publication or backend dispatch")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.MigrateBackup(ctx, "daily", target); !errors.Is(err, context.Canceled) || backend.calls != 1 {
		t.Fatal("canceled migration reached backend")
	}
	target.Acceleration.Datasets[0].AuthorizationVersion = "changed"
	if _, err := manager.MigrateBackup(context.Background(), "daily", target); err == nil || backend.calls != 1 {
		t.Fatal("policy changed before backend")
	}
}

type migrationDeadlineFixture struct {
	Backend
	remaining time.Duration
	observed  bool
}

func (f *migrationDeadlineFixture) MigrateBackup(ctx context.Context, _ MigrationRequest) (MigrationResult, error) {
	deadline, ok := ctx.Deadline()
	f.observed = ok
	f.remaining = time.Until(deadline)
	return MigrationResult{}, nil
}
func TestMigrationManagerPropagatesConfiguredTotalDeadline(t *testing.T) {
	source, target := migrationPolicyCatalogs(t)
	backend := &migrationDeadlineFixture{}
	manager := &Manager{config: source, store: backend}
	if _, err := manager.MigrateBackup(context.Background(), "daily", target); err != nil {
		t.Fatal(err)
	}
	if !backend.observed || backend.remaining <= 0 || backend.remaining > source.Acceleration.Datasets[0].Limits.Timeout {
		t.Fatal("configured migration deadline missing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := manager.MigrateBackup(ctx, "daily", target); err != nil {
		t.Fatal(err)
	}
	if backend.remaining <= 0 || backend.remaining > time.Second {
		t.Fatal("migration extended caller deadline")
	}
}
