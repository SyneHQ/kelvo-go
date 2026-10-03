// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type backupPolicyFixture struct {
	Backend
	calls   int
	request BackupRequest
	result  Snapshot
	err     error
}

func (f *backupPolicyFixture) Backup(_ context.Context, request BackupRequest) (Snapshot, error) {
	f.calls++
	f.request = request
	return f.result, f.err
}
func backupPolicyCatalog() catalog.Config {
	return catalog.Config{
		Sources: []catalog.Source{{ID: "orders", Type: "postgres", DSNEnv: "KELVO_SOURCE_ORDERS_DSN"}},
		Acceleration: &catalog.AccelerationConfig{Directory: "/private/configured-root", TenantID: "tenant-a", Datasets: []catalog.Dataset{{
			ID: "daily", Query: query.Request{Mode: "native", ConnectionID: "orders", SQL: "SELECT * FROM orders"},
			AuthorizationVersion: "readers-v1", MaxAge: time.Hour, Limits: query.DefaultLimits(),
		}}},
	}
}
func TestBackupManagerUsesActiveCatalogPolicyWithoutExecutingSources(t *testing.T) {
	config := backupPolicyCatalog()
	backend := &backupPolicyFixture{result: Snapshot{Generation: "preserved", RefreshedAt: time.Now().Add(-2 * time.Hour)}}
	manager := &Manager{config: config, store: backend, factory: func(catalog.Config, query.Limits) (query.Executor, error) {
		t.Fatal("backup executed source factory")
		return nil, nil
	}}
	for _, epoch := range []string{"readers-v1", "readers-v2"} {
		config.Acceleration.Datasets[0].AuthorizationVersion = epoch
		want, err := config.DatasetFingerprint("daily")
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := manager.Backup(context.Background(), "daily", "/private/new-root")
		if err != nil || snapshot.Generation != backend.result.Generation || !snapshot.RefreshedAt.Equal(backend.result.RefreshedAt) {
			t.Fatal("backup changed generation freshness")
		}
		if backend.request.Dataset != "daily" || backend.request.Fingerprint != want || backend.request.MaxBytes != config.Acceleration.Datasets[0].Limits.MaxBytes || backend.request.Destination != "/private/new-root" {
			t.Fatal("caller-supplied policy reached backup instead of catalog policy")
		}
	}
	if backend.calls != 2 {
		t.Fatal("backup backend not invoked exactly once per request")
	}
}
func TestBackupManagerRejectsUnknownUnsupportedAndInvalidPolicy(t *testing.T) {
	config := backupPolicyCatalog()
	backend := &backupPolicyFixture{}
	manager := &Manager{config: config, store: backend}
	if _, err := manager.Backup(context.Background(), "other", "/private/new-root"); err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" {
		t.Fatal("unknown dataset reached backup")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.Backup(ctx, "daily", "/private/new-root"); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled backup reached backend")
	}
	for _, limit := range []int64{0, -1, (1 << 40) + 1} {
		config.Acceleration.Datasets[0].Limits.MaxBytes = limit
		if _, err := manager.Backup(context.Background(), "daily", "/private/new-root"); err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" {
			t.Fatal("invalid snapshot budget accepted")
		}
	}
	config.Acceleration.Datasets[0].Limits = query.DefaultLimits()
	config.Acceleration.ObjectStorage = &catalog.ObjectStorage{}
	if _, err := manager.Backup(context.Background(), "daily", "/private/new-root"); !errors.Is(err, ErrRecoveryUnsupported) {
		t.Fatal("remote backup unexpectedly accepted")
	}
	config.Acceleration.ObjectStorage = nil
	manager.store = struct{ Backend }{Backend: backend}
	if _, err := manager.Backup(context.Background(), "daily", "/private/new-root"); !errors.Is(err, ErrRecoveryUnsupported) {
		t.Fatal("non-backup backend accepted")
	}
	if backend.calls != 0 {
		t.Fatal("invalid request reached backup backend")
	}
}
func TestBackupManagerRetainsAmbiguousPublicationResult(t *testing.T) {
	cause := errors.New("fixture directory durability failure")
	backend := &backupPolicyFixture{result: Snapshot{Generation: "visible-generation"}, err: cause}
	manager := &Manager{config: backupPolicyCatalog(), store: backend}
	snapshot, err := manager.Backup(context.Background(), "daily", "/private/new-root")
	if !errors.Is(err, cause) || snapshot.Generation != backend.result.Generation {
		t.Fatal("manager hid a potentially published backup")
	}
}
