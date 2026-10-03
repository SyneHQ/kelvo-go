// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"flag"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/cluster"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"go.yaml.in/yaml/v3"
	"os"
	"time"
)

// These operator commands use the tenant's provisioned NATS identity; there is
// no tenant-client endpoint that can clear permanent source failures.
func runRefreshControl(args []string) error {
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	config := f.String("config", "kelvo.yml", "Node configuration (YAML)")
	dataset := f.String("dataset", "", "Configured accelerated dataset")
	expected := f.String("expected-fingerprint", "", "Catalog fingerprint required for reset")
	if err := f.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if f.NArg() != 0 || *dataset == "" || (args[0] == "refresh-reset" && *expected == "") || (args[0] == "refresh-status" && *expected != "") {
		return query.NewError("INVALID_ARGUMENT", "Dataset is required; reset also requires expected-fingerprint")
	}
	cfg, err := cluster.LoadNode(*config)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Node configuration cannot be loaded")
	}
	c, err := catalog.Load(cfg.CatalogFile)
	if err != nil || c.Acceleration == nil || c.Acceleration.TenantID != cfg.Policy.TenantID {
		return query.NewError("CONFIGURATION_ERROR", "Tenant acceleration catalog cannot be loaded")
	}
	if _, ok := c.Dataset(*dataset); !ok {
		return query.NewError("INVALID_ARGUMENT", "Unknown accelerated dataset")
	}
	fingerprint, err := c.DatasetFingerprint(*dataset)
	if err != nil {
		return err
	}
	if args[0] == "refresh-reset" && *expected != fingerprint {
		return query.NewError("INVALID_ARGUMENT", "Expected fingerprint differs from current catalog")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := cluster.OpenStore(ctx, cfg.NATS, cfg.Policy, false)
	if err != nil {
		reportStoreMetadataDiagnostic(err)
		return query.NewError("UNAVAILABLE", "Tenant refresh state is unavailable")
	}
	defer store.Close()
	queue, err := store.OpenRefreshQueue(ctx, false)
	if err != nil {
		return query.NewError("UNAVAILABLE", "Tenant refresh state is unavailable; initialize its resources first")
	}
	job := cluster.RefreshJob{Dataset: *dataset, Fingerprint: fingerprint}
	if args[0] == "refresh-reset" {
		if err := queue.Reset(ctx, job); err != nil {
			return query.NewError("CONFLICT", "Refresh state could not be reset")
		}
	}
	status, err := queue.Status(ctx, job)
	if err != nil {
		return query.NewError("UNAVAILABLE", "Refresh status is unavailable")
	}
	return yaml.NewEncoder(os.Stdout).Encode(struct {
		Fingerprint string                `yaml:"fingerprint"`
		Status      cluster.RefreshStatus `yaml:"status"`
	}{fingerprint, status})
}
