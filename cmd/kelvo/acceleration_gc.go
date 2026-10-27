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
)

// runRemoteGC reports remote generations by default. Only --delete quarantines
// and, after --grace on the provider clock, deletes exact object versions.
func runRemoteGC(args []string) error {
	f := flag.NewFlagSet("accelerate gc", flag.ContinueOnError)
	file := f.String("config", "kelvo.yml", "Registered sources and acceleration configuration (YAML)")
	id := f.String("dataset", "", "Dataset ID")
	del := f.Bool("delete", false, "Quarantine unreferenced generations and delete them after the grace period (protected namespaces only)")
	grace := f.Duration("grace", acceleration.DefaultRemoteGCGrace, "Minimum provider-clock time between quarantine and deletion")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if f.NArg() != 0 || *id == "" {
		return query.NewError("INVALID_ARGUMENT", "Specify --dataset for accelerate gc")
	}
	if *del && *grace < acceleration.MinRemoteGCGrace {
		return query.NewError("INVALID_ARGUMENT", "Grace must be at least 10 minutes")
	}
	c, err := catalog.Load(*file)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	if _, ok := c.Dataset(*id); !ok {
		return query.NewError("INVALID_ARGUMENT", "Unknown accelerated dataset")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := acceleration.RunRemoteGC(ctx, c, *id, acceleration.RemoteGCOptions{Delete: *del, Grace: *grace})
	if errors.Is(err, acceleration.ErrProtectionRequired) {
		return query.NewError("UNSUPPORTED", "Remote deletion requires a protected namespace; omit --delete for a report")
	}
	if err != nil {
		return err
	}
	return encodeAccelerationResult(os.Stdout, report)
}
