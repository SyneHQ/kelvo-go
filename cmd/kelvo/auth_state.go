// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/cluster"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// Initialization is an offline command. In particular, do not route it through
// runCluster, which opens broker connections before constructing a gateway.
func runAuthStateInit(args []string, out io.Writer) error {
	invalid := query.NewError("INVALID_ARGUMENT", "Expected auth-state-init --config gateway.yml")
	if out == nil {
		return invalid
	}
	f := flag.NewFlagSet("auth-state-init", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	path, configured := "", false
	f.Func("config", "Gateway configuration (YAML)", func(value string) error {
		if configured || strings.TrimSpace(value) == "" {
			return errors.New("invalid configuration argument")
		}
		path, configured = value, true
		return nil
	})
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			const help = "Usage: kelvo auth-state-init --config gateway.yml\nInitialize private authentication history once, with the gateway stopped.\n"
			if n, err := io.WriteString(out, help); err != nil || n != len(help) {
				return query.NewError("UNAVAILABLE", "Authentication setup help could not be written")
			}
			return nil
		}
		return invalid
	}
	if !configured || f.NArg() != 0 {
		return invalid
	}
	cfg, err := cluster.LoadGateway(path)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Gateway configuration is invalid or unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cluster.InitializeGatewayAuthState(ctx, cfg); err != nil {
		return query.NewError("UNAVAILABLE", "Authentication state could not be initialized; preserve existing state and check configuration, ownership and storage")
	}
	const success = "Authentication state initialized. Retain this private directory across gateway restarts.\n"
	if n, err := io.WriteString(out, success); err != nil || n != len(success) {
		return query.NewError("UNAVAILABLE", "Authentication state initialized, but confirmation could not be written")
	}
	return nil
}
