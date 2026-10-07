// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"example.com/kelvo-application/internal/application"
)

func run() error {
	var options application.ExerciseOptions
	path := flag.String("config", "app.yaml", "application configuration file")
	flag.StringVar(&options.Team, "team", "", "configured application team")
	flag.StringVar(&options.Subject, "subject", "", "configured application subject")
	flag.StringVar(&options.Connection, "connection", "", "configured saved connection")
	flag.StringVar(&options.RunID, "run-id", "", "stable exercise row and mutation key")
	flag.StringVar(&options.Journal, "journal", "mutation-journal.json", "private mutation recovery journal (retain after uncertainty)")
	flag.StringVar(&options.Mode, "mode", "all", "all, read (operation and analytical), or query (analytical only)")
	flag.BoolVar(&options.ExpectDenied, "expect-denied", false, "query mode: require an explicit remote permission denial")
	flag.BoolVar(&options.ExpectResolverUnavailable, "expect-resolver-unavailable", false, "query mode: require terminal UNAVAILABLE; harness must independently verify authority policy denial")
	flag.Parse()
	c, err := application.LoadConfig(*path)
	if err != nil {
		return fmt.Errorf("exercise load_config: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	report, err := application.Exercise(ctx, c, options)
	// Retain completed checks and safe stage/status codes even on failure.
	if writeErr := application.WriteReport(os.Stdout, report); writeErr != nil && err == nil {
		return fmt.Errorf("exercise write_report: %w", writeErr)
	}
	return err
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
