// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func runAudit(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "read" {
		return query.NewError("INVALID_ARGUMENT", "Expected audit read")
	}
	f := flag.NewFlagSet("audit read", flag.ContinueOnError)
	directory := f.String("directory", "", "Absolute private journal directory; writer must be stopped")
	cursor := f.Int("cursor", 0, "Physical slot cursor from the previous page")
	limit := f.Int("limit", 100, "Maximum physical slots to scan (1-256)")
	if err := f.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if f.NArg() != 0 {
		return query.NewError("INVALID_ARGUMENT", "Unexpected positional arguments")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	page, err := audit.ReadPage(ctx, *directory, *cursor, *limit)
	if err != nil {
		if errors.Is(err, audit.ErrBusy) {
			return query.NewError("UNAVAILABLE", "Audit writer is active; drain and stop the service before reading")
		}
		if errors.Is(err, audit.ErrInvalid) {
			return query.NewError("INVALID_ARGUMENT", "Audit reads require an absolute private directory and bounded cursor/page")
		}
		return query.NewError("UNAVAILABLE", "Audit journal could not be read; preserve it and check ownership, storage and integrity")
	}
	return json.NewEncoder(out).Encode(page)
}
