//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"errors"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// SandboxCommand is unavailable because the supported launcher relies on
// Linux Landlock. Callers must fail closed instead of running an unsandboxed
// worker on another platform.
func SandboxCommand(string, string, catalog.Config, query.Limits) ([]string, error) {
	return nil, errors.New("worker sandbox requires Linux Landlock")
}
