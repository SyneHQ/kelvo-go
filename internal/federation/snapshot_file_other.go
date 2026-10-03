//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"os"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func openSnapshotFile(string) (*os.File, error) {
	return nil, query.NewError("UNSUPPORTED", "Guarded snapshot reads require Linux")
}
