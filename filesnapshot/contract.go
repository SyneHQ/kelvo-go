// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package filesnapshot describes an immutable uploaded source without paths.
package filesnapshot

import (
	"errors"
	"github.com/SYNEHQ/kelvo-go/operations"
)

const Version = 1
const MaxBytes int64 = 50 << 20

var ErrInvalid = errors.New("invalid source snapshot")

// Descriptor travels only over private resolution and adapter pipes. Its digest
// binds all source bytes; it grants no access to a filename or storage endpoint.
type Descriptor struct {
	Version int    `json:"version"`
	Format  string `json:"format"`
	Bytes   int64  `json:"bytes"`
	SHA256  string `json:"sha256"`
}

func FormatSupported(format string) bool {
	switch format {
	case "csv", "parquet", "json", "jsonl", "duckdb", "sqlite":
		return true
	}
	return false
}

func (d Descriptor) Validate() error {
	if d.Version != Version || !FormatSupported(d.Format) || d.Bytes < 1 || d.Bytes > MaxBytes || !operations.ValidDigest(d.SHA256) {
		return ErrInvalid
	}
	return nil
}
