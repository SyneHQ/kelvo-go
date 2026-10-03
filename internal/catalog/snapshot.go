// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"errors"
	"path/filepath"
	"regexp"
	"strings"
)

// SnapshotScanLimits bound cumulative raw Arrow work before row/column filtering.
// They do not replace a query's output, timeout, native memory or process limits.
type SnapshotScanLimits struct {
	MaxRows  int64 `json:"max_rows" yaml:"max_rows"`
	MaxBytes int64 `json:"max_bytes" yaml:"max_bytes"`
}

func (s SnapshotScanLimits) Effective() (SnapshotScanLimits, error) {
	if s.MaxRows == 0 {
		s.MaxRows = 1_000_000
	}
	if s.MaxBytes == 0 {
		s.MaxBytes = 256 << 20
	}
	if s.MaxRows < 1 || s.MaxRows > 100_000_000 || s.MaxBytes < 1024 || s.MaxBytes > 1<<40 {
		return SnapshotScanLimits{}, errors.New("invalid snapshot scan limits")
	}
	return s, nil
}

func (d Dataset) EffectiveSnapshotScanLimits() (SnapshotScanLimits, error) {
	if d.Scan == nil {
		return (SnapshotScanLimits{}).Effective()
	}
	return d.Scan.Effective()
}

// LocalSnapshotRead binds an acquired generation to its exact source paths.
// It is trusted worker-envelope metadata, not an authorization token. The parent
// retains the generation lease until every native callback and child has exited.
type LocalSnapshotRead struct {
	Dataset      string              `json:"dataset"`
	Generation   string              `json:"generation"`
	SchemaSHA256 string              `json:"schema_sha256,omitempty"`
	Parts        []LocalSnapshotPart `json:"parts"`
	Scan         SnapshotScanLimits  `json:"scan"`
}

type LocalSnapshotPart struct {
	Rows   int64  `json:"rows"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

var snapshotGeneration = regexp.MustCompile(`^[0-9a-f]{32}$`)
var snapshotDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ValidateLocalSnapshot leaves legacy sources unchanged. Schema-less acquired
// generations are valid for unrestricted reads; policy admission rejects them.
func (s Source) ValidateLocalSnapshot() error {
	read := s.LocalSnapshot
	if read == nil {
		return nil
	}
	bad := errors.New("invalid trusted local snapshot source")
	if s.Type != "parquet" || !ValidID(s.ID) || read.Dataset != s.ID || !snapshotGeneration.MatchString(read.Generation) ||
		(read.SchemaSHA256 != "" && !snapshotDigest.MatchString(read.SchemaSHA256)) || s.Federation != nil || s.Adapter != "" ||
		s.Object != nil || s.Range != nil || s.Ranges != nil || s.DSNEnv != "" || s.URLEnv != "" || s.UsernameEnv != "" ||
		s.PasswordEnv != "" || s.TokenEnv != "" || len(s.Options) != 0 {
		return bad
	}
	if effective, err := read.Scan.Effective(); err != nil || effective != read.Scan {
		return bad
	}
	paths := s.ParquetPaths
	if paths == nil {
		paths = []string{s.Path}
	} else if s.Path != "" {
		return bad
	}
	if len(paths) == 0 || len(paths) > 256 || len(paths) != len(read.Parts) {
		return bad
	}
	seen := make(map[string]bool, len(paths))
	var rows, bytes int64
	for i, path := range paths {
		if path == "" || len(path) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path ||
			strings.ContainsAny(path, "*?[]{}\\\x00\r\n") || strings.Contains(path, "://") || seen[path] {
			return bad
		}
		seen[path] = true
		part := read.Parts[i]
		if part.Rows < 0 || part.Rows > 100_000_000-rows || part.Bytes < 12 || part.Bytes > 1<<40-bytes || !snapshotDigest.MatchString(part.SHA256) {
			return bad
		}
		rows += part.Rows
		bytes += part.Bytes
	}
	return nil
}
