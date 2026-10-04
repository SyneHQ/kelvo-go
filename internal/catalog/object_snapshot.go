// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"errors"
	"net/url"
	"strings"
)

// ObjectSnapshotRead binds a leased generation to exact parent-owned range
// capabilities. Provider keys, versions and credentials remain in the parent,
// which pins each upstream read and retains the lease until the child exits.
// This is trusted worker-envelope metadata, never an authorization token.
type ObjectSnapshotRead struct {
	Dataset      string               `json:"dataset"`
	Generation   string               `json:"generation"`
	SchemaSHA256 string               `json:"schema_sha256,omitempty"`
	Parts        []ObjectSnapshotPart `json:"parts"`
	Scan         SnapshotScanLimits   `json:"scan"`
}

type ObjectSnapshotPart struct {
	URL    string `json:"url"`
	Rows   int64  `json:"rows"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// ValidateObjectSnapshot leaves legacy range sources unchanged. Schema-less
// single-object generations remain valid for unrestricted reads only; access
// policy admission requires the original Arrow schema for guarded reads.
func (s Source) ValidateObjectSnapshot() error {
	read := s.ObjectSnapshot
	if read == nil {
		return nil
	}
	bad := errors.New("invalid trusted object snapshot source")
	if s.Type != "parquet" || !ValidID(s.ID) || read.Dataset != s.ID || !snapshotGeneration.MatchString(read.Generation) ||
		(read.SchemaSHA256 != "" && !snapshotDigest.MatchString(read.SchemaSHA256)) || s.LocalSnapshot != nil ||
		s.ParquetPaths != nil || s.Object != nil || s.Federation != nil || s.Adapter != "" || s.DSNEnv != "" ||
		s.URLEnv != "" || s.UsernameEnv != "" || s.PasswordEnv != "" || s.TokenEnv != "" || len(s.Options) != 0 {
		return bad
	}
	if effective, err := read.Scan.Effective(); err != nil || effective != read.Scan {
		return bad
	}
	if len(read.Parts) == 0 || len(read.Parts) > 256 {
		return bad
	}
	ranges := s.Ranges
	if ranges == nil {
		if s.Range == nil || s.Path != s.Range.URL || s.Range.Validate() != nil {
			return bad
		}
		u, _ := url.Parse(s.Range.URL)
		components := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
		if len(components) != 2 || components[1] != s.ID {
			return bad
		}
		ranges = []ObjectRange{*s.Range}
	} else if s.ValidateObjectRanges() != nil || read.SchemaSHA256 == "" {
		return bad
	}
	if len(ranges) != len(read.Parts) {
		return bad
	}
	var rows, bytes int64
	for index, part := range read.Parts {
		if part.URL != ranges[index].URL || part.Bytes != ranges[index].Bytes ||
			part.Rows < 0 || part.Rows > 100_000_000-rows || part.Bytes < 12 || part.Bytes > 1<<40-bytes || !snapshotDigest.MatchString(part.SHA256) {
			return bad
		}
		rows += part.Rows
		bytes += part.Bytes
	}
	return nil
}
