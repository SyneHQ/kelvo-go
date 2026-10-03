// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"errors"
	"path/filepath"
	"strings"
)

// ValidateParquetPaths checks the trusted process-only multipart representation.
// File existence and exact filesystem grants are checked by the worker sandbox.
func (s Source) ValidateParquetPaths() error {
	if s.ParquetPaths == nil {
		return nil
	}
	if s.Type != "parquet" || s.Path != "" || s.Adapter != "" || s.Federation != nil || s.Object != nil || s.Range != nil || s.Ranges != nil || s.DSNEnv != "" || s.URLEnv != "" || s.UsernameEnv != "" || s.PasswordEnv != "" || s.TokenEnv != "" || len(s.Options) != 0 || len(s.ParquetPaths) == 0 || len(s.ParquetPaths) > 256 {
		return errors.New("invalid multipart parquet source")
	}
	seen := make(map[string]bool, len(s.ParquetPaths))
	for _, path := range s.ParquetPaths {
		if len(path) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsAny(path, "*?[]{}\\\x00\r\n") || strings.Contains(path, "://") || seen[path] {
			return errors.New("multipart parquet paths must be unique exact local files")
		}
		seen[path] = true
	}
	return nil
}
