// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/operations"
	"strconv"
)

func (r ProcessRequest) validateSourceFile() error {
	if !filesnapshot.FormatSupported(r.Source.Engine) {
		if r.SourceFile != nil {
			return ErrInvalid
		}
		return nil
	}
	if r.SourceFile == nil || r.SourceFile.Validate() != nil || r.SourceFile.Format != r.Source.Engine || r.Source.DSN != "" || r.Source.URL != "" || r.Source.Username != "" || r.Source.Password != "" || r.Source.Token != "" || len(r.Source.Options) != 3 || r.Source.Options["file_format"] != r.SourceFile.Format || r.Source.Options["file_sha256"] != r.SourceFile.SHA256 || r.Source.Options["file_bytes"] != strconv.FormatInt(r.SourceFile.Bytes, 10) {
		return ErrInvalid
	}
	if r.Limits.MemoryMB < 16 || r.Limits.Threads < 1 || r.Limits.MaxTempMB < 1 || r.SourceFile.Bytes > int64(r.Limits.MaxTempMB)<<20 {
		return ErrInvalid
	}
	switch r.Request.Kind {
	case operations.StatementExecute:
		if r.SourceFile.Format == "sqlite" || r.SourceFile.Format == "duckdb" {
			return nil
		}
		return ErrUnsupported
	case operations.ConnectionTest, operations.QueryRead, operations.MetadataInspect:
		return nil
	default:
		return ErrUnsupported
	}
}
