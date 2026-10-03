// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package secrets

import (
	"context"
	"path/filepath"
)

// MaxDocumentBytes bounds a trusted private configuration document. Ordinary
// source secret values retain their separate, smaller MaxValueBytes limit.
const MaxDocumentBytes = 256 << 10

// ReadPrivateDocument applies the same descriptor-relative ownership, symlink,
// hardlink and replacement checks as source secrets. It does not cache values.
// The caller owns and should clear the returned byte slice. Filesystem syscalls
// cannot always be interrupted; callers requiring a wall-time bound must bound
// their outstanding reads as well as the lifetime of accepted data.
func ReadPrivateDocument(ctx context.Context, path string, limit int) ([]byte, error) {
	if limit < 1 || limit > MaxDocumentBytes || len(path) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return nil, ErrInvalid
	}
	return readPrivateDocument(ctx, path, limit)
}
