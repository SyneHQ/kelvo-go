// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package query

import "github.com/apache/arrow-go/v18/arrow/ipc"

func validateResultCompression(compression string) error {
	switch compression {
	case "", "none", "lz4_frame":
		return nil
	default:
		return NewError("INVALID_ARGUMENT", "Result compression must be none or lz4_frame")
	}
}

// ResultIPCOptions configures public Arrow IPC buffer compression. Empty keeps
// the original uncompressed format. Compression is synchronous with one codec
// worker per writer; it does not buffer a complete result. Arrow still allocates
// compressed buffers alongside the borrowed batch, so this is not an RSS cap.
// The untrusted child-worker IPC boundary must continue to use no compression.
func ResultIPCOptions(compression string) ([]ipc.Option, error) {
	if err := validateResultCompression(compression); err != nil {
		return nil, err
	}
	if compression == "lz4_frame" {
		return []ipc.Option{ipc.WithLZ4(), ipc.WithCompressConcurrency(1)}, nil
	}
	return nil, nil
}
