// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package runtime

import (
	"context"
	"os"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/files"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"golang.org/x/sys/unix"
)

func openFileSource(ctx context.Context, input adapter.ProcessRequest) (adapter.Session, error) {
	if input.Validate() != nil || input.SourceFile == nil {
		return nil, adapter.ErrInvalid
	}
	const descriptor = 6
	flags, err := unix.FcntlInt(uintptr(descriptor), unix.F_GETFL, 0)
	if err != nil || flags&unix.O_ACCMODE != unix.O_RDONLY {
		return nil, adapter.ErrInvalid
	}
	file := os.NewFile(descriptor, "source-snapshot")
	if file == nil {
		return nil, adapter.ErrInvalid
	}
	limits := query.Limits{MaxRows: input.Limits.MaxRows, MaxBytes: input.Limits.MaxBytes, Timeout: time.Duration(input.Limits.TimeoutMS) * time.Millisecond, MemoryMB: input.Limits.MemoryMB, Threads: input.Limits.Threads, MaxTempMB: input.Limits.MaxTempMB}
	name := input.Source.Database
	if name == "" {
		name = "data"
	}
	session, err := files.Open(ctx, file, "/proc/self/fd/6", name, *input.SourceFile, limits)
	if err != nil {
		file.Close()
		return nil, err
	}
	return session, nil
}
