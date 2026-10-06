//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"golang.org/x/sys/unix"
)

func prepareOperationSourceFile(ctx context.Context, workspace *scratchWorkspace, input adapter.ProcessRequest, maxBytes int64) (*os.File, operationBinaryIdentity, int64, error) {
	bad := func() (*os.File, operationBinaryIdentity, int64, error) {
		return nil, operationBinaryIdentity{}, 0, connectionUnavailable()
	}
	resolve, ok := ctx.Value(operationFileResolverKey{}).(OperationFileResolver)
	if !ok || resolve == nil || workspace == nil || input.SourceFile == nil || input.SourceFile.Validate() != nil || input.SourceFile.Bytes > maxBytes {
		return bad()
	}
	file, err := os.CreateTemp(workspace.path, ".source-snapshot-")
	if err != nil {
		return bad()
	}
	name := file.Name()
	defer os.Remove(name)
	defer file.Close()
	hash := sha256.New()
	bounded := &operationSourceWriter{destination: io.MultiWriter(file, hash), remaining: input.SourceFile.Bytes}
	until, err := resolve(ctx, input, bounded)
	if err != nil || ctx.Err() != nil || bounded.remaining != 0 || hex.EncodeToString(hash.Sum(nil)) != input.SourceFile.SHA256 || file.Sync() != nil || file.Chmod(0400) != nil {
		return bad()
	}
	before, err := file.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() != input.SourceFile.Bytes {
		return bad()
	}
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return bad()
	}
	reader := os.NewFile(uintptr(fd), "source-snapshot")
	keep := false
	defer func() {
		if !keep {
			reader.Close()
		}
	}()
	after, err := reader.Stat()
	if err != nil || !os.SameFile(before, after) || after.Size() != before.Size() || file.Close() != nil || os.Remove(name) != nil {
		return bad()
	}
	// Unlink before launch. The child receives only a read-only description;
	// the launcher grants that inode read access without writable ancestry.
	identity := operationBinaryIdentity{info: after}
	if operationBinaryCurrent(reader, identity) != nil {
		return bad()
	}
	keep = true
	return reader, identity, until, nil
}

type operationSourceWriter struct {
	destination io.Writer
	remaining   int64
}

func (w *operationSourceWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, connectionUnavailable()
	}
	n, err := w.destination.Write(p)
	w.remaining -= int64(n)
	return n, err
}
