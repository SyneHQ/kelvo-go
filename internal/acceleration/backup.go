// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"io"
	"os"
)

const MaxBackupBytes int64 = 1 << 40
const backupBufferBytes = 256 << 10

var ErrBackupUnsupported = errors.New("snapshot backup requires Linux atomic no-replace directory publication")

// BackupRequest is operator-only. Fingerprint must be derived from the currently
// authorized catalog. Destination is a NEW acceleration root, never a live store
// to overwrite. MaxBytes bounds the complete selected immutable generation.
type BackupRequest struct {
	Dataset     string
	Fingerprint string
	Destination string
	MaxBytes    int64
}

type BackupBackend interface {
	Backup(context.Context, BackupRequest) (Snapshot, error)
}

type backupCopyFunc func(context.Context, io.Writer, io.Reader, int64) error

// Backup copies one verified CURRENT generation, retaining its original age and
// identity. Selecting a backup root as the source performs recovery into another
// new root without querying the original database or overwriting a live store.
// A populated snapshot with an error means publication occurred but subsequent
// durability/cleanup could not be confirmed; the destination must not be removed.
func (s *Store) Backup(ctx context.Context, request BackupRequest) (Snapshot, error) {
	return s.backup(ctx, request, backupCopy)
}

// The per-call copy function lets failure tests control copy timing without
// process-global hooks. It does not bypass destination integrity verification.
func (s *Store) backup(ctx context.Context, request BackupRequest, copyFile backupCopyFunc) (Snapshot, error) {
	return s.backupWithSync(ctx, request, copyFile, nil)
}

// syncParent is a per-call post-publication durability seam for failure tests.
// Production calls leave it nil and always fsync the real parent directory.
func (s *Store) backupWithSync(ctx context.Context, request BackupRequest, copyFile backupCopyFunc, syncParent func(*os.File) error) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if s == nil || !storeTenantID.MatchString(s.tenant) || !storeDatasetID.MatchString(request.Dataset) ||
		request.Fingerprint == "" || len(request.Fingerprint) > storeFingerprintLimit ||
		request.MaxBytes < 1 || request.MaxBytes > MaxBackupBytes || copyFile == nil {
		return Snapshot{}, errors.New("backup requires dataset, authorized fingerprint and bounded byte budget")
	}
	return backupLocal(ctx, s, request, copyFile, syncParent)
}

// backupCopy requires exactly size bytes followed by EOF and never buffers an
// entire snapshot. Cancellation and short writes cannot become success. The
// caller still verifies payload hash, footer rows and schema before publication.
func backupCopy(ctx context.Context, dst io.Writer, src io.Reader, size int64) error {
	if size < 0 || size > MaxBackupBytes {
		return errors.New("invalid backup payload size")
	}
	buffer := make([]byte, backupBufferBytes)
	remaining := size
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := src.Read(buffer[:min(int64(len(buffer)), remaining)])
		if n < 0 || int64(n) > min(int64(len(buffer)), remaining) {
			return ErrCorrupt
		}
		if n > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			written, writeErr := dst.Write(buffer[:n])
			if writeErr != nil {
				return writeErr
			}
			if written != n {
				return io.ErrShortWrite
			}
			remaining -= int64(n)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && remaining == 0 {
				return ctx.Err()
			}
			if errors.Is(readErr, io.EOF) {
				return io.ErrUnexpectedEOF
			}
			return readErr
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var extra [1]byte
	n, err := src.Read(extra[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		return ErrCorrupt
	}
	return ctx.Err()
}

var _ BackupBackend = (*localBackend)(nil)
