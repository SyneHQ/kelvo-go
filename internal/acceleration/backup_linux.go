//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"go.yaml.in/yaml/v3"
	"golang.org/x/sys/unix"
)

// backupPin captures metadata and acquires every payload lease under the same
// metadata lock. A concurrent refresh can then proceed while pruning must retain
// this exact generation until the copy/verification is complete.
func backupPin(ctx context.Context, s *Store, request BackupRequest) (manifest storeManifest, raw []byte, lease *Lease, err error) {
	dir, err := s.openDataset(request.Dataset, false)
	if err != nil {
		return manifest, nil, nil, err
	}
	defer dir.Close()
	metadata, err := storeLockNamed(ctx, dir, ".metadata.lock", false)
	if err != nil {
		return manifest, nil, nil, err
	}
	defer metadata.Close()
	manifest, err = storeReadManifest(dir, request.Dataset)
	if err != nil {
		return manifest, nil, nil, err
	}
	// Existing immutable metadata must agree with current.yaml. A missing
	// sidecar is permitted for pre-sidecar generations, but backup must not
	// silently repair conflicting or unreadable immutable metadata.
	sidecar, sidecarErr := storeReadManifestNamed(dir, request.Dataset, generationManifestName(manifest.Generation))
	if sidecarErr == nil {
		if !reflect.DeepEqual(sidecar, manifest) {
			return manifest, nil, nil, fmt.Errorf("%w: current generation metadata differs", ErrCorrupt)
		}
	} else if !errors.Is(sidecarErr, ErrNotFound) {
		return manifest, nil, nil, sidecarErr
	}
	if manifest.Fingerprint != request.Fingerprint {
		return manifest, nil, nil, ErrFingerprintMismatch
	}
	if manifest.Bytes < 1 || manifest.Bytes > request.MaxBytes {
		return manifest, nil, nil, errors.New("backup generation exceeds byte budget")
	}
	file, err := storeOpenFile(dir, storeManifestName, os.O_RDONLY, 0)
	if err != nil {
		return manifest, nil, nil, err
	}
	if err = storeCheckPrivate(file, false, true); err == nil {
		raw, err = io.ReadAll(io.LimitReader(file, storeManifestLimit+1))
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return manifest, nil, nil, err
	}
	if len(raw) > storeManifestLimit {
		return manifest, nil, nil, ErrCorrupt
	}
	// Retain the original YAML bytes, but reject even an unexpected concurrent
	// out-of-protocol replacement between the validated and raw manifest reads.
	var captured storeManifest
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&captured); err != nil || !reflect.DeepEqual(captured, manifest) {
		return manifest, nil, nil, ErrCorrupt
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return manifest, nil, nil, ErrCorrupt
	}
	if manifest.Version == 2 {
		lease, err = acquireMultipart(ctx, dir, manifest)
		return manifest, raw, lease, err
	}
	payload, err := storeValidatePayload(dir, manifest)
	if err != nil {
		return manifest, nil, nil, err
	}
	if err := storeLock(ctx, payload, false); err != nil {
		_ = payload.Close()
		return manifest, nil, nil, err
	}
	return manifest, raw, &Lease{Snapshot: manifest.snapshot(dir.Name()), file: payload}, nil
}

// backupStage owns only paths created in its private new tree. Cleanup uses open
// directory descriptors and exact created names, never recursive path traversal.
type backupStage struct {
	tenantCreated, datasetCreated bool
	parent, root, tenant, dataset *os.File
	name, tenantName, datasetName string
	files                         []string
	published                     bool
}

func (stage *backupStage) cleanup() error {
	var errs []error
	if !stage.published {
		if stage.dataset != nil {
			for _, name := range stage.files {
				if err := storeRemove(stage.dataset, name); err != nil && !errors.Is(err, os.ErrNotExist) {
					errs = append(errs, err)
				}
			}
		}
		if stage.tenant != nil && stage.datasetCreated {
			errs = append(errs, unix.Unlinkat(int(stage.tenant.Fd()), stage.datasetName, unix.AT_REMOVEDIR))
		}
		if stage.root != nil && stage.tenantCreated {
			errs = append(errs, unix.Unlinkat(int(stage.root.Fd()), stage.tenantName, unix.AT_REMOVEDIR))
		}
		if stage.parent != nil && stage.name != "" {
			errs = append(errs, unix.Unlinkat(int(stage.parent.Fd()), stage.name, unix.AT_REMOVEDIR))
		}
	}
	for _, file := range []*os.File{stage.dataset, stage.tenant, stage.root, stage.parent} {
		if file != nil {
			errs = append(errs, file.Close())
		}
	}
	return errors.Join(errs...)
}

func backupLocal(ctx context.Context, s *Store, request BackupRequest, copyFile backupCopyFunc, syncParent func(*os.File) error) (snapshot Snapshot, resultErr error) {
	if !filepath.IsAbs(request.Destination) || filepath.Clean(request.Destination) != request.Destination || request.Destination == "/" {
		return Snapshot{}, errors.New("backup destination must be a new absolute directory")
	}
	parent, err := storeOpenDirectory(filepath.Dir(request.Destination), false)
	if err != nil {
		return Snapshot{}, err
	}
	stage := &backupStage{parent: parent, tenantName: s.tenant, datasetName: request.Dataset}
	defer func() { resultErr = errors.Join(resultErr, stage.cleanup()) }()
	name := filepath.Base(request.Destination)
	var stat unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		return Snapshot{}, os.ErrExist
	} else if !errors.Is(err, unix.ENOENT) {
		return Snapshot{}, err
	}
	manifest, raw, lease, err := backupPin(ctx, s, request)
	if err != nil {
		return Snapshot{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return Snapshot{}, err
	}
	stageName := ".kelvo-backup-" + hex.EncodeToString(token[:])
	if err := unix.Mkdirat(int(parent.Fd()), stageName, 0700); err != nil {
		return Snapshot{}, err
	}
	stage.name = stageName
	stage.root, err = storeOpenChildDirectory(parent, stageName, false)
	if err != nil {
		return Snapshot{}, err
	}
	if err := unix.Mkdirat(int(stage.root.Fd()), s.tenant, 0700); err != nil {
		return Snapshot{}, err
	}
	stage.tenantCreated = true
	stage.tenant, err = storeOpenChildDirectory(stage.root, s.tenant, false)
	if err != nil {
		return Snapshot{}, err
	}
	if err := unix.Mkdirat(int(stage.tenant.Fd()), request.Dataset, 0700); err != nil {
		return Snapshot{}, err
	}
	stage.datasetCreated = true
	stage.dataset, err = storeOpenChildDirectory(stage.tenant, request.Dataset, false)
	if err != nil {
		return Snapshot{}, err
	}
	parts, files := manifest.Parts, lease.files
	if manifest.Version == 1 {
		parts = []SnapshotPart{{Path: manifest.Generation + ".parquet", Rows: manifest.Rows, Bytes: manifest.Bytes, SHA256: manifest.SHA256}}
		files = []*os.File{lease.file}
	}
	var schema *arrow.Schema
	for i, part := range parts {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}
		copiedSchema, err := stage.copyPayload(ctx, files[i], part, manifest.SchemaHash, copyFile)
		if err != nil {
			return Snapshot{}, err
		}
		if schema != nil && !SchemaEqual(schema, copiedSchema) {
			return Snapshot{}, fmt.Errorf("%w: backup part schemas differ", ErrCorrupt)
		}
		schema = copiedSchema
	}
	for _, filename := range []string{generationManifestName(manifest.Generation), storeManifestName} {
		// Track the file after successful exclusive creation, including failures
		// during writing/fsync, so cleanup never claims a pre-existing entry.
		file, err := storeOpenFile(stage.dataset, filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return Snapshot{}, err
		}
		stage.files = append(stage.files, filename)
		written, writeErr := file.Write(raw)
		if writeErr == nil && written != len(raw) {
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil {
			writeErr = file.Chmod(0400)
		}
		if writeErr == nil {
			writeErr = file.Sync()
		}
		if err := errors.Join(writeErr, file.Close()); err != nil {
			return Snapshot{}, err
		}
	}
	for _, dir := range []*os.File{stage.dataset, stage.tenant, stage.root} {
		if err := dir.Sync(); err != nil {
			return Snapshot{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err := unix.Renameat2(int(parent.Fd()), stage.name, int(parent.Fd()), name, unix.RENAME_NOREPLACE); err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) {
			return Snapshot{}, ErrBackupUnsupported
		}
		return Snapshot{}, err
	}
	stage.published = true
	snapshot = manifest.snapshot(filepath.Join(request.Destination, s.tenant, request.Dataset))
	if syncParent == nil {
		syncParent = func(directory *os.File) error { return directory.Sync() }
	}
	if err := syncParent(parent); err != nil {
		return snapshot, fmt.Errorf("backup published but directory durability is uncertain: %w", err)
	}
	return snapshot, nil
}

func (stage *backupStage) copyPayload(ctx context.Context, source *os.File, part SnapshotPart, schemaHash string, copyFile backupCopyFunc) (schema *arrow.Schema, resultErr error) {
	file, err := storeOpenFile(stage.dataset, part.Path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	stage.files = append(stage.files, part.Path)
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if err := copyFile(ctx, file, source, part.Bytes); err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() != part.Bytes {
		return nil, ErrCorrupt
	}
	if err := file.Chmod(0400); err != nil {
		return nil, err
	}
	if err := file.Sync(); err != nil {
		return nil, err
	}
	return verifyMultipartPart(ctx, file, part, schemaHash)
}
