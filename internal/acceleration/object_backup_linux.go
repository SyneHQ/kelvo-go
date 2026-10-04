//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"go.yaml.in/yaml/v3"
	"golang.org/x/sys/unix"
)

func backupObjectToLocal(ctx context.Context, backend *objectBackend, request MigrationRequest, copyFile backupCopyFunc, syncParent func(*os.File) error) (result MigrationResult, resultErr error) {
	if backend.protected() {
		return result, ErrRecoveryUnsupported
	}
	if !filepath.IsAbs(request.Destination) || filepath.Clean(request.Destination) != request.Destination || request.Destination == "/" {
		return result, errors.New("migration destination must be a new absolute directory")
	}
	parent, err := storeOpenDirectory(filepath.Dir(request.Destination), false)
	if err != nil {
		return result, err
	}
	stage := &backupStage{parent: parent, tenantName: backend.config.TenantID, datasetName: request.Dataset}
	defer func() { resultErr = errors.Join(resultErr, stage.cleanup()) }()
	name := filepath.Base(request.Destination)
	var stat unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		return result, os.ErrExist
	} else if !errors.Is(err, unix.ENOENT) {
		return result, err
	}
	// Capture exactly one current manifest. Remote objects are immutable and
	// Kelvo never deletes them; this lease deliberately has no remote writer.
	lease, err := backend.Acquire(ctx, request.Dataset, request.SourceFingerprint, 0)
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	source := lease.Snapshot
	if source.Bytes < 1 || source.Bytes > request.MaxBytes || source.Rows > request.MaxRows {
		return result, errors.New("migration generation exceeds configured limits")
	}
	if !migrationAgeCompatible(source) {
		return result, errors.New("migration requires synchronized source and target clocks")
	}
	payloads, err := objectPayloadSnapshots(source)
	if err != nil {
		return result, err
	}
	manifest := storeManifest{Version: 1, Dataset: source.Dataset, Generation: source.Generation, Fingerprint: request.TargetFingerprint, SchemaHash: source.SchemaHash, SHA256: source.SHA256, Rows: source.Rows, Bytes: source.Bytes, RefreshedAt: source.RefreshedAt}
	localParts := make([]SnapshotPart, len(payloads))
	for index, payload := range payloads {
		filename := source.Generation + ".parquet"
		if len(source.Parts) > 0 {
			filename = multipartName(source.Generation, index)
		}
		if payload.ObjectKey != backend.key(request.Dataset, filename) {
			return result, ErrCorrupt
		}
		localParts[index] = SnapshotPart{Path: filename, Rows: payload.Rows, Bytes: payload.Bytes, SHA256: payload.SHA256}
	}
	if len(source.Parts) > 0 {
		manifest.Version = 2
		manifest.Parts = localParts
		manifest.SHA256 = multipartDigest(localParts)
	}
	if err := validateManifestParts(manifest); err != nil {
		return result, err
	}
	raw, err := yaml.Marshal(manifest)
	if err != nil {
		return result, err
	}
	if len(raw) > storeManifestLimit {
		return result, ErrCorrupt
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return result, err
	}
	stageName := ".kelvo-backup-" + hex.EncodeToString(token[:])
	if err := unix.Mkdirat(int(parent.Fd()), stageName, 0700); err != nil {
		return result, err
	}
	stage.name = stageName
	stage.root, err = storeOpenChildDirectory(parent, stageName, false)
	if err != nil {
		return result, err
	}
	if err := unix.Mkdirat(int(stage.root.Fd()), stage.tenantName, 0700); err != nil {
		return result, err
	}
	stage.tenantCreated = true
	stage.tenant, err = storeOpenChildDirectory(stage.root, stage.tenantName, false)
	if err != nil {
		return result, err
	}
	if err := unix.Mkdirat(int(stage.tenant.Fd()), stage.datasetName, 0700); err != nil {
		return result, err
	}
	stage.datasetCreated = true
	stage.dataset, err = storeOpenChildDirectory(stage.tenant, stage.datasetName, false)
	if err != nil {
		return result, err
	}
	var schema *arrow.Schema
	for index, payload := range payloads {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		copied, err := stage.copyObjectPayload(ctx, backend, payload, localParts[index], source.SchemaHash, copyFile)
		if err != nil {
			return result, err
		}
		if schema != nil && !SchemaEqual(schema, copied) {
			return result, fmt.Errorf("%w: migration part schemas differ", ErrCorrupt)
		}
		schema = copied
	}
	for _, filename := range []string{generationManifestName(manifest.Generation), storeManifestName} {
		file, err := storeOpenFile(stage.dataset, filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return result, err
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
			return result, err
		}
	}
	for _, dir := range []*os.File{stage.dataset, stage.tenant, stage.root} {
		if err := dir.Sync(); err != nil {
			return result, err
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !migrationAgeCompatible(source) {
		return result, errors.New("migration requires synchronized source and target clocks")
	}
	// Same-policy refreshes may advance current while this immutable generation
	// is copied. A newly published authorization/configuration fingerprint must
	// stop migration of the older policy at the final publication boundary.
	current, err := backend.readState(ctx, request.Dataset, backend.reader)
	if err != nil {
		return result, err
	}
	if current.manifest.Committed == nil {
		return result, ErrNotFound
	}
	if current.manifest.Committed.Fingerprint != request.SourceFingerprint {
		return result, ErrFingerprintMismatch
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := unix.Renameat2(int(parent.Fd()), stage.name, int(parent.Fd()), name, unix.RENAME_NOREPLACE); err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) {
			return result, ErrBackupUnsupported
		}
		return result, err
	}
	stage.published = true
	result = MigrationResult{Snapshot: manifest.snapshot(filepath.Join(request.Destination, stage.tenantName, stage.datasetName)), SourceFingerprint: source.Fingerprint, SourceSHA256: source.SHA256}
	if syncParent == nil {
		syncParent = func(dir *os.File) error { return dir.Sync() }
	}
	if err := syncParent(parent); err != nil {
		return result, fmt.Errorf("migration published but directory durability is uncertain: %w", err)
	}
	return result, nil
}

func (stage *backupStage) copyObjectPayload(ctx context.Context, backend *objectBackend, source Snapshot, part SnapshotPart, schemaHash string, copyFile backupCopyFunc) (schema *arrow.Schema, resultErr error) {
	body, info, err := backend.reader.Get(ctx, source.ObjectKey, source.ObjectVersion)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, body.Close()) }()
	if err := validateSnapshotObject(source, info); err != nil {
		return nil, err
	}
	file, err := storeOpenFile(stage.dataset, part.Path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	stage.files = append(stage.files, part.Path)
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	if err := copyFile(ctx, file, body, part.Bytes); err != nil {
		return nil, err
	}
	if err := file.Chmod(0400); err != nil {
		return nil, err
	}
	if err := file.Sync(); err != nil {
		return nil, err
	}
	return verifyMultipartPart(ctx, file, part, schemaHash)
}
