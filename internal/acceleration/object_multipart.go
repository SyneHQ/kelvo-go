// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/apache/arrow-go/v18/arrow"
)

// objectMultipartTransaction reuses the existing remote fence and one private
// local staging file. Seal uploads and confirms each immutable part before
// removing its staging bytes; no remote object is ever deleted by this writer.
type objectMultipartTransaction struct {
	base        *objectTransaction
	options     MultipartOptions
	parts       []objectPart
	schema      *arrow.Schema
	rows, bytes int64
	active      bool
	failed      error
}

func validateObjectMultipartOptions(options MultipartOptions) error {
	if options.MaxParts < 1 || options.MaxParts > MaxSnapshotParts || options.MaxPartBytes < 1 || options.MaxPartBytes > objectstore.MaxUploadBytes || options.MaxTotalBytes < 1 || options.MaxTotalBytes > int64(MaxSnapshotParts)*objectstore.MaxUploadBytes {
		return errors.New("remote multipart snapshot requires bounded part count, part bytes and total bytes")
	}
	return nil
}
func (backend *objectBackend) BeginMultipart(ctx context.Context, dataset string, options MultipartOptions) (MultipartRefreshWriter, error) {
	if err := validateObjectMultipartOptions(options); err != nil {
		return nil, err
	}
	writer, err := backend.Begin(ctx, dataset)
	if err != nil {
		return nil, err
	}
	return &objectMultipartTransaction{base: writer.(*objectTransaction), options: options}, nil
}
func (tx *objectMultipartTransaction) Context() context.Context { return tx.base.Context() }
func (tx *objectMultipartTransaction) PreviousSchema() (*arrow.Schema, error) {
	return tx.base.PreviousSchema()
}
func (tx *objectMultipartTransaction) SetSchema(schema *arrow.Schema) error {
	base := tx.base
	base.finishMu.Lock()
	defer base.finishMu.Unlock()
	if err := tx.check(); err != nil {
		return err
	}
	if tx.schema != nil && !SchemaEqual(tx.schema, schema) {
		return ErrSchemaMismatch
	}
	hash, err := SchemaFingerprint(schema)
	if err != nil {
		return err
	}
	base.schemaHash = hash
	return nil
}
func (tx *objectMultipartTransaction) check() error {
	if tx.base.done {
		return errors.New("acceleration transaction is already finished")
	}
	if err := context.Cause(tx.base.ctx); err != nil {
		return err
	}
	return tx.failed
}
func (tx *objectMultipartTransaction) NewPart() (*os.File, error) {
	base := tx.base
	base.finishMu.Lock()
	defer base.finishMu.Unlock()
	if err := tx.check(); err != nil {
		return nil, err
	}
	if tx.active {
		return nil, errors.New("seal current snapshot part first")
	}
	if len(tx.parts) >= tx.options.MaxParts {
		return nil, errors.New("multipart snapshot part limit exceeded")
	}
	local := base.local
	local.mu.Lock()
	defer local.mu.Unlock()
	if local.file == nil {
		file, err := storeOpenFile(local.dir, local.stage, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, err
		}
		local.file = file
	}
	tx.active = true
	return local.file, nil
}
func (tx *objectMultipartTransaction) SealPart(rows int64) (err error) {
	base := tx.base
	base.finishMu.Lock()
	defer base.finishMu.Unlock()
	if err = tx.check(); err != nil {
		return err
	}
	if !tx.active {
		return errors.New("no active snapshot part")
	}
	// A failed upload or integrity check cannot be retried as a different part or
	// committed as a truncated generation. Abort is the only remaining operation.
	defer func() {
		if err != nil {
			tx.failed = err
		}
	}()
	local := base.local
	local.mu.Lock()
	defer local.mu.Unlock()
	file := local.file
	if file == nil {
		return errors.New("snapshot staging file is unavailable")
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if rows < 0 || rows > int64(^uint64(0)>>1)-tx.rows || info.Size() <= 0 || info.Size() > tx.options.MaxPartBytes || info.Size() > tx.options.MaxTotalBytes-tx.bytes {
		return errors.New("multipart snapshot size or row limit exceeded")
	}
	var actualRows int64
	schema, err := readParquetSchema(file, info.Size(), &actualRows)
	if err != nil {
		return err
	}
	if actualRows != rows {
		return fmt.Errorf("%w: snapshot part row count mismatch", ErrCorrupt)
	}
	if tx.schema != nil && !SchemaEqual(tx.schema, schema) {
		return ErrSchemaMismatch
	}
	if err = verifySchemaHash(schema, base.schemaHash); err != nil {
		return err
	}
	if err = file.Chmod(0400); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	digest, err := storeHash(base.ctx, file)
	if err != nil {
		return err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	key := base.backend.key(base.dataset, multipartName(local.generation, len(tx.parts)))
	uploaded, err := putImmutableObject(base.ctx, base.client, key, file, info.Size(), digest)
	if err != nil {
		return err
	}
	if err = context.Cause(base.ctx); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		local.file = nil
		return refreshCleanupError(err)
	}
	local.file = nil
	if err = storeRemove(local.dir, local.stage); err != nil {
		return err
	}
	tx.parts = append(tx.parts, objectPart{Rows: rows, Bytes: info.Size(), SHA256: digest, ObjectVersion: uploaded.Version})
	tx.rows += rows
	tx.bytes += info.Size()
	tx.schema = schema
	tx.active = false
	return nil
}

// putImmutableObject deliberately does not retry ambiguous uploads. Their keys
// remain unreachable until a successful root CAS, and remote deletion is disabled.
func putImmutableObject(ctx context.Context, client objectstore.Client, key string, body io.ReadSeeker, size int64, digest string) (objectstore.Info, error) {
	uploaded, err := client.Put(ctx, key, body, size, digest, objectstore.Condition{Absent: true})
	if err != nil {
		return objectstore.Info{}, err
	}
	if !validObjectVersion(uploaded.Version) {
		return objectstore.Info{}, fmt.Errorf("%w: storage returned no payload version", ErrCorrupt)
	}
	confirmed, err := client.Head(ctx, key, uploaded.Version)
	if err != nil {
		return objectstore.Info{}, err
	}
	if confirmed.Size != size || confirmed.Version != uploaded.Version || confirmed.SHA256 != digest {
		return objectstore.Info{}, fmt.Errorf("%w: uploaded snapshot metadata mismatch", ErrCorrupt)
	}
	return confirmed, nil
}
func (tx *objectMultipartTransaction) Commit(fingerprint string) (snapshot Snapshot, err error) {
	base := tx.base
	base.finishMu.Lock()
	defer base.finishMu.Unlock()
	if base.done {
		return Snapshot{}, errors.New("acceleration transaction is already finished")
	}
	base.done = true
	defer func() {
		if cause := context.Cause(base.ctx); err != nil && cause != nil {
			err = errors.Join(err, cause)
		}
		err = errors.Join(err, base.cleanup())
	}()
	if tx.failed != nil {
		return Snapshot{}, tx.failed
	}
	if err = context.Cause(base.ctx); err != nil {
		return Snapshot{}, err
	}
	if tx.active || len(tx.parts) == 0 || fingerprint == "" || len(fingerprint) > storeFingerprintLimit {
		return Snapshot{}, errors.New("multipart snapshot requires sealed parts and a bounded fingerprint")
	}
	schemaHash, err := SchemaFingerprint(tx.schema)
	if err != nil {
		return Snapshot{}, err
	}
	if base.schemaHash != "" && base.schemaHash != schemaHash {
		return Snapshot{}, ErrSchemaMismatch
	}
	descriptor := objectGenerationDescriptor{Version: 1, Dataset: base.dataset, Generation: base.local.generation, SchemaHash: schemaHash, Rows: tx.rows, Bytes: tx.bytes, Parts: tx.parts}
	data, err := marshalObjectDescriptor(descriptor)
	if err != nil {
		return Snapshot{}, err
	}
	digest := objectSHA256(data)
	uploaded, err := putImmutableObject(base.ctx, base.client, base.backend.key(base.dataset, objectDescriptorName(base.local.generation)), bytes.NewReader(data), int64(len(data)), digest)
	if err != nil {
		return Snapshot{}, err
	}
	committed := &objectCommitted{SchemaHash: schemaHash, Generation: base.local.generation, Fingerprint: fingerprint, SHA256: digest, Rows: tx.rows, Bytes: tx.bytes, Descriptor: &objectDescriptorRef{ObjectVersion: uploaded.Version, Bytes: int64(len(data)), PartCount: len(tx.parts)}}
	prepared := Snapshot{SchemaHash: schemaHash, Dataset: base.dataset, Generation: base.local.generation, Fingerprint: fingerprint, SHA256: digest, Rows: tx.rows, Bytes: tx.bytes, Parts: make([]SnapshotPart, len(tx.parts))}
	for i, part := range tx.parts {
		prepared.Parts[i] = SnapshotPart{Rows: part.Rows, Bytes: part.Bytes, SHA256: part.SHA256, ObjectKey: base.backend.key(base.dataset, multipartName(base.local.generation, i)), ObjectVersion: part.ObjectVersion}
	}
	return base.publishCommitted(committed, &prepared)
}
func (tx *objectMultipartTransaction) Abort() error { return tx.base.Abort() }

var _ MultipartBackend = (*objectBackend)(nil)
var _ MultipartRefreshWriter = (*objectMultipartTransaction)(nil)
