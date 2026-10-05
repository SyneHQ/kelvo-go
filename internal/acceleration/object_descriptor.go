// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/apache/arrow-go/v18/arrow"
	"go.yaml.in/yaml/v3"
)

func validateObjectDescriptor(d objectGenerationDescriptor) error {
	if d.Version != 1 || !storeDatasetID.MatchString(d.Dataset) || !storeGenerationID.MatchString(d.Generation) || !storeDigest.MatchString(d.SchemaHash) || d.Rows < 0 || d.Bytes <= 0 || d.Bytes > 1<<40 || len(d.Parts) < 1 || len(d.Parts) > 256 {
		return fmt.Errorf("%w: invalid object generation descriptor", ErrCorrupt)
	}
	var rows, total int64
	for _, part := range d.Parts {
		if part.Rows < 0 || part.Bytes <= 0 || part.Bytes > objectstore.MaxUploadBytes || !storeDigest.MatchString(part.SHA256) || !validObjectVersion(part.ObjectVersion) || part.Rows > d.Rows-rows || part.Bytes > d.Bytes-total {
			return fmt.Errorf("%w: invalid object descriptor part", ErrCorrupt)
		}
		rows += part.Rows
		total += part.Bytes
	}
	if rows != d.Rows || total != d.Bytes {
		return fmt.Errorf("%w: object descriptor totals differ", ErrCorrupt)
	}
	return nil
}
func marshalObjectDescriptor(d objectGenerationDescriptor) ([]byte, error) {
	if err := validateObjectDescriptor(d); err != nil {
		return nil, err
	}
	data, err := yaml.Marshal(d)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > objectDescriptorLimit {
		return nil, fmt.Errorf("%w: object descriptor exceeds limit", ErrCorrupt)
	}
	return data, nil
}
func decodeObjectDescriptor(data []byte) (objectGenerationDescriptor, error) {
	var d objectGenerationDescriptor
	bad := fmt.Errorf("%w: invalid object generation descriptor", ErrCorrupt)
	if len(data) == 0 || int64(len(data)) > objectDescriptorLimit {
		return d, bad
	}
	// Forbid alias/anchor expansion before decoding into the closed schema.
	var node yaml.Node
	if yaml.Unmarshal(data, &node) != nil {
		return d, bad
	}
	var valid func(*yaml.Node) bool
	valid = func(n *yaml.Node) bool {
		if n.Kind == yaml.AliasNode || n.Anchor != "" {
			return false
		}
		for _, child := range n.Content {
			if !valid(child) {
				return false
			}
		}
		return true
	}
	if !valid(&node) {
		return d, bad
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if decoder.Decode(&d) != nil {
		return d, bad
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return d, bad
	}
	if err := validateObjectDescriptor(d); err != nil {
		return d, err
	}
	return d, nil
}

func (backend *objectBackend) loadObjectSnapshot(ctx context.Context, dataset string, committed *objectCommitted, reference time.Time, client objectstore.Client) (snapshot Snapshot, resultErr error) {
	if committed == nil {
		return Snapshot{}, ErrNotFound
	}
	if !storeDatasetID.MatchString(dataset) {
		return Snapshot{}, ErrCorrupt
	}
	if err := validateObjectCommit(committed); err != nil {
		return Snapshot{}, err
	}
	if committed.Descriptor == nil {
		return backend.snapshot(dataset, committed, reference)
	}
	ref := committed.Descriptor
	key := backend.key(dataset, objectDescriptorName(committed.Generation))
	body, info, err := client.Get(ctx, key, ref.ObjectVersion)
	if backend.protected() && !nilReaderDependency(body) {
		defer func() {
			finishProtectedRead(body, &resultErr)
			if resultErr != nil {
				snapshot = Snapshot{}
			}
		}()
	}
	if err != nil {
		return Snapshot{}, err
	}
	if backend.protected() {
		if nilReaderDependency(body) {
			return Snapshot{}, ErrCorrupt
		}
	} else {
		defer body.Close()
	}
	if info.Size != ref.Bytes || info.Version != ref.ObjectVersion || info.SHA256 != committed.SHA256 {
		return Snapshot{}, fmt.Errorf("%w: object descriptor metadata changed", ErrCorrupt)
	}
	data, err := io.ReadAll(io.LimitReader(body, ref.Bytes+1))
	if err != nil {
		return Snapshot{}, err
	}
	if int64(len(data)) != ref.Bytes || objectSHA256(data) != committed.SHA256 {
		return Snapshot{}, fmt.Errorf("%w: object descriptor checksum or size differs", ErrCorrupt)
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	descriptor, err := decodeObjectDescriptor(data)
	if err != nil {
		return Snapshot{}, err
	}
	if descriptor.Dataset != dataset || descriptor.Generation != committed.Generation || descriptor.SchemaHash != committed.SchemaHash || descriptor.Rows != committed.Rows || descriptor.Bytes != committed.Bytes || len(descriptor.Parts) != ref.PartCount {
		return Snapshot{}, fmt.Errorf("%w: descriptor identity or totals differ from manifest", ErrCorrupt)
	}
	snapshot = Snapshot{Dataset: dataset, Generation: committed.Generation, SchemaHash: committed.SchemaHash, Fingerprint: committed.Fingerprint, SHA256: committed.SHA256, Rows: committed.Rows, Bytes: committed.Bytes, RefreshedAt: committed.RefreshedAt, Parts: make([]SnapshotPart, len(descriptor.Parts))}
	for index, part := range descriptor.Parts {
		snapshot.Parts[index] = SnapshotPart{Rows: part.Rows, Bytes: part.Bytes, SHA256: part.SHA256, ObjectVersion: part.ObjectVersion, ObjectKey: backend.key(dataset, multipartName(committed.Generation, index))}
	}
	return snapshot.observeClock(reference), nil
}

func objectPayloadSnapshots(snapshot Snapshot) ([]Snapshot, error) {
	if len(snapshot.Parts) == 0 {
		if snapshot.ObjectKey == "" || !validObjectVersion(snapshot.ObjectVersion) || snapshot.Bytes <= 0 || snapshot.Bytes > objectstore.MaxUploadBytes || snapshot.Rows < 0 || !storeDigest.MatchString(snapshot.SHA256) {
			return nil, ErrCorrupt
		}
		return []Snapshot{snapshot}, nil
	}
	if len(snapshot.Parts) > 256 || snapshot.Path != "" || snapshot.ObjectKey != "" || snapshot.ObjectVersion != "" || snapshot.Rows < 0 || snapshot.Bytes <= 0 || snapshot.Bytes > 1<<40 {
		return nil, ErrCorrupt
	}
	result := make([]Snapshot, len(snapshot.Parts))
	var rows, total int64
	for index, part := range snapshot.Parts {
		if part.Path != "" || part.ObjectKey == "" || !validObjectVersion(part.ObjectVersion) || part.Bytes <= 0 || part.Bytes > objectstore.MaxUploadBytes || part.Rows < 0 || part.Rows > snapshot.Rows-rows || part.Bytes > snapshot.Bytes-total || !storeDigest.MatchString(part.SHA256) {
			return nil, ErrCorrupt
		}
		leaf := snapshot
		leaf.Parts = nil
		leaf.ObjectKey = part.ObjectKey
		leaf.ObjectVersion = part.ObjectVersion
		leaf.SHA256 = part.SHA256
		leaf.Rows = part.Rows
		leaf.Bytes = part.Bytes
		result[index] = leaf
		rows += part.Rows
		total += part.Bytes
	}
	if rows != snapshot.Rows || total != snapshot.Bytes {
		return nil, ErrCorrupt
	}
	return result, nil
}

func (backend *objectBackend) objectSnapshotSchema(ctx context.Context, snapshot Snapshot, verifyBytes bool) (*arrow.Schema, error) {
	return backend.objectSnapshotSchemaWithReader(ctx, snapshot, verifyBytes, backend.reader)
}

func (backend *objectBackend) objectSnapshotSchemaWithReader(ctx context.Context, snapshot Snapshot, verifyBytes bool, client objectstore.Client) (*arrow.Schema, error) {
	parts, err := objectPayloadSnapshots(snapshot)
	if err != nil {
		return nil, err
	}
	ranges, ok := client.(objectstore.RangeClient)
	if !ok {
		return nil, ErrRecoveryUnsupported
	}
	var expected *arrow.Schema
	for index, part := range parts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name := snapshot.Generation + ".parquet"
		if len(snapshot.Parts) > 0 {
			name = multipartName(snapshot.Generation, index)
		}
		if part.ObjectKey != backend.key(snapshot.Dataset, name) {
			return nil, ErrCorrupt
		}
		if verifyBytes {
			if _, err := backend.verifyObjectBytesWithReader(ctx, part, client); err != nil {
				return nil, err
			}
		}
		var rows int64
		schema, err := readParquetSchema(&schemaObjectReader{ctx: ctx, client: ranges, snapshot: part, protected: backend.protected()}, part.Bytes, &rows)
		if err != nil {
			return nil, err
		}
		if rows != part.Rows {
			return nil, fmt.Errorf("%w: object Parquet footer row count differs", ErrCorrupt)
		}
		if err := verifySchemaHash(schema, snapshot.SchemaHash); err != nil {
			return nil, err
		}
		if expected != nil && !SchemaEqual(expected, schema) {
			return nil, fmt.Errorf("%w: object part schemas differ", ErrCorrupt)
		}
		expected = schema
	}
	return expected, ctx.Err()
}
func (backend *objectBackend) verifyObjectSnapshot(ctx context.Context, snapshot Snapshot) (*arrow.Schema, error) {
	return backend.objectSnapshotSchema(ctx, snapshot, true)
}

// At most four metadata requests are outstanding, independent of part count.
func (backend *objectBackend) headObjectSnapshot(ctx context.Context, snapshot Snapshot) error {
	parts, err := objectPayloadSnapshots(snapshot)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan Snapshot)
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	for range min(4, len(parts)) {
		wg.Go(func() {
			for part := range jobs {
				info, err := backend.reader.Head(ctx, part.ObjectKey, part.ObjectVersion)
				if err == nil {
					err = validateSnapshotObject(part, info)
				}
				if err != nil {
					once.Do(func() { first = err; cancel() })
					return
				}
			}
		})
	}
send:
	for index, part := range parts {
		name := snapshot.Generation + ".parquet"
		if len(snapshot.Parts) > 0 {
			name = multipartName(snapshot.Generation, index)
		}
		if part.ObjectKey != backend.key(snapshot.Dataset, name) {
			once.Do(func() { first = ErrCorrupt; cancel() })
			break
		}
		select {
		case jobs <- part:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}
