// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/apache/arrow-go/v18/arrow"
)

func verifySchemaHash(schema *arrow.Schema, want string) error {
	if want == "" {
		return nil
	} // Legacy generations derive their first contract from Parquet.
	got, err := SchemaFingerprint(schema)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%w: schema fingerprint mismatch", ErrCorrupt)
	}
	return nil
}

func (tx *Transaction) PreviousSchema() (*arrow.Schema, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, errors.New("acceleration transaction is already finished")
	}
	if err := tx.ctx.Err(); err != nil {
		return nil, err
	}
	// The writer lock excludes other publication, restore and pruning until
	// Commit/Abort, so this immutable payload remains pinned for the comparison.
	manifest, err := storeReadManifest(tx.dir, tx.dataset)
	if err != nil {
		return nil, err
	}
	return storeVerifyGeneration(tx.ctx, tx.dir, manifest)
}

func (tx *Transaction) SetSchema(schema *arrow.Schema) error {
	fingerprint, err := SchemaFingerprint(schema)
	if err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return errors.New("acceleration transaction is already finished")
	}
	if err = tx.ctx.Err(); err != nil {
		return err
	}
	tx.schemaHash = fingerprint
	return nil
}

func (tx *objectTransaction) PreviousSchema() (*arrow.Schema, error) {
	tx.finishMu.Lock()
	defer tx.finishMu.Unlock()
	if tx.done {
		return nil, errors.New("acceleration transaction is already finished")
	}
	if err := context.Cause(tx.ctx); err != nil {
		return nil, err
	}
	tx.stateMu.Lock()
	committed := tx.state.manifest.Committed
	reference := tx.state.now()
	tx.stateMu.Unlock()
	if committed == nil {
		return nil, ErrNotFound
	}
	client, ok := tx.backend.reader.(objectstore.RangeClient)
	if !ok {
		return nil, errors.New("schema verification requires object range reads")
	}
	snapshot, err := tx.backend.snapshot(tx.dataset, committed, reference)
	if err != nil {
		return nil, err
	}
	reader := &schemaObjectReader{ctx: tx.ctx, client: client, snapshot: snapshot}
	schema, err := ReadParquetSchema(reader, snapshot.Bytes)
	if err != nil {
		return nil, err
	}
	if err = verifySchemaHash(schema, committed.SchemaHash); err != nil {
		return nil, err
	}
	return schema, nil
}

func (tx *objectTransaction) SetSchema(schema *arrow.Schema) error {
	fingerprint, err := SchemaFingerprint(schema)
	if err != nil {
		return err
	}
	tx.finishMu.Lock()
	defer tx.finishMu.Unlock()
	if tx.done {
		return errors.New("acceleration transaction is already finished")
	}
	if err = context.Cause(tx.ctx); err != nil {
		return err
	}
	tx.schemaHash = fingerprint
	return nil
}

type schemaObjectReader struct {
	ctx      context.Context
	client   objectstore.RangeClient
	snapshot Snapshot
}

func (r *schemaObjectReader) ReadAt(p []byte, offset int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if offset < 0 || offset >= r.snapshot.Bytes {
		return 0, io.EOF
	}
	length := int64(len(p))
	if length > r.snapshot.Bytes-offset {
		return 0, io.ErrUnexpectedEOF
	}
	body, info, err := r.client.GetRange(r.ctx, r.snapshot.ObjectKey, r.snapshot.ObjectVersion, offset, length)
	if err != nil {
		return 0, err
	}
	defer body.Close()
	if err = validateSnapshotObject(r.snapshot, info); err != nil {
		return 0, err
	}
	n, err := io.ReadFull(body, p)
	if err != nil {
		return n, err
	}
	var extra [1]byte
	count, tailErr := body.Read(extra[:])
	if count != 0 || !errors.Is(tailErr, io.EOF) {
		return n, fmt.Errorf("%w: unexpected schema range length", ErrCorrupt)
	}
	return n, nil
}

var _ SchemaWriter = (*Transaction)(nil)
var _ SchemaWriter = (*objectTransaction)(nil)
