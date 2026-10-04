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
	if tx.backend.protected() {
		return tx.protectedPreviousSchema()
	}
	tx.stateMu.Lock()
	committed := tx.state.manifest.Committed
	reference := tx.state.now()
	tx.stateMu.Unlock()
	if committed == nil {
		return nil, ErrNotFound
	}
	snapshot, err := tx.backend.loadObjectSnapshot(tx.ctx, tx.dataset, committed, reference, tx.backend.reader)
	if err != nil {
		return nil, err
	}
	return tx.backend.objectSnapshotSchema(tx.ctx, snapshot, false)
}

func (tx *objectTransaction) protectedPreviousSchema() (schema *arrow.Schema, resultErr error) {
	if tx.backend.runtime == nil {
		return nil, ErrProtectionRequired
	}
	op, err := tx.backend.runtime.beginOperation(tx.ctx)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, op.Close()) }()
	tx.stateMu.Lock()
	state := tx.state
	state.manifest.Committed = cloneObjectCommit(state.manifest.Committed)
	tx.stateMu.Unlock()
	selected, err := tx.backend.selectObjectState(op.Context(), tx.dataset, state, "", 0, false)
	if err != nil {
		return nil, err
	}
	_, err = tx.backend.readProtectedSelection(op.Context(), selected, func(ctx context.Context, snapshot Snapshot) error {
		var readErr error
		schema, readErr = tx.backend.objectSnapshotSchema(ctx, snapshot, false)
		return readErr
	})
	if err != nil {
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
	ctx       context.Context
	client    objectstore.RangeClient
	snapshot  Snapshot
	protected bool
}

func (r *schemaObjectReader) ReadAt(p []byte, offset int64) (n int, resultErr error) {
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
	if r.protected && !nilReaderDependency(body) {
		defer func() {
			finishProtectedRead(body, &resultErr)
			if resultErr != nil {
				n = 0
			}
		}()
	}
	if err != nil {
		return 0, err
	}
	if r.protected {
		if nilReaderDependency(body) {
			return 0, ErrCorrupt
		}
	} else {
		defer body.Close()
	}
	if err = validateSnapshotObject(r.snapshot, info); err != nil {
		return 0, err
	}
	n, err = io.ReadFull(body, p)
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
