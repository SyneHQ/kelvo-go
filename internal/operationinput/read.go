// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operationinput

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// Load releases no payload until every part, schema, raw digest and byte count
// has been verified. The returned buffer is owned by the caller. Authorization
// after this call and admission of any side effect remain the caller's job.
func (s *Store) Load(ctx context.Context, identity exports.Identity, ref operations.InputRef) (payload []byte, resultErr error) {
	if err := s.validateReference(ctx, ref); err != nil {
		return nil, err
	}
	r, err := s.storage.Acquire(ctx, ref.ID, identity)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, r.Close())
		if resultErr != nil {
			clear(payload)
			payload = nil
		}
	}()
	manifest := r.Manifest()
	parts := int((ref.Bytes + ChunkBytes - 1) / ChunkBytes)
	if manifest.ID != ref.ID || len(manifest.Parts) != parts || manifest.Rows != int64(parts) || !time.Now().Before(manifest.ExpiresAt) {
		return nil, ErrCorrupt
	}
	expectedSchema := inputSchema(Format(ref.Format))
	payload = make([]byte, 0, int(ref.Bytes))
	owned := payload[:cap(payload)]
	defer func() {
		if resultErr != nil {
			clear(owned)
		}
	}()
	for index, info := range manifest.Parts {
		if info.Rows != 1 || info.Batches != 1 || info.EncodedBytes > ChunkBytes+8<<10 || info.DecodedBytes > ChunkBytes+4<<10 {
			return nil, ErrCorrupt
		}
		part, err := r.OpenPart(ctx, index, identity)
		if err != nil {
			return nil, err
		}
		wanted := int(min(int64(ChunkBytes), ref.Bytes-int64(len(payload))))
		chunk, err := readChunk(ctx, part, expectedSchema, wanted)
		err = errors.Join(err, part.Close())
		if err != nil {
			return nil, err
		}
		payload = append(payload, chunk...)
	}
	digest := sha256.Sum256(payload)
	if len(payload) != int(ref.Bytes) || subtle.ConstantTimeCompare([]byte(hex.EncodeToString(digest[:])), []byte(ref.SHA256)) != 1 || (ref.Format == string(OperationRequest) && !canonicalRequest(payload)) {
		return nil, ErrCorrupt
	}
	// Part reads hold immutable leases. Reacquire after verification to reject a
	// cancellation which won while the final part was being decoded.
	current, err := s.storage.Acquire(ctx, ref.ID, identity)
	if err != nil {
		return nil, err
	}
	latest := current.Manifest()
	err = current.Close()
	if err != nil {
		return nil, err
	}
	if latest.Fence != manifest.Fence || latest.SchemaSHA256 != manifest.SchemaSHA256 || !time.Now().Before(latest.ExpiresAt) {
		return nil, exports.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return payload, nil
}

func readChunk(ctx context.Context, part io.Reader, schema *arrow.Schema, wanted int) (chunk []byte, resultErr error) {
	defer func() {
		if recover() != nil {
			chunk, resultErr = nil, ErrCorrupt
		}
	}()
	reader, err := ipc.NewReader(part)
	if err != nil {
		return nil, ErrCorrupt
	}
	defer reader.Release()
	if !reader.Schema().Equal(schema) || !reader.Schema().Metadata().Equal(schema.Metadata()) || !reader.Next() {
		return nil, ErrCorrupt
	}
	batch := reader.RecordBatch()
	if batch.NumRows() != 1 || batch.NumCols() != 1 {
		return nil, ErrCorrupt
	}
	column, ok := batch.Column(0).(*array.Binary)
	if !ok || column.IsNull(0) || len(column.Value(0)) != wanted || wanted < 1 || wanted > ChunkBytes {
		return nil, ErrCorrupt
	}
	chunk = append([]byte(nil), column.Value(0)...)
	if reader.Next() || reader.Err() != nil {
		return nil, ErrCorrupt
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return chunk, nil
}

// Stream verifies the complete bounded input before the first destination
// write. This deliberately uses up to MaxInputBytes of memory; it is not a
// partial-validation pipeline suitable for executing side effects as it reads.
func (s *Store) Stream(ctx context.Context, identity exports.Identity, ref operations.InputRef, output io.Writer) error {
	if output == nil {
		return ErrInvalid
	}
	payload, err := s.Load(ctx, identity, ref)
	if err != nil {
		return err
	}
	defer clear(payload)
	for len(payload) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A slow destination must not turn a verified buffer into an input
		// stream that remains usable after cancellation or expiry.
		current, err := s.storage.Acquire(ctx, ref.ID, identity)
		if err != nil {
			return err
		}
		if err := current.Close(); err != nil {
			return err
		}
		chunk := payload[:min(len(payload), ChunkBytes)]
		n, err := output.Write(chunk)
		if err != nil {
			return err
		}
		if n != len(chunk) {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}
