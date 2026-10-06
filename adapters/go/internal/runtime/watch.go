// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/watch"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func executeWatch(ctx context.Context, input adapter.ProcessRequest, session adapter.Session, output io.Writer, receipt operations.Receipt) (operations.Receipt, error) {
	source, ok := session.(adapter.WatchSession)
	if !ok {
		receipt.Outcome, receipt.ErrorCode = operations.Rejected, "UNSUPPORTED"
		return receipt, adapter.ErrUnsupported
	}
	scope, err := input.WatchScope()
	if err != nil {
		receipt.Outcome, receipt.ErrorCode = operations.Rejected, "INVALID_ARGUMENT"
		return receipt, err
	}
	var batch watch.Batch
	v := input.Request.Spec.Watch
	switch input.Request.Kind {
	case operations.WatchInstall:
		if v.Resume != nil {
			importer, ok := session.(adapter.WatchImportSession)
			if !ok || input.Source.Engine != "mongodb" {
				err = adapter.ErrUnsupported
				break
			}
			var resume watch.Import
			resume, err = watch.ParseImport(input.Input, scope, input.Source.Revision)
			if err == nil {
				err = importer.ImportWatch(ctx, scope, resume)
			}
		} else {
			err = source.InstallWatch(ctx, scope)
		}
	case operations.WatchRemove:
		err = source.RemoveWatch(ctx, scope)
	case operations.WatchRead:
		maximum := v.MaxEvents
		if int64(maximum) > input.Limits.MaxRows {
			maximum = int(input.Limits.MaxRows)
		}
		// Leave room for the Arrow schema and framing around the binary batch.
		batch, err = source.ReadWatch(ctx, scope, maximum, v.MaxWaitMS, input.Limits.MaxBytes-4096)
		if err == nil {
			err = batch.Validate(scope)
		}
	case operations.WatchAck:
		var checkpoint watch.Checkpoint
		checkpoint, err = watch.ParseCheckpoint(input.Input, scope)
		if err == nil {
			err = source.AckWatch(ctx, scope, checkpoint, v.SinkReceiptSHA256)
		}
	default:
		err = adapter.ErrUnsupported
	}
	if err != nil {
		switch {
		case errors.Is(err, watch.ErrOutcomeUnknown):
			receipt.Outcome, receipt.Effect, receipt.ErrorCode = operations.OutcomeUnknown, operations.EffectUnknown, "OUTCOME_UNKNOWN"
		case errors.Is(err, watch.ErrConflict):
			receipt.ErrorCode = "CONFLICT"
		case errors.Is(err, watch.ErrUninitialized):
			receipt.ErrorCode = "NOT_INITIALIZED"
		case errors.Is(err, watch.ErrLimit):
			receipt.ErrorCode = "RESOURCE_EXHAUSTED"
		case errors.Is(err, adapter.ErrUnsupported):
			receipt.Outcome, receipt.ErrorCode = operations.Rejected, "UNSUPPORTED"
		}
		return receipt, err
	}
	receipt.Outcome, receipt.ErrorCode = operations.Completed, ""
	if input.Request.Kind.Mutating() {
		receipt.Effect = operations.EffectCommitted
		return receipt, nil
	}
	receipt.Result, err = writeWatchResult(ctx, input, output, batch)
	if err != nil {
		receipt.Outcome, receipt.ErrorCode = operations.Failed, "SOURCE_FAILED"
	}
	return receipt, err
}

func writeWatchResult(ctx context.Context, input adapter.ProcessRequest, output io.Writer, batch watch.Batch) (*operations.ResultRef, error) {
	raw, err := json.Marshal(batch)
	if err != nil || len(raw) > watch.MaxBatchBytes {
		return nil, adapter.ErrLimit
	}
	defer clear(raw)
	wire := &boundedHashWriter{writer: output, hash: sha256.New(), maximum: input.Limits.MaxBytes}
	sink := &arrowSink{ctx: ctx, wire: wire, maxRows: input.Limits.MaxRows, batchRows: int64(input.Limits.BatchRows)}
	metadata := arrow.NewMetadata([]string{"kelvo_watch_result"}, []string{string(input.Request.Kind)})
	schema := arrow.NewSchema([]arrow.Field{{Name: "watch", Type: arrow.BinaryTypes.Binary}}, &metadata)
	if err := sink.Schema(schema); err != nil {
		return nil, err
	}
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	builder.Append(raw)
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	defer record.Release()
	err = sink.Write(record)
	closeErr := sink.writer.Close()
	if err != nil || closeErr != nil || ctx.Err() != nil {
		return nil, errors.Join(err, closeErr, ctx.Err())
	}
	return &operations.ResultRef{ID: input.OperationID, SHA256: hex.EncodeToString(wire.hash.Sum(nil)), Bytes: wire.bytes, Rows: 1, Format: "arrow_ipc"}, nil
}
