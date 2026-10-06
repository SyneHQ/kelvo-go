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
	"github.com/SYNEHQ/kelvo-go/ingestion"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func executeIngestion(ctx context.Context, input adapter.ProcessRequest, session adapter.Session, output io.Writer, receipt operations.Receipt) (operations.Receipt, error) {
	destination, ok := session.(adapter.IngestionSession)
	if !ok {
		receipt.Outcome, receipt.ErrorCode = operations.Rejected, "UNSUPPORTED"
		return receipt, adapter.ErrUnsupported
	}
	scope, err := input.IngestionScope()
	if err != nil {
		receipt.Outcome, receipt.ErrorCode = operations.Rejected, "INVALID_ARGUMENT"
		return receipt, err
	}
	var result any
	switch input.Request.Kind {
	case operations.IngestionInstall:
		err = destination.InstallIngestion(ctx, scope)
	case operations.IngestionState:
		var state ingestion.State
		state, err = destination.IngestionState(ctx, scope)
		if err == nil {
			err = state.Validate()
		}
		result = state
	case operations.IngestionCommit:
		batch, parseErr := ingestion.ParseBatch(input.Input)
		if parseErr != nil {
			return receipt, parseErr
		}
		var committed ingestion.Receipt
		committed, err = destination.CommitIngestion(ctx, scope, batch)
		if err == nil && committed.ValidateBatch(batch) != nil {
			err = ingestion.ErrOutcomeUnknown
		}
		result = committed
	default:
		return receipt, adapter.ErrUnsupported
	}
	if err != nil {
		switch {
		case errors.Is(err, ingestion.ErrOutcomeUnknown):
			receipt.Outcome, receipt.Effect, receipt.ErrorCode = operations.OutcomeUnknown, operations.EffectUnknown, "OUTCOME_UNKNOWN"
		case errors.Is(err, ingestion.ErrConflict):
			receipt.ErrorCode = "CONFLICT"
		case errors.Is(err, ingestion.ErrDatabase):
			receipt.ErrorCode = "DATABASE_MISMATCH"
		case errors.Is(err, ingestion.ErrUninitialized):
			receipt.ErrorCode = "NOT_INITIALIZED"
		case errors.Is(err, adapter.ErrUnsupported):
			receipt.Outcome, receipt.ErrorCode = operations.Rejected, "UNSUPPORTED"
		}
		return receipt, err
	}
	receipt.Outcome, receipt.ErrorCode = operations.Completed, ""
	if input.Request.Kind.Mutating() {
		receipt.Effect = operations.EffectCommitted
	}
	if result == nil {
		return receipt, nil
	}
	// A source commit is definitive even if the optional result pipe fails.
	ref, err := writeIngestionResult(ctx, input, output, result)
	if err != nil && !input.Request.Kind.Mutating() {
		receipt.Outcome, receipt.ErrorCode = operations.Failed, "SOURCE_FAILED"
	}
	receipt.Result = ref
	return receipt, err
}

func writeIngestionResult(ctx context.Context, input adapter.ProcessRequest, output io.Writer, result any) (*operations.ResultRef, error) {
	raw, err := json.Marshal(result)
	if err != nil || len(raw) > ingestion.MaxResultBytes {
		return nil, adapter.ErrLimit
	}
	defer clear(raw)
	wire := &boundedHashWriter{writer: output, hash: sha256.New(), maximum: input.Limits.MaxBytes}
	sink := &arrowSink{ctx: ctx, wire: wire, maxRows: input.Limits.MaxRows, batchRows: int64(input.Limits.BatchRows)}
	metadata := arrow.NewMetadata([]string{"kelvo_ingestion_result"}, []string{string(input.Request.Kind)})
	schema := arrow.NewSchema([]arrow.Field{{Name: "ingestion", Type: arrow.BinaryTypes.Binary, Nullable: false}}, &metadata)
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
