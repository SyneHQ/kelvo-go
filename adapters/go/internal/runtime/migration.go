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
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func executeMigration(ctx context.Context, input adapter.ProcessRequest, session adapter.Session, output io.Writer, receipt operations.Receipt) (operations.Receipt, error) {
	s, ok := session.(adapter.MigrationSession)
	if !ok {
		receipt.Outcome, receipt.ErrorCode = operations.Rejected, "UNSUPPORTED"
		return receipt, adapter.ErrUnsupported
	}
	var result any
	var err error
	if input.Request.Kind == operations.MigrationStatus {
		var state migration.State
		state, err = s.MigrationStatus(ctx)
		if err == nil {
			err = state.Validate()
		}
		result = state
	} else {
		plan, parseErr := migration.ParsePlan(input.Input)
		if parseErr != nil {
			return receipt, parseErr
		}
		var applied migration.Result
		applied, err = s.ApplyMigration(ctx, plan)
		if applied.Validate() != nil {
			err = migration.ErrOutcomeUnknown
		} else {
			receipt.Effect = applied.Effect
		}
		result = applied
	}
	if err != nil {
		switch {
		case errors.Is(err, migration.ErrOutcomeUnknown), receipt.Effect == operations.EffectUnknown:
			receipt.Outcome, receipt.Effect, receipt.ErrorCode = operations.OutcomeUnknown, operations.EffectUnknown, "OUTCOME_UNKNOWN"
		case errors.Is(err, migration.ErrConflict), errors.Is(err, migration.ErrDirty):
			receipt.ErrorCode = "CONFLICT"
		case errors.Is(err, adapter.ErrUnsupported):
			receipt.Outcome, receipt.ErrorCode = operations.Rejected, "UNSUPPORTED"
		}
		return receipt, err
	}
	if receipt.Effect != operations.EffectNone && receipt.Effect != operations.EffectCommitted {
		receipt.Outcome, receipt.Effect, receipt.ErrorCode = operations.OutcomeUnknown, operations.EffectUnknown, "OUTCOME_UNKNOWN"
		return receipt, migration.ErrOutcomeUnknown
	}
	receipt.Outcome, receipt.ErrorCode = operations.Completed, ""
	ref, err := writeMigrationResult(ctx, input, output, result)
	// Delivery failure cannot undo a confirmed migration or cause it to replay.
	if err != nil && receipt.Effect == operations.EffectNone {
		receipt.Outcome, receipt.ErrorCode = operations.Failed, "SOURCE_FAILED"
	}
	receipt.Result = ref
	return receipt, err
}

func writeMigrationResult(ctx context.Context, input adapter.ProcessRequest, output io.Writer, result any) (*operations.ResultRef, error) {
	raw, err := json.Marshal(result)
	if err != nil || len(raw) > migration.MaxResultBytes {
		return nil, adapter.ErrLimit
	}
	defer clear(raw)
	wire := &boundedHashWriter{writer: output, hash: sha256.New(), maximum: input.Limits.MaxBytes}
	sink := &arrowSink{ctx: ctx, wire: wire, maxRows: input.Limits.MaxRows, batchRows: int64(input.Limits.BatchRows)}
	metadata := arrow.NewMetadata([]string{"kelvo_migration_result"}, []string{string(input.Request.Kind)})
	schema := arrow.NewSchema([]arrow.Field{{Name: "migration", Type: arrow.BinaryTypes.Binary, Nullable: false}}, &metadata)
	if err := sink.Schema(schema); err != nil {
		return nil, err
	}
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	b.Append(raw)
	values := b.NewArray()
	b.Release()
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
