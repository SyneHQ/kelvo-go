// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func executeNative(ctx context.Context, input adapter.ProcessRequest, native adapter.Native, session adapter.Session, output io.Writer, receipt operations.Receipt) (operations.Receipt, error) {
	source, ok := session.(adapter.NativeSession)
	if !ok {
		receipt.Outcome, receipt.ErrorCode = operations.Rejected, "UNSUPPORTED"
		return receipt, adapter.ErrUnsupported
	}
	var sink *arrowSink
	var receiver adapter.Sink
	if native.Kind == operations.NativeRead || native.Spec.ReturnResult {
		sink = &arrowSink{ctx: ctx, wire: &boundedHashWriter{writer: output, hash: sha256.New(), maximum: input.Limits.MaxBytes}, maxRows: input.Limits.MaxRows, batchRows: int64(input.Limits.BatchRows)}
		receiver = sink
		if input.Source.Engine == "mongodb" {
			receiver = &mongoJSONSink{next: sink, maxBytes: input.Limits.MaxBytes}
		}
	}
	result, err := source.RunNative(ctx, native, receiver)
	if sink != nil && sink.writer != nil {
		err = errors.Join(err, sink.writer.Close())
	}
	if native.Kind == operations.NativeRead {
		if result.Effect != operations.EffectNone {
			return receipt, errors.New("invalid native read effect")
		}
		if err != nil || result.Outcome != operations.Completed || sink.writer == nil || result.Stats.Rows != sink.rows || ctx.Err() != nil {
			if errors.Is(err, adapter.ErrUnsupported) {
				receipt.Outcome, receipt.ErrorCode = operations.Rejected, "UNSUPPORTED"
			}
			return receipt, errors.Join(err, errors.New("native result incomplete"))
		}
		receipt.Outcome, receipt.ErrorCode = operations.Completed, ""
		receipt.Result = &operations.ResultRef{ID: input.OperationID, SHA256: hex.EncodeToString(sink.wire.hash.Sum(nil)), Rows: sink.rows, Bytes: sink.wire.bytes, Format: "arrow_ipc"}
		return receipt, nil
	}
	// Effects must describe source acknowledgement, even when cleanup or later
	// transport fails. A driver cannot turn an uncertain write into a retry.
	switch {
	case result.Outcome == operations.Completed && result.Effect == operations.EffectCommitted:
		receipt.Outcome, receipt.Effect, receipt.ErrorCode = operations.Completed, operations.EffectCommitted, ""
		receipt.AffectedRows = result.AffectedRows
		if native.Spec.ReturnResult {
			if err == nil && sink != nil && sink.writer != nil && result.Stats.Rows == sink.rows && ctx.Err() == nil {
				receipt.Result = &operations.ResultRef{ID: input.OperationID, SHA256: hex.EncodeToString(sink.wire.hash.Sum(nil)), Rows: sink.rows, Bytes: sink.wire.bytes, Format: "arrow_ipc"}
			} else {
				// The mutation is confirmed even if its response cannot be retained.
				err = errors.Join(err, errors.New("native mutation result incomplete"))
			}
		}
	case result.Outcome == operations.Rejected && result.Effect == operations.EffectNone:
		receipt.Outcome, receipt.ErrorCode = operations.Rejected, "INVALID_ARGUMENT"
		if errors.Is(err, adapter.ErrUnsupported) {
			receipt.ErrorCode = "UNSUPPORTED"
		}
	case result.Outcome == operations.CancelledBeforeStart && result.Effect == operations.EffectNone:
		receipt.Outcome, receipt.ErrorCode = operations.CancelledBeforeStart, "CANCELLED"
	case result.Outcome == operations.Failed && (result.Effect == operations.EffectNone || result.Effect == operations.EffectPartial):
		receipt.Outcome, receipt.Effect = operations.Failed, result.Effect
	default:
		receipt.Outcome, receipt.Effect, receipt.ErrorCode = operations.OutcomeUnknown, operations.EffectUnknown, "OUTCOME_UNKNOWN"
	}
	return receipt, err
}
