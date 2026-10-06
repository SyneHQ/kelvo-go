package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func (r Runner) runFileMutation(parent context.Context, input adapter.ProcessRequest, output, receiptOutput io.Writer) error {
	if input.Validate() != nil || input.SourceFile == nil || input.Request.Kind != operations.StatementExecute {
		return adapter.ErrInvalid
	}
	ctx, cancel := context.WithDeadline(parent, time.Unix(input.ExpiresAt, 0))
	defer cancel()
	receipt := operations.Receipt{Version: 1, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256, Outcome: operations.Rejected, Effect: operations.EffectNone, ErrorCode: "UNSUPPORTED"}
	frame := adapter.FileProcessReceipt{Version: 1, Receipt: receipt}
	run := func() error {
		invocation, err := adapter.FromOperation(input.Request, adapter.Limits{MaxRows: input.Limits.MaxRows, MaxBytes: input.Limits.MaxBytes, BatchRows: input.Limits.BatchRows})
		if err != nil || invocation.Change == nil {
			return adapter.ErrInvalid
		}
		open := r.Open
		if open == nil {
			open = func(ctx context.Context, _ adapter.ConnectionSpec, _ operations.Request) (adapter.Session, error) {
				return openFileSource(ctx, input)
			}
		}
		openCtx, stop := context.WithDeadline(ctx, time.Unix(input.CredentialsValidUntil, 0))
		session, err := open(openCtx, input.Source, input.Request)
		stop()
		if err != nil {
			return err
		}
		defer session.Close()
		mutator, ok := session.(adapter.FileMutationSession)
		if !ok {
			return adapter.ErrUnsupported
		}
		bounded := &boundedHashWriter{writer: output, hash: sha256.New(), maximum: filesnapshot.MaxBytes}
		// The frame binds the replacement hash; no Arrow or public result reference
		// is emitted. Parent publication remains a separate, authenticated action.
		result, candidate, executionErr := mutator.PrepareFileChange(ctx, *invocation.Change, bounded)
		frame.Candidate = candidate
		frame.Receipt.Outcome = operations.Failed
		frame.Receipt.ErrorCode = "SOURCE_FAILED"
		if candidate != nil {
			if result.Outcome == "succeeded" {
				frame.Receipt.Outcome = operations.Completed
				frame.Receipt.Effect = operations.EffectCommitted
				frame.Receipt.ErrorCode = ""
			} else if result.Completed > 0 {
				frame.Receipt.Effect = operations.EffectPartial
			} else {
				return adapter.ErrInvalid
			}
			frame.Receipt.AffectedRows = result.AffectedRows
		}
		if batch := input.Request.Spec.Statement.Batch; batch != nil {
			frame.Receipt.Steps, err = changeSteps(batch, result, invocation.Change.Transaction)
			if err != nil {
				return err
			}
		}
		// An execution failure with a valid partial image is conveyed in the frame.
		// Exiting successfully proves the private image stream itself is complete.
		if candidate != nil {
			return nil
		}
		return executionErr
	}
	runErr := run()
	if frame.Validate(input) != nil {
		return errors.New("invalid file adapter receipt")
	}
	encoded, err := json.Marshal(frame)
	if err != nil || len(encoded) > operations.MaxReceiptBytes {
		return adapter.ErrInvalid
	}
	if n, err := receiptOutput.Write(encoded); err != nil {
		return err
	} else if n != len(encoded) {
		return io.ErrShortWrite
	}
	if frame.Candidate != nil {
		return nil
	}
	return runErr
}
