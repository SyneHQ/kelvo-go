//go:build !slim

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func (r Runner) Run(parent context.Context, input io.Reader, output io.Writer, receiptOutput io.Writer) error {
	if parent == nil || input == nil || output == nil || receiptOutput == nil {
		return adapter.ErrInvalid
	}
	raw, err := io.ReadAll(io.LimitReader(input, adapter.MaxProcessRequestBytes+1))
	if err != nil || len(raw) > adapter.MaxProcessRequestBytes {
		return adapter.ErrInvalid
	}
	defer clear(raw)
	request, err := adapter.ParseProcessRequest(raw)
	if err != nil {
		return err
	}
	defer clear(request.Input)
	if request.SourceFile != nil && request.Request.Kind == operations.StatementExecute {
		return r.runFileMutation(parent, request, output, receiptOutput)
	}
	if request.Runtime != nil {
		return runJDBC(parent, request, raw, output, receiptOutput)
	}
	receipt, runErr := r.execute(parent, request, output)
	if receipt.Validate() != nil {
		return errors.New("invalid adapter receipt")
	}
	encoded, err := json.Marshal(receipt)
	if err != nil || len(encoded) > operations.MaxReceiptBytes {
		return errors.New("invalid adapter receipt")
	}
	n, err := receiptOutput.Write(encoded)
	if err != nil {
		return err
	}
	if n != len(encoded) {
		return io.ErrShortWrite
	}
	return runErr
}

func (r Runner) execute(parent context.Context, input adapter.ProcessRequest, output io.Writer) (receipt operations.Receipt, resultErr error) {
	receipt = operations.Receipt{Version: operations.Version, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256, Outcome: operations.Rejected, Effect: operations.EffectNone, ErrorCode: "INVALID_ARGUMENT"}
	if err := input.Validate(); err != nil {
		return receipt, err
	}
	deadline := time.Unix(input.ExpiresAt, 0)
	if bounded := time.Now().Add(time.Duration(input.Limits.TimeoutMS) * time.Millisecond); bounded.Before(deadline) {
		deadline = bounded
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	defer func() { receipt = withFailureCode(ctx, receipt, resultErr) }()
	if err := ctx.Err(); err != nil {
		receipt.Outcome = operations.CancelledBeforeStart
		receipt.ErrorCode = "CANCELLED"
		return receipt, err
	}
	limits := adapter.Limits{MaxRows: input.Limits.MaxRows, MaxBytes: input.Limits.MaxBytes, BatchRows: input.Limits.BatchRows}
	invocation, err := adapter.FromOperation(input.Request, limits)
	if err != nil {
		receipt.ErrorCode = "UNSUPPORTED"
		return receipt, err
	}
	if err := input.Validate(); err != nil {
		receipt.ErrorCode = "PERMISSION_DENIED"
		return receipt, err
	}
	open := r.Open
	var closePrivate func() error
	defer func() {
		if closePrivate != nil {
			if err := closePrivate(); err != nil {
				resultErr = errors.Join(resultErr, errors.New("private channel cleanup failed"))
			}
		}
	}()
	if input.PrivateTransport != nil {
		if open != nil {
			receipt.ErrorCode = "PERMISSION_DENIED"
			return receipt, adapter.ErrInvalid
		}
		open = func(ctx context.Context, spec adapter.ConnectionSpec, request operations.Request) (adapter.Session, error) {
			var session adapter.Session
			var err error
			session, closePrivate, err = openPrivateSource(ctx, spec, request, input.PrivateTransport)
			return session, err
		}
	}
	if open == nil {
		open = openSource
		if input.SourceFile != nil {
			open = func(ctx context.Context, _ adapter.ConnectionSpec, _ operations.Request) (adapter.Session, error) {
				return openFileSource(ctx, input)
			}
		} else if adapter.NativeReader(input.Source.Engine) {
			open = func(ctx context.Context, _ adapter.ConnectionSpec, _ operations.Request) (adapter.Session, error) {
				return openNativeReaderSource(ctx, input)
			}
		} else if input.Source.Engine == "google_sheets" {
			open = func(ctx context.Context, _ adapter.ConnectionSpec, _ operations.Request) (adapter.Session, error) {
				return openSheetsSource(ctx, input)
			}
		} else if adapter.CloudSQL(input.Source.Engine) {
			open = func(ctx context.Context, _ adapter.ConnectionSpec, _ operations.Request) (adapter.Session, error) {
				return openCloudSQLSource(ctx, input)
			}
		}
	}
	openCtx, stopOpen := context.WithDeadline(ctx, time.Unix(input.CredentialsValidUntil, 0))
	session, err := open(openCtx, input.Source, input.Request)
	stopOpen()
	if err != nil {
		if session != nil {
			_ = session.Close()
		}
		receipt.ErrorCode = "SOURCE_FAILED"
		return receipt, err
	}
	if session == nil {
		receipt.ErrorCode = "SOURCE_FAILED"
		return receipt, adapter.ErrInvalid
	}
	defer func() {
		// Confirmed source effects survive a later connection cleanup failure.
		if err := session.Close(); err != nil {
			resultErr = errors.Join(resultErr, errors.New("adapter session cleanup failed"))
		}
	}()
	if err := ctx.Err(); err != nil {
		receipt.Outcome = operations.CancelledBeforeStart
		receipt.ErrorCode = "CANCELLED"
		return receipt, err
	}
	receipt.Outcome = operations.Failed
	receipt.ErrorCode = "SOURCE_FAILED"
	return executeInvocation(ctx, input, invocation, session, output, receipt)
}

func executeInvocation(ctx context.Context, input adapter.ProcessRequest, invocation adapter.Invocation, session adapter.Session, output io.Writer, receipt operations.Receipt) (operations.Receipt, error) {
	switch {
	case invocation.Migration != nil:
		return executeMigration(ctx, input, session, output, receipt)
	case invocation.Watch != nil:
		return executeWatch(ctx, input, session, output, receipt)
	case invocation.Native != nil:
		return executeNative(ctx, input, *invocation.Native, session, output, receipt)
	case invocation.Ingestion != nil:
		return executeIngestion(ctx, input, session, output, receipt)
	case invocation.Test:
		s, ok := session.(adapter.TestSession)
		if !ok {
			receipt.Outcome = operations.Rejected
			receipt.ErrorCode = "UNSUPPORTED"
			return receipt, adapter.ErrUnsupported
		}
		if err := s.Test(ctx); err != nil {
			return receipt, err
		}
		receipt.Outcome = operations.Completed
		receipt.ErrorCode = ""
		return receipt, nil
	case invocation.Change != nil:
		s, ok := session.(adapter.ChangeSession)
		if !ok {
			receipt.Outcome = operations.Rejected
			receipt.ErrorCode = "UNSUPPORTED"
			return receipt, adapter.ErrUnsupported
		}
		result, err := s.Execute(ctx, *invocation.Change)
		switch result.Outcome {
		case "succeeded":
			receipt.Outcome = operations.Completed
			receipt.Effect = operations.EffectCommitted
			receipt.ErrorCode = ""
			receipt.AffectedRows = result.AffectedRows
		case "failed":
			if result.Completed > 0 {
				receipt.Effect = operations.EffectPartial
			}
		default:
			receipt.Outcome = operations.OutcomeUnknown
			receipt.Effect = operations.EffectUnknown
			receipt.ErrorCode = "OUTCOME_UNKNOWN"
		}
		if err != nil {
			var statement *int
			if input.Request.Spec.Statement.Batch != nil && result.Attempted > 0 {
				failed := result.Attempted - 1
				statement = &failed
			}
			receipt.SourceError = sourceError(err, statement)
		}
		if batch := input.Request.Spec.Statement.Batch; batch != nil {
			steps, stepErr := changeSteps(batch, result, invocation.Change.Transaction)
			if stepErr != nil {
				return operations.Receipt{}, stepErr
			}
			receipt.Steps = steps
		}
		return receipt, err
	case invocation.Query != nil || invocation.Metadata != nil:
		return executeRead(ctx, input, invocation, session, output, receipt)
	default:
		receipt.Outcome = operations.Rejected
		receipt.ErrorCode = "UNSUPPORTED"
		return receipt, adapter.ErrUnsupported
	}
}

func changeSteps(batch *operations.BatchSpec, result adapter.ChangeResult, transaction bool) ([]operations.StepReceipt, error) {
	count := len(batch.Statements)
	if result.Attempted < 0 || result.Attempted > count || result.Completed < 0 || result.Completed > result.Attempted {
		return nil, adapter.ErrInvalid
	}
	if result.Outcome == "succeeded" && (result.Attempted != count || result.Completed != count) {
		return nil, adapter.ErrInvalid
	}
	if transaction && result.Outcome != "succeeded" && result.Completed != 0 {
		return nil, adapter.ErrInvalid
	}
	steps := make([]operations.StepReceipt, count)
	for i, statement := range batch.Statements {
		digest, err := operations.StatementDigest(statement)
		if err != nil {
			return nil, err
		}
		effect := operations.EffectNone
		if i < result.Completed {
			effect = operations.EffectCommitted
		}
		if result.Outcome != "succeeded" && result.Outcome != "failed" && i < result.Attempted && (transaction || i >= result.Completed) {
			effect = operations.EffectUnknown
		}
		steps[i] = operations.StepReceipt{Index: i, SHA256: digest, Effect: effect}
	}
	return steps, nil
}
