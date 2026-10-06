package adapter

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/operations"
	"io"
)

// FileMutationSession prepares a new image without changing the original.
// Only the parent's separately authorized compare-and-swap publishes it.
type FileMutationSession interface {
	Session
	PrepareFileChange(context.Context, Change, io.Writer) (ChangeResult, *filesnapshot.Descriptor, error)
}

// FileProcessReceipt is private subprocess framing, never a ledger receipt.
// Receipt effects describe the candidate until parent publication succeeds.
type FileProcessReceipt struct {
	Version   int                      `json:"version"`
	Receipt   operations.Receipt       `json:"receipt"`
	Candidate *filesnapshot.Descriptor `json:"candidate,omitempty"`
}

func (r FileProcessReceipt) Validate(input ProcessRequest) error {
	if r.Version != 1 || input.SourceFile == nil || input.Request.Kind != operations.StatementExecute || r.Receipt.Validate() != nil || r.Receipt.OperationID != input.OperationID || r.Receipt.RequestSHA256 != input.RequestSHA256 || r.Receipt.Result != nil || operations.ValidateStatementReceipt(input.Request, r.Receipt) != nil {
		return ErrInvalid
	}
	if r.Candidate != nil {
		if r.Candidate.Validate() != nil || r.Candidate.Format != input.SourceFile.Format || r.Receipt.Effect != operations.EffectCommitted && r.Receipt.Effect != operations.EffectPartial {
			return ErrInvalid
		}
	} else if r.Receipt.Effect != operations.EffectNone {
		return ErrInvalid
	}
	return nil
}
