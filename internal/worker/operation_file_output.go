package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// Only parent-owned bytes can be published. The child never receives this file
// descriptor or its pathname; it emits the candidate through stdout.
func finishFileOperation(ctx context.Context, input adapter.ProcessRequest, read operationReceiptRead, file *os.File, bytes int64, digest string, delivered bool) (operations.Receipt, error) {
	rejected := rejectedOperation(input, "SOURCE_FAILED")
	var frame adapter.FileProcessReceipt
	if read.err != nil || operations.DecodeStrict(read.raw, &frame, operations.MaxReceiptBytes) != nil || frame.Validate(input) != nil {
		return rejected, operationFailure("SOURCE_FAILED")
	}
	if frame.Candidate == nil {
		if bytes != 0 {
			return rejected, operationFailure("SOURCE_FAILED")
		}
		return frame.Receipt, nil
	}
	if !delivered || file == nil || frame.Candidate.Bytes != bytes || frame.Candidate.SHA256 != digest || ctx.Err() != nil {
		return rejected, operationFailure("SOURCE_FAILED")
	}
	if file.Sync() != nil {
		return rejected, operationFailure("SOURCE_FAILED")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return rejected, operationFailure("SOURCE_FAILED")
	}
	// Rehash the parent copy before opening the publication request.
	hash := sha256.New()
	count, err := io.Copy(hash, file)
	if err != nil || count != bytes || hex.EncodeToString(hash.Sum(nil)) != digest {
		return rejected, operationFailure("SOURCE_FAILED")
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return rejected, operationFailure("SOURCE_FAILED")
	}
	publish, ok := ctx.Value(operationFilePublisherKey{}).(OperationFilePublisher)
	if !ok || publish == nil {
		return rejected, operationFailure("UNAVAILABLE")
	}
	committed, err := publish(ctx, input, *frame.Candidate, file)
	if err != nil {
		return uncertainOperation(input), operationFailure("OUTCOME_UNKNOWN")
	}
	if !committed {
		return rejectedOperation(input, "CONFLICT"), operationFailure("CONFLICT")
	}
	return frame.Receipt, nil
}
