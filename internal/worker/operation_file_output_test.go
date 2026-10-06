package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func fileMutationInput(t *testing.T) adapter.ProcessRequest {
	input := operationProcessInput(t, operations.StatementExecute, "trips SET fare=2")
	input.Request.Connection.Schema = ""
	input.Source.Schema = ""
	input.Source.Password = ""
	input.Source.Engine = "sqlite"
	input.RequestSHA256, _ = operations.Digest(input.Request)
	input.SourceFile = &filesnapshot.Descriptor{Version: 1, Format: "sqlite", Bytes: 8, SHA256: strings.Repeat("a", 64)}
	input.Source.Options = map[string]string{"file_format": "sqlite", "file_bytes": strconv.FormatInt(input.SourceFile.Bytes, 10), "file_sha256": input.SourceFile.SHA256}
	input.Limits.MemoryMB, input.Limits.Threads, input.Limits.MaxTempMB = 64, 1, 32
	if input.Validate() != nil {
		t.Fatal("invalid file input")
	}
	return input
}
func TestFileReceiptPublishesOnlyVerifiedParentCopy(t *testing.T) {
	data := []byte("candidate")
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	for _, mode := range []string{"committed", "conflict", "lost-reply", "bad-hash", "bad-bytes", "bad-operation", "interrupted", "candidate-changed"} {
		t.Run(mode, func(t *testing.T) {
			input := fileMutationInput(t)
			frame := adapter.FileProcessReceipt{Version: 1, Receipt: operations.Receipt{Version: 1, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256, Outcome: operations.Completed, Effect: operations.EffectCommitted}, Candidate: &filesnapshot.Descriptor{Version: 1, Format: "sqlite", Bytes: int64(len(data)), SHA256: digest}}
			switch mode {
			case "bad-hash":
				frame.Candidate.SHA256 = strings.Repeat("b", 64)
			case "bad-bytes":
				frame.Candidate.Bytes++
			case "bad-operation":
				frame.Receipt.OperationID = "other"
			}
			raw, err := json.Marshal(frame)
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.CreateTemp(t.TempDir(), "candidate-")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err = file.Write(data); err != nil {
				t.Fatal(err)
			}
			if mode == "candidate-changed" {
				if _, err = file.WriteAt([]byte("x"), 0); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			ctx := WithOperationFilePublisher(context.Background(), func(_ context.Context, got adapter.ProcessRequest, descriptor filesnapshot.Descriptor, source io.Reader) (bool, error) {
				calls++
				body, err := io.ReadAll(source)
				if err != nil || string(body) != string(data) || got.OperationID != input.OperationID || descriptor.SHA256 != digest {
					t.Error("unbound publication")
				}
				if mode == "lost-reply" {
					return false, errors.New("connection lost")
				}
				return mode != "conflict", nil
			})
			receipt, err := finishFileOperation(ctx, input, operationReceiptRead{raw: raw}, file, int64(len(data)), digest, mode != "interrupted")
			if receipt.Validate() != nil {
				t.Fatal("invalid public operation receipt", receipt)
			}
			switch mode {
			case "committed":
				if err != nil || receipt.Effect != operations.EffectCommitted || calls != 1 {
					t.Fatal(receipt, calls, err)
				}
			case "conflict":
				if err == nil || receipt.Effect != operations.EffectNone || calls != 1 {
					t.Fatal(receipt, calls, err)
				}
			case "lost-reply":
				if err == nil || receipt.Effect != operations.EffectUnknown || receipt.Outcome != operations.OutcomeUnknown || calls != 1 {
					t.Fatal(receipt, calls, err)
				}
			default:
				if err == nil || receipt.Effect != operations.EffectNone || calls != 0 {
					t.Fatal(receipt, calls, err)
				}
			}
		})
	}
}
