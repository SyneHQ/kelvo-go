package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type nativeProcessSession struct {
	processSession
	nativeResult adapter.NativeResult
	nativeError  error
	nativeCalls  int
	writing      bool
}

func (s *nativeProcessSession) RunNative(ctx context.Context, call adapter.Native, sink adapter.Sink) (adapter.NativeResult, error) {
	s.nativeCalls++
	result := s.nativeResult
	if call.Kind == operations.NativeRead || call.Spec.ReturnResult {
		stats, err := s.Query(ctx, adapter.Query{}, sink)
		result.Stats = stats
		if err != nil {
			return result, err
		}
	} else if sink != nil {
		return adapter.NativeResult{}, errors.New("write received result sink")
	}
	return result, s.nativeError
}

func TestNativeMutationResultPreservesConfirmedEffectOnPipeFailure(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			input := runtimeRequest(t, operations.QueryRead)
			input.Request.Kind = operations.NativeExecute
			input.Request.IdempotencyKey = "provider-write"
			input.Request.Spec = operations.Spec{Native: &operations.NativeSpec{Provider: "motherduck", Command: "query", ReturnResult: true}}
			input.RequestSHA256, _ = operations.Digest(input.Request)
			raw, _ := json.Marshal(input)
			session := &nativeProcessSession{processSession: processSession{rows: 3}, nativeResult: adapter.NativeResult{Outcome: operations.Completed, Effect: operations.EffectCommitted}}
			var out, receiptBytes bytes.Buffer
			var output io.Writer = &out
			if failure {
				output = nativeFailWriter{}
			}
			err := (Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
				return session, nil
			}}).Run(context.Background(), bytes.NewReader(raw), output, &receiptBytes)
			var receipt operations.Receipt
			if operations.DecodeStrict(receiptBytes.Bytes(), &receipt, operations.MaxReceiptBytes) != nil || receipt.Validate() != nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectCommitted || session.nativeCalls != 1 {
				t.Fatal("confirmed provider write was lost", receipt, err)
			}
			if failure {
				if err == nil || receipt.Result != nil {
					t.Fatal("invalid mutation result retained")
				}
			} else if err != nil || receipt.Result == nil || receipt.Result.Rows != 3 {
				t.Fatal("mutation result unavailable", receipt, err)
			}
		})
	}
}

type nativeFailWriter struct{}

func (nativeFailWriter) Write([]byte) (int, error) { return 0, errors.New("result pipe closed") }
func runNativeProcess(t *testing.T, write bool, session *nativeProcessSession) (operations.Receipt, []byte, error) {
	t.Helper()
	session.bsonOutput = true
	request := runtimeRequest(t, operations.QueryRead)
	request.Request.Kind = operations.NativeRead
	command := "find"
	if write {
		request.Request.Kind = operations.NativeExecute
		request.Request.IdempotencyKey = "mutation"
		command = "insert_one"
	}
	request.Request.Spec = operations.Spec{Native: &operations.NativeSpec{Provider: "mongodb", Command: command, Parameters: []operations.Parameter{{Type: "json", Value: json.RawMessage(`{"collection":"orders"}`)}}}}
	request.RequestSHA256, _ = operations.Digest(request.Request)
	request.Source.Engine = "mongodb"
	raw, _ := json.Marshal(request)
	var output, receiptBytes bytes.Buffer
	err := (Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
		return session, nil
	}}).Run(context.Background(), bytes.NewReader(raw), &output, &receiptBytes)
	var receipt operations.Receipt
	if e := operations.DecodeStrict(receiptBytes.Bytes(), &receipt, operations.MaxReceiptBytes); e != nil {
		t.Fatal(e, err)
	}
	if bytes.Contains(receiptBytes.Bytes(), []byte("secret-provider-error")) {
		t.Fatal("provider error leaked")
	}
	if session.nativeCalls != 1 || session.closeCount != 1 {
		t.Fatal("native execution replayed or cleanup missing")
	}
	return receipt, output.Bytes(), err
}

func TestNativeProcessReadReceiptBindsExactArrow(t *testing.T) {
	s := &nativeProcessSession{processSession: processSession{rows: 3}, nativeResult: adapter.NativeResult{Outcome: operations.Completed, Effect: operations.EffectNone}}
	receipt, out, err := runNativeProcess(t, false, s)
	digest := sha256.Sum256(out)
	if err != nil || receipt.Outcome != operations.Completed || receipt.Result == nil || receipt.Result.Rows != 3 || receipt.Result.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatal(receipt, err)
	}
	for _, test := range []struct {
		name string
		edit func(*nativeProcessSession)
	}{
		{"rows", func(s *nativeProcessSession) { s.rows = 101 }},
		{"stats", func(s *nativeProcessSession) { s.badStats = true }},
		{"schema", func(s *nativeProcessSession) { s.noSchema = true }},
		{"source", func(s *nativeProcessSession) { s.nativeError = errors.New("secret-provider-error") }},
		{"effect", func(s *nativeProcessSession) { s.nativeResult.Effect = operations.EffectCommitted }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := &nativeProcessSession{processSession: processSession{rows: 3}, nativeResult: adapter.NativeResult{Outcome: operations.Completed, Effect: operations.EffectNone}}
			test.edit(s)
			receipt, _, err := runNativeProcess(t, false, s)
			if err == nil || receipt.Outcome == operations.Completed || receipt.Result != nil {
				t.Fatal(receipt, err)
			}
		})
	}
}

func TestNativeProcessWriteRetainsSourceEffectWithoutReplay(t *testing.T) {
	for _, test := range []struct {
		outcome operations.Outcome
		effect  operations.Effect
	}{
		{operations.Completed, operations.EffectCommitted},
		{operations.Rejected, operations.EffectNone},
		{operations.OutcomeUnknown, operations.EffectUnknown},
		{operations.Failed, operations.EffectPartial},
		{operations.CancelledBeforeStart, operations.EffectNone},
	} {
		s := &nativeProcessSession{processSession: processSession{closeErr: errors.New("cleanup failed")}, nativeResult: adapter.NativeResult{Outcome: test.outcome, Effect: test.effect}, nativeError: errors.New("secret-provider-error")}
		receipt, out, err := runNativeProcess(t, true, s)
		if err == nil || receipt.Outcome != test.outcome || receipt.Effect != test.effect || receipt.Result != nil || len(out) != 0 {
			t.Fatal(receipt, err)
		}
	}
}
