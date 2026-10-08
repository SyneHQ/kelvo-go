// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func operationProcessInput(t *testing.T, kind operations.Kind, mode string) adapter.ProcessRequest {
	t.Helper()
	request := operations.Request{Version: operations.Version, Kind: kind,
		Connection: operations.ConnectionRef{ID: "saved-connection", Database: "customer", Schema: "public"}}
	switch kind {
	case operations.QueryRead:
		request.Spec.Query = &operations.QuerySpec{SQL: "SELECT " + mode}
	case operations.MetadataInspect:
		request.Spec.Metadata = &operations.MetadataSpec{Object: "tables", Limit: 10}
	case operations.NativeExecute:
		request.IdempotencyKey = "test-native-write"
		request.Spec.Native = &operations.NativeSpec{Provider: "fixture", Command: mode, ReturnResult: mode != "no_result"}
	case operations.StatementExecute:
		request.IdempotencyKey = "test-write"
		request.Spec.Statement = &operations.StatementSpec{SQL: "UPDATE " + mode, Transaction: operations.TransactionRequired}
		if mode == "large_receipt" {
			request.Spec.Statement.SQL = ""
			request.Spec.Statement.Batch = &operations.BatchSpec{}
			for i := 0; i < 100; i++ {
				request.Spec.Statement.Batch.Statements = append(request.Spec.Statement.Batch.Statements, operations.BoundStatement{SQL: "UPDATE " + mode})
			}
		}
	}
	digest, err := operations.Digest(request)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	return adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: "operation-test", RequestSHA256: digest, Request: request,
		Limits: adapter.ProcessLimits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 10, TimeoutMS: 10000},
		Source: adapter.ConnectionSpec{Engine: "fixture", TenantID: "tenant-a", ConnectionID: request.Connection.ID,
			Revision: "revision-1", Database: request.Connection.Database, Schema: request.Connection.Schema, Password: "fixture-private-value"},
		CredentialsValidUntil: now + 5, ExpiresAt: now + 30}
}

func operationReceiptBytes(t *testing.T, receipt operations.Receipt) operationReceiptRead {
	t.Helper()
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	return operationReceiptRead{raw: raw}
}

func TestOperationReceiptRequiresBoundObservedArrow(t *testing.T) {
	input := operationProcessInput(t, operations.QueryRead, "fixture")
	wire := ipcFixture(t)
	hash := sha256.Sum256(wire)
	digest := hex.EncodeToString(hash[:])
	stats, err := readWorkerIPC(context.Background(), bytes.NewReader(wire), query.DefaultLimits(), &workerTestSink{})
	if err != nil {
		t.Fatal(err)
	}
	base := operations.Receipt{Version: operations.Version, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256,
		Outcome: operations.Completed, Effect: operations.EffectNone}
	for _, test := range []struct {
		name      string
		change    func(*operations.Receipt)
		delivered bool
		valid     bool
	}{
		{"valid", func(*operations.Receipt) {}, true, true},
		{"wrong_operation", func(r *operations.Receipt) { r.OperationID = "other" }, true, false},
		{"wrong_request", func(r *operations.Receipt) { r.RequestSHA256 = strings.Repeat("a", 64) }, true, false},
		{"wrong_result_id", func(r *operations.Receipt) { r.Result.ID = "other" }, true, false},
		{"wrong_wire_digest", func(r *operations.Receipt) { r.Result.SHA256 = strings.Repeat("a", 64) }, true, false},
		{"wrong_rows", func(r *operations.Receipt) { r.Result.Rows++ }, true, false},
		{"wrong_bytes", func(r *operations.Receipt) { r.Result.Bytes++ }, true, false},
		{"missing_result", func(r *operations.Receipt) { r.Result = nil }, true, false},
		{"delivery_failed", func(*operations.Receipt) {}, false, false},
		{"read_claims_write", func(r *operations.Receipt) { r.Effect = operations.EffectCommitted }, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := base
			r.Result = &operations.ResultRef{ID: input.OperationID, SHA256: digest, Bytes: int64(len(wire)), Rows: stats.Rows, Format: "arrow_ipc"}
			test.change(&r)
			got, err := operationProcessReceipt(input, operationReceiptBytes(t, r), true, stats, digest, test.delivered)
			if test.valid {
				if err != nil || got.Outcome != operations.Completed {
					t.Fatal(got, err)
				}
			} else if err == nil || got.Outcome != operations.Failed || got.Effect != operations.EffectNone {
				t.Fatal("unverified read receipt accepted", got, err)
			}
		})
	}
}

func TestOperationReceiptPreservesMutationEvidence(t *testing.T) {
	input := operationProcessInput(t, operations.StatementExecute, "fixture")
	for _, outcome := range []operations.Outcome{operations.Completed, operations.Failed, operations.CancelledBeforeStart} {
		t.Run(string(outcome), func(t *testing.T) {
			r := operations.Receipt{Version: operations.Version, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256, Outcome: outcome}
			switch outcome {
			case operations.Completed:
				r.Effect = operations.EffectCommitted
			case operations.Failed:
				r.Effect = operations.EffectPartial
				r.ErrorCode = "SOURCE_FAILED"
			case operations.CancelledBeforeStart:
				r.Effect = operations.EffectNone
				r.ErrorCode = "CANCELLED"
			}
			got, err := operationProcessReceipt(input, operationReceiptBytes(t, r), false, query.Stats{}, "", false)
			expected := r.Outcome
			if expected == operations.CancelledBeforeStart {
				expected = operations.Rejected
			}
			if err != nil || got.Outcome != expected || got.Effect != r.Effect {
				t.Fatal("lost source evidence on delivery failure", got, err)
			}
		})
	}
	for _, raw := range [][]byte{nil, []byte("{}"), []byte("{}{}"), bytes.Repeat([]byte("x"), operations.MaxReceiptBytes+1)} {
		got, err := operationProcessReceipt(input, readOperationReceipt(bytes.NewReader(raw)), false, query.Stats{}, "", true)
		if err == nil || got.Outcome != operations.OutcomeUnknown || got.Effect != operations.EffectUnknown {
			t.Fatal("missing receipt became no-effect", got, err)
		}
	}
	read := operationReceiptRead{raw: []byte("{}"), err: io.ErrUnexpectedEOF}
	got, err := operationProcessReceipt(input, read, false, query.Stats{}, "", true)
	if err == nil || got.Outcome != operations.OutcomeUnknown {
		t.Fatal(got, err)
	}
}

func TestOperationReceiptKeepsCommitAfterArrowFailure(t *testing.T) {
	for _, kind := range []operations.Kind{operations.StatementExecute, operations.NativeExecute} {
		input := operationProcessInput(t, kind, "fixture")
		for _, withRef := range []bool{false, true} {
			receipt := operations.Receipt{Version: operations.Version, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256,
				Outcome: operations.Completed, Effect: operations.EffectCommitted}
			if withRef {
				receipt.Result = &operations.ResultRef{ID: input.OperationID, SHA256: strings.Repeat("a", 64), Bytes: 128, Rows: 1, Format: "arrow_ipc"}
			}
			got, err := operationProcessReceipt(input, operationReceiptBytes(t, receipt), true, query.Stats{}, "", false)
			if err == nil || got.Outcome != operations.Completed || got.Effect != operations.EffectCommitted || got.Result != nil {
				t.Fatal("confirmed commit became uncertain after result failure", got, err)
			}
		}
	}
}

func TestOperationReceiptBoundAndStrictJSON(t *testing.T) {
	for _, size := range []int{0, operations.MaxReceiptBytes, operations.MaxReceiptBytes + 1} {
		got := readOperationReceipt(strings.NewReader(strings.Repeat(" ", size)))
		if size <= operations.MaxReceiptBytes && (got.err != nil || len(got.raw) != size) {
			t.Fatal("receipt boundary changed", size)
		}
		if size > operations.MaxReceiptBytes && (got.err == nil || len(got.raw) != 0) {
			t.Fatal("oversized receipt retained")
		}
	}
	input := operationProcessInput(t, operations.ConnectionTest, "")
	r := operations.Receipt{Version: operations.Version, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256, Outcome: operations.Completed, Effect: operations.EffectNone}
	raw := operationReceiptBytes(t, r).raw
	for _, malformed := range [][]byte{append(append([]byte{}, raw...), raw...), append([]byte(`{"version":1,`), raw[1:]...)} {
		if _, err := operationProcessReceipt(input, operationReceiptRead{raw: malformed}, false, query.Stats{}, "", true); err == nil {
			t.Fatal("ambiguous receipt accepted")
		}
	}
}

func TestOperationExecutorRequiresContainment(t *testing.T) {
	input := operationProcessInput(t, operations.StatementExecute, "fixture")
	for _, executor := range []*Executor{nil, {Limits: query.DefaultLimits()}} {
		receipt, err := executor.ExecuteOperation(context.Background(), OperationProcessConfig{}, input, nil)
		if err == nil || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone {
			t.Fatal("uncontained operation launched", receipt, err)
		}
	}
}

type operationFinalizerTestSink struct {
	workerTestSink
	finalize func(operations.Receipt, error) (operations.Receipt, error)
}

func (s *operationFinalizerTestSink) FinalizeOperation(receipt operations.Receipt, err error) (operations.Receipt, error) {
	return s.finalize(receipt, err)
}

func TestOperationFinalizerAbortsBeforeAdmission(t *testing.T) {
	input := operationProcessInput(t, operations.QueryRead, "fixture")
	calls := 0
	sink := &operationFinalizerTestSink{finalize: func(r operations.Receipt, err error) (operations.Receipt, error) {
		calls++
		if err == nil || r.Outcome != operations.Rejected {
			t.Error("finalizer missed rejection", r, err)
		}
		return r, err
	}}
	var executor *Executor
	_, err := executor.ExecuteResolvedOperation(context.Background(), OperationProcessConfig{}, input.OperationID, input.RequestSHA256, func(context.Context) (adapter.ProcessRequest, error) {
		t.Error("unexpected resolution")
		return input, nil
	}, sink)
	if err == nil || calls != 1 {
		t.Fatal("pre-admission result was not finalized", err, calls)
	}
}
