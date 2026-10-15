package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/internal/frame"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
)

func TestServeLoop_QueryFailureKeepsBoundedPublicCode(t *testing.T) {
	const driverText = "numeric(0,0) scale from driver pgx"
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"unsupported", fmt.Errorf("stream rows: %w", query.NewError("UNSUPPORTED", driverText)), "UNSUPPORTED"},
		{"adapter unsupported", adapter.ErrUnsupported, "UNSUPPORTED"},
		{"driver deadline", fmt.Errorf("%s: %w", driverText, context.DeadlineExceeded), "SOURCE_FAILED"},
		{"public deadline", query.NewError("DEADLINE_EXCEEDED", driverText), "SOURCE_FAILED"},
		{"resource", query.NewError("RESOURCE_EXHAUSTED", driverText), "RESOURCE_EXHAUSTED"},
		{"unlisted public code", query.NewError("PERMISSION_DENIED", driverText), "SOURCE_FAILED"},
		{"query failed", query.NewError("QUERY_FAILED", driverText), "SOURCE_FAILED"},
		{"driver error", errors.New(driverText), "SOURCE_FAILED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipeInR, pipeInW := io.Pipe()
			pipeOutR, pipeOutW := io.Pipe()
			runner := Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
				return &fakeQuerySession{err: tc.err}, nil
			}}
			done := make(chan error, 1)
			go func() { done <- runner.Serve(context.Background(), pipeInR, pipeOutW) }()

			req := validTestProcessRequest("op-code", "tenant-1", "team-1", "conn-1", strings.Repeat("a", 64), operations.QueryRead)
			reqBytes, _ := adapter.EncodeProcessRequest(req)
			_ = frame.WriteRequest(pipeInW, reqBytes)

			typ, payload, err := frame.Read(pipeOutR)
			if err != nil || typ != frame.Receipt {
				t.Fatalf("frame = %v, %v", typ, err)
			}
			if strings.Contains(string(payload), "pgx") || strings.Contains(string(payload), "numeric") {
				t.Fatalf("receipt leaked driver text: %s", payload)
			}
			var receipt operations.Receipt
			if err := json.Unmarshal(payload, &receipt); err != nil {
				t.Fatal(err)
			}
			if receipt.Outcome != operations.Failed || receipt.ErrorCode != tc.want {
				t.Fatalf("receipt = %s/%s, want failed/%s", receipt.Outcome, receipt.ErrorCode, tc.want)
			}
			if err := <-done; err == nil {
				t.Fatal("failed request must terminate the serve loop")
			}
		})
	}
}

type blockingQuerySession struct{ processSession }

func (s *blockingQuerySession) Query(ctx context.Context, _ adapter.Query, _ adapter.Sink) (adapter.QueryStats, error) {
	<-ctx.Done()
	return adapter.QueryStats{}, errors.New("driver: canceling statement due to user request")
}

func TestExecute_RequestDeadlineReportsDeadlineExceeded(t *testing.T) {
	input := runtimeRequest(t, operations.QueryRead)
	input.Limits.TimeoutMS = 50
	runner := Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
		return &blockingQuerySession{}, nil
	}}
	receipt, err := runner.execute(context.Background(), input, io.Discard)
	if err == nil || receipt.Outcome != operations.Failed || receipt.ErrorCode != "DEADLINE_EXCEEDED" {
		t.Fatalf("receipt = %s/%s, err = %v", receipt.Outcome, receipt.ErrorCode, err)
	}
}

func TestExecute_SourceEffectKeepsSourceFailed(t *testing.T) {
	unsupported := query.NewError("UNSUPPORTED", "driver text")
	for _, tc := range []struct {
		completed  int
		wantEffect operations.Effect
		wantCode   string
	}{
		{0, operations.EffectNone, "UNSUPPORTED"},
		{1, operations.EffectPartial, "SOURCE_FAILED"},
	} {
		session := &processSession{errorResult: unsupported, result: adapter.ChangeResult{Outcome: "failed", Completed: tc.completed}}
		runner := Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) { return session, nil }}
		receipt, err := runner.execute(context.Background(), runtimeRequest(t, operations.StatementExecute), io.Discard)
		if err == nil || receipt.Effect != tc.wantEffect || receipt.ErrorCode != tc.wantCode {
			t.Fatalf("completed=%d: receipt = %s/%s, want %s/%s", tc.completed, receipt.Effect, receipt.ErrorCode, tc.wantEffect, tc.wantCode)
		}
	}
}

func TestSourceFailureCodeLeavesOtherCategoriesAlone(t *testing.T) {
	unsupported := query.NewError("UNSUPPORTED", "x")
	expired, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancel()
	failed := func(code string, effect operations.Effect) operations.Receipt {
		return operations.Receipt{ErrorCode: code, Effect: effect}
	}
	for _, tc := range []struct {
		ctx     context.Context
		receipt operations.Receipt
		err     error
		want    string
	}{
		{context.Background(), failed("OUTCOME_UNKNOWN", operations.EffectUnknown), unsupported, "OUTCOME_UNKNOWN"},
		{context.Background(), failed("CANCELLED", operations.EffectNone), unsupported, "CANCELLED"},
		{context.Background(), failed("SOURCE_FAILED", operations.EffectNone), nil, "SOURCE_FAILED"},
		{context.Background(), failed("SOURCE_FAILED", operations.EffectPartial), unsupported, "SOURCE_FAILED"},
		{expired, failed("SOURCE_FAILED", operations.EffectPartial), errors.New("timeout"), "SOURCE_FAILED"},
		{expired, failed("SOURCE_FAILED", operations.EffectNone), errors.New("driver: i/o timeout"), "DEADLINE_EXCEEDED"},
	} {
		if got := sourceFailureCode(tc.ctx, tc.receipt, tc.err); got != tc.want {
			t.Fatalf("sourceFailureCode(%+v, %v) = %q, want %q", tc.receipt, tc.err, got, tc.want)
		}
	}
}
