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
		{"deadline", fmt.Errorf("%s: %w", driverText, context.DeadlineExceeded), "DEADLINE_EXCEEDED"},
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

func TestSourceFailureCodeLeavesOtherCategoriesAlone(t *testing.T) {
	unsupported := query.NewError("UNSUPPORTED", "x")
	expired, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancel()
	for _, tc := range []struct {
		ctx  context.Context
		code string
		err  error
		want string
	}{
		{context.Background(), "OUTCOME_UNKNOWN", unsupported, "OUTCOME_UNKNOWN"},
		{context.Background(), "CANCELLED", unsupported, "CANCELLED"},
		{context.Background(), "SOURCE_FAILED", nil, "SOURCE_FAILED"},
		{context.Background(), "", nil, ""},
		{expired, "SOURCE_FAILED", errors.New("driver: i/o timeout"), "DEADLINE_EXCEEDED"},
	} {
		if got := sourceFailureCode(tc.ctx, tc.code, tc.err); got != tc.want {
			t.Fatalf("sourceFailureCode(%q, %v) = %q, want %q", tc.code, tc.err, got, tc.want)
		}
	}
}
