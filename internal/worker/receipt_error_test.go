// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestReceiptErrorReturnsPostgresStatementText(t *testing.T) {
	failed := func(state string) operations.Receipt {
		return operations.Receipt{Outcome: operations.Failed, ErrorCode: "SOURCE_FAILED",
			SourceError: &operations.SourceError{SQLState: state, Message: "operator does not exist: text = uuid", Hint: "Add casts."}}
	}
	got := query.PublicError(receiptError("postgres", failed("42883"), nil))
	if got.Code != "INVALID_ARGUMENT" || got.Message != "PostgreSQL error 42883: operator does not exist: text = uuid Hint: Add casts." {
		t.Fatalf("got %+v", got)
	}
	for _, tc := range []struct{ engine, state string }{{"postgres", "42501"}, {"postgres", "53300"}, {"mysql", "42S22"}, {"duckdb", "42883"}} {
		if got := query.PublicError(receiptError(tc.engine, failed(tc.state), nil)); got.Code != "SOURCE_FAILED" || got.Message != "Source read failed" {
			t.Fatalf("%v: got %+v", tc, got)
		}
	}
}
