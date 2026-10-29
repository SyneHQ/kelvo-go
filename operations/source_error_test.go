// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"strings"
	"testing"
)

func TestReceiptSourceErrorBounds(t *testing.T) {
	digest := strings.Repeat("a", 64)
	one := 1
	failed := func(code string, e *SourceError, steps int) Receipt {
		r := Receipt{Version: Version, OperationID: "op-1", RequestSHA256: digest, Outcome: Failed, Effect: EffectNone, ErrorCode: code, SourceError: e}
		for i := range steps {
			r.Steps = append(r.Steps, StepReceipt{Index: i, SHA256: digest, Effect: EffectNone})
		}
		return r
	}
	for _, tc := range []struct {
		name  string
		r     Receipt
		valid bool
	}{
		{"statement error", failed("SOURCE_FAILED", &SourceError{SQLState: "23502", Message: "null value in column \"name\"", Column: "name"}, 0), true},
		{"batch step", failed("SOURCE_FAILED", &SourceError{Message: "duplicate key", Statement: &one}, 2), true},
		{"step out of range", failed("SOURCE_FAILED", &SourceError{Message: "duplicate key", Statement: &one}, 1), false},
		{"other category", failed("UNSUPPORTED", &SourceError{Message: "x"}, 0), false},
		{"empty message", failed("SOURCE_FAILED", &SourceError{SQLState: "23502"}, 0), false},
		{"multi-line", failed("SOURCE_FAILED", &SourceError{Message: "a\nb"}, 0), false},
		{"bad sqlstate", failed("SOURCE_FAILED", &SourceError{SQLState: "2350", Message: "x"}, 0), false},
		{"oversized detail", failed("SOURCE_FAILED", &SourceError{Message: "x", Detail: strings.Repeat("d", MaxSourceErrorText+1)}, 0), false},
	} {
		if got := tc.r.Validate() == nil; got != tc.valid {
			t.Errorf("%s: valid = %v, want %v", tc.name, got, tc.valid)
		}
	}
}
