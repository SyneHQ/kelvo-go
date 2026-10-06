// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"strings"
	"testing"
)

func TestStatementReceiptBindsEveryStep(t *testing.T) {
	request := Request{Version: Version, Kind: StatementExecute, Connection: ConnectionRef{ID: "saved-connection"}, IdempotencyKey: "batch-test", Spec: Spec{Statement: &StatementSpec{Transaction: TransactionRequired}}}
	request.Spec.Statement.SQL = ""
	request.Spec.Statement.Batch = &BatchSpec{Statements: []BoundStatement{{SQL: "UPDATE first"}, {SQL: "UPDATE second"}, {SQL: "UPDATE third"}}}
	for _, test := range []struct {
		name        string
		transaction TransactionMode
		outcome     Outcome
		effect      Effect
		steps       []Effect
		valid       bool
	}{
		{"committed", TransactionRequired, Completed, EffectCommitted, []Effect{EffectCommitted, EffectCommitted, EffectCommitted}, true},
		{"rollback", TransactionRequired, Failed, EffectNone, []Effect{EffectNone, EffectNone, EffectNone}, true},
		{"partial", TransactionAutocommit, Failed, EffectPartial, []Effect{EffectCommitted, EffectNone, EffectNone}, true},
		{"unknown_autocommit", TransactionAutocommit, OutcomeUnknown, EffectUnknown, []Effect{EffectCommitted, EffectUnknown, EffectNone}, true},
		{"unknown_transaction", TransactionRequired, OutcomeUnknown, EffectUnknown, []Effect{EffectUnknown, EffectUnknown, EffectNone}, true},
		{"commit_uncertainty", TransactionRequired, OutcomeUnknown, EffectUnknown, []Effect{EffectUnknown, EffectUnknown, EffectUnknown}, true},
		{"missing_step", TransactionRequired, Completed, EffectCommitted, []Effect{EffectCommitted}, false},
		{"incomplete_success", TransactionRequired, Completed, EffectCommitted, []Effect{EffectCommitted, EffectNone, EffectNone}, false},
		{"transaction_partial", TransactionRequired, Failed, EffectPartial, []Effect{EffectCommitted, EffectNone, EffectNone}, false},
		{"disordered_commit", TransactionAutocommit, Failed, EffectPartial, []Effect{EffectNone, EffectCommitted, EffectNone}, false},
		{"hidden_partial", TransactionAutocommit, Failed, EffectNone, []Effect{EffectCommitted, EffectNone, EffectNone}, false},
		{"multiple_uncertain_autocommit", TransactionAutocommit, OutcomeUnknown, EffectUnknown, []Effect{EffectUnknown, EffectUnknown, EffectNone}, false},
		{"unknown_without_attempt", TransactionRequired, OutcomeUnknown, EffectUnknown, []Effect{EffectNone, EffectNone, EffectNone}, false},
		{"lost_progress_evidence", TransactionAutocommit, OutcomeUnknown, EffectUnknown, nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request.Spec.Statement.Transaction = test.transaction
			digest, err := Digest(request)
			if err != nil {
				t.Fatal(err)
			}
			r := Receipt{Version: Version, OperationID: "operation-test", RequestSHA256: digest, Outcome: test.outcome, Effect: test.effect}
			switch test.outcome {
			case Failed:
				r.ErrorCode = "SOURCE_FAILED"
			case OutcomeUnknown:
				r.ErrorCode = "OUTCOME_UNKNOWN"
			}
			for i, effect := range test.steps {
				digest, err := StatementDigest(request.Spec.Statement.Batch.Statements[i])
				if err != nil {
					t.Fatal(err)
				}
				r.Steps = append(r.Steps, StepReceipt{Index: i, SHA256: digest, Effect: effect})
			}
			if got := ValidateStatementReceipt(request, r) == nil; got != test.valid {
				t.Fatal("incorrect step evidence accepted", got)
			}
			if test.valid && len(r.Steps) > 0 {
				r.Steps[0].SHA256 = strings.Repeat("a", 64)
				if ValidateStatementReceipt(request, r) == nil {
					t.Fatal("foreign statement receipt accepted")
				}
			}
		})
	}
}
