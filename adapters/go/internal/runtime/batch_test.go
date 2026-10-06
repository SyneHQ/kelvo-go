package runtime

import (
	"errors"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestBatchReceiptsPreserveEachSourceEffect(t *testing.T) {
	for _, test := range []struct {
		name, outcome        string
		transaction          bool
		attempted, completed int
		want                 []operations.Effect
	}{
		{"committed", "succeeded", true, 3, 3, []operations.Effect{operations.EffectCommitted, operations.EffectCommitted, operations.EffectCommitted}},
		{"rollback", "failed", true, 2, 0, []operations.Effect{operations.EffectNone, operations.EffectNone, operations.EffectNone}},
		{"commit uncertainty", "unknown", true, 3, 0, []operations.Effect{operations.EffectUnknown, operations.EffectUnknown, operations.EffectUnknown}},
		{"rollback uncertainty", "unknown", true, 2, 0, []operations.Effect{operations.EffectUnknown, operations.EffectUnknown, operations.EffectNone}},
		{"autocommit uncertainty", "unknown", false, 2, 1, []operations.Effect{operations.EffectCommitted, operations.EffectUnknown, operations.EffectNone}},
		{"cancelled between statements", "failed", false, 1, 1, []operations.Effect{operations.EffectCommitted, operations.EffectNone, operations.EffectNone}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := runtimeRequest(t, operations.StatementExecute)
			r.Request.Spec.Statement.SQL = ""
			r.Request.Spec.Statement.Batch = &operations.BatchSpec{Statements: []operations.BoundStatement{{SQL: "UPDATE a SET n=1"}, {SQL: "UPDATE b SET n=2"}, {SQL: "UPDATE c SET n=3"}}}
			if !test.transaction {
				r.Request.Spec.Statement.Transaction = operations.TransactionAutocommit
			}
			r.RequestSHA256, _ = operations.Digest(r.Request)
			s := &processSession{result: adapter.ChangeResult{Outcome: test.outcome, Attempted: test.attempted, Completed: test.completed}}
			if test.outcome != "succeeded" {
				s.errorResult = errors.New("source error")
			}
			receipt, _, err := runProcess(t, r, s)
			if test.outcome == "succeeded" && err != nil {
				t.Fatal(err)
			}
			if len(receipt.Steps) != 3 {
				t.Fatal("batch steps missing")
			}
			for i, step := range receipt.Steps {
				want, _ := operations.StatementDigest(r.Request.Spec.Statement.Batch.Statements[i])
				if step.Index != i || step.SHA256 != want || step.Effect != test.want[i] {
					t.Fatal("batch effects misreported", receipt)
				}
			}
		})
	}
}

func TestBatchRejectsImpossibleDriverCounts(t *testing.T) {
	batch := &operations.BatchSpec{Statements: []operations.BoundStatement{{SQL: "UPDATE a SET n=1"}}}
	for _, result := range []adapter.ChangeResult{{Outcome: "succeeded"}, {Outcome: "succeeded", Attempted: 1, Completed: 2}, {Outcome: "failed", Attempted: 1, Completed: 1}, {Outcome: "unknown", Attempted: 2}} {
		if _, err := changeSteps(batch, result, true); err == nil {
			t.Fatal("impossible source effect accepted")
		}
	}
}
