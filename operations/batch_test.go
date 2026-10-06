package operations

import (
	"encoding/json"
	"strings"
	"testing"
)

func batchRequest() Request {
	return Request{
		Version: Version, Kind: StatementExecute,
		Connection: ConnectionRef{ID: "saved-1"}, IdempotencyKey: "change-1",
		Spec: Spec{Statement: &StatementSpec{
			Transaction: TransactionRequired, Isolation: "serializable", Role: "editor",
			Batch: &BatchSpec{Statements: []BoundStatement{
				{SQL: "UPDATE items SET n=$1", Parameters: []Parameter{{Type: "int64", Value: json.RawMessage(`9007199254740993`)}}},
				{SQL: "DELETE FROM items WHERE n=0"},
			}},
		}},
	}
}

func TestBatchDigestBindsOrderParametersAndTransactionSettings(t *testing.T) {
	r := batchRequest()
	original, err := Digest(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Request){
		func(r *Request) {
			r.Spec.Statement.Batch.Statements[0], r.Spec.Statement.Batch.Statements[1] = r.Spec.Statement.Batch.Statements[1], r.Spec.Statement.Batch.Statements[0]
		},
		func(r *Request) {
			r.Spec.Statement.Batch.Statements[0].Parameters[0].Value = json.RawMessage(`9007199254740994`)
		},
		func(r *Request) { r.Spec.Statement.Isolation = "read_committed" },
		func(r *Request) { r.Spec.Statement.Role = "other" },
	} {
		r := batchRequest()
		mutate(&r)
		changed, err := Digest(r)
		if err != nil || changed == original {
			t.Fatal("batch mutation not bound", err)
		}
	}
	step, err := StatementDigest(r.Spec.Statement.Batch.Statements[0])
	if err != nil {
		t.Fatal(err)
	}
	r.Spec.Statement.Batch.Statements[0].Parameters[0].Value = json.RawMessage(`9007199254740994`)
	changed, _ := StatementDigest(r.Spec.Statement.Batch.Statements[0])
	if step == changed {
		t.Fatal("step parameter not bound")
	}
}

func TestBatchRejectsAmbiguousShapeAndUnboundedInputs(t *testing.T) {
	for _, mutate := range []func(*Request){
		func(r *Request) { r.Spec.Statement.SQL = "SELECT 1" }, func(r *Request) {
			r.Spec.Statement.Parameters = []Parameter{{Type: "null", Value: json.RawMessage(`null`)}}
		},
		func(r *Request) { r.Spec.Statement.Batch.Statements = nil }, func(r *Request) { r.Spec.Statement.Batch.Statements = make([]BoundStatement, 101) },
		func(r *Request) { r.Spec.Statement.Isolation = "snapshot" }, func(r *Request) { r.Spec.Statement.Transaction = TransactionAutocommit },
		func(r *Request) { r.Spec.Statement.Batch.Statements[0].SQL = "" },
		func(r *Request) {
			r.Spec.Statement.Batch.Statements = []BoundStatement{{SQL: strings.Repeat("x", 90000)}, {SQL: strings.Repeat("x", 90000)}, {SQL: strings.Repeat("x", 90000)}}
		},
	} {
		r := batchRequest()
		mutate(&r)
		if r.Validate() == nil {
			t.Fatal("invalid batch accepted")
		}
	}
}

func TestBatchCapabilitiesCheckAllStatementsAndIsolation(t *testing.T) {
	r := batchRequest()
	c := Capabilities{Version: Version, Engine: "postgresql", Operations: []Capability{{Kind: StatementExecute, Transactions: []TransactionMode{TransactionRequired}, IsolationLevels: []string{"serializable"}, Roles: true, ParameterTypes: []string{"int64"}, Idempotency: "none", Cancellation: "best_effort"}}}
	if err := c.Supports(r); err != nil {
		t.Fatal(err)
	}
	c.Operations[0].IsolationLevels = nil
	if c.Supports(r) == nil {
		t.Fatal("unadvertised isolation accepted")
	}
	r.Spec.Statement.Isolation = ""
	r.Spec.Statement.Batch.Statements[1].Parameters = []Parameter{{Type: "binary", Value: json.RawMessage(`"AA=="`)}}
	if c.Supports(r) == nil {
		t.Fatal("later unsupported parameter accepted")
	}
	c.Operations[0].IsolationLevels = []string{"serializable", "serializable"}
	if c.Validate() == nil {
		t.Fatal("duplicate isolation accepted")
	}
}

func TestSingleStatementWireDoesNotGainBatchFields(t *testing.T) {
	r := StatementSpec{SQL: "UPDATE items SET n=1", Transaction: TransactionAutocommit}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"sql":"UPDATE items SET n=1","transaction":"autocommit"}` {
		t.Fatalf("single wire changed: %s", raw)
	}
}
