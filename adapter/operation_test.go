package adapter

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestStatementMappingPreservesRoleAndParameters(t *testing.T) {
	request := operations.Request{Version: operations.Version, Kind: operations.StatementExecute, Connection: operations.ConnectionRef{ID: "saved-connection"}, IdempotencyKey: "change-1", Spec: operations.Spec{Statement: &operations.StatementSpec{SQL: "UPDATE items SET amount=$1", Role: "reviewer", Transaction: operations.TransactionRequired, Parameters: []operations.Parameter{{Type: "int64", Value: json.RawMessage(`"9007199254740993"`)}}}}}
	invocation, err := FromOperation(request, Limits{})
	if err != nil || invocation.Change == nil || invocation.Change.Role != "reviewer" || !invocation.Change.Transaction || !reflect.DeepEqual(invocation.Change.Parameters[0], request.Spec.Statement.Parameters) {
		t.Fatalf("mapping: %+v %v", invocation, err)
	}
}

func TestSQLParameterExactness(t *testing.T) {
	parameters := []operations.Parameter{
		{Type: "int64", Value: json.RawMessage(`"9007199254740993"`)},
		{Type: "uint64", Value: json.RawMessage(`"18446744073709551615"`)},
		{Type: "decimal128", Value: json.RawMessage(`"12345.678901234567890"`)},
		{Type: "binary", Value: json.RawMessage(`"AP8="`)},
		{Type: "null", Value: json.RawMessage(`null`)},
	}
	values, err := SQLParameters(parameters)
	want := []any{int64(9007199254740993), "18446744073709551615", "12345.678901234567890", []byte{0, 255}, nil}
	if err != nil || !reflect.DeepEqual(values, want) {
		t.Fatalf("values=%#v err=%v", values, err)
	}
}

func TestBatchMappingPinsOrderParametersAndIsolation(t *testing.T) {
	request := operations.Request{Version: operations.Version, Kind: operations.StatementExecute, Connection: operations.ConnectionRef{ID: "saved-1"}, IdempotencyKey: "change-1", Spec: operations.Spec{Statement: &operations.StatementSpec{Transaction: operations.TransactionRequired, Role: "editor", Isolation: "serializable", Batch: &operations.BatchSpec{Statements: []operations.BoundStatement{{SQL: "UPDATE items SET n=$1", Parameters: []operations.Parameter{{Type: "int64", Value: json.RawMessage(`9007199254740993`)}}}, {SQL: "DELETE FROM items WHERE n=0"}}}}}}
	invocation, err := FromOperation(request, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	change := invocation.Change
	if change == nil || !change.Transaction || change.Role != "editor" || change.Isolation != "serializable" || len(change.Statements) != 2 || change.Statements[1] != "DELETE FROM items WHERE n=0" {
		t.Fatal("batch mapping changed")
	}
	request.Spec.Statement.Batch.Statements[0].Parameters[0].Value[0] = '1'
	if string(change.Parameters[0][0].Value) != `9007199254740993` {
		t.Fatal("caller mutated pinned parameters")
	}
}
