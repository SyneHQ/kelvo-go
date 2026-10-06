package adapter

import (
	"encoding/json"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestFromOperationPreservesNativeKindAndOwnsParameters(t *testing.T) {
	for _, kind := range []operations.Kind{operations.NativeRead, operations.NativeExecute} {
		request := operations.Request{Version: operations.Version, Kind: kind, Connection: operations.ConnectionRef{ID: "saved"}, Spec: operations.Spec{Native: &operations.NativeSpec{Provider: "mongodb", Command: "find", Parameters: []operations.Parameter{{Type: "json", Value: json.RawMessage(`{"collection":"orders"}`)}}}}}
		if kind.Mutating() {
			request.IdempotencyKey = "mutation"
		}
		invocation, err := FromOperation(request, Limits{MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 10})
		if err != nil || invocation.Native == nil || invocation.Native.Kind != kind {
			t.Fatal(invocation, err)
		}
		request.Spec.Native.Parameters[0].Value[0] = '['
		if invocation.Native.Spec.Parameters[0].Value[0] != '{' {
			t.Fatal("native input aliases caller")
		}
	}
}
