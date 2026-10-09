package runtime

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"io"
	"testing"
)

func TestPrivateRuntimeRejectsOrdinaryOpenOverride(t *testing.T) {
	request := runtimeRequest(t, operations.QueryRead)
	request.PrivateTransport = &adapter.PrivateTransport{ControlFD: 7}
	calls := 0
	runner := Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
		calls++
		return nil, nil
	}}
	receipt, err := runner.execute(context.Background(), request, io.Discard)
	if err == nil || calls != 0 || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone {
		t.Fatal("private runtime used ordinary override", receipt, err, calls)
	}
}
