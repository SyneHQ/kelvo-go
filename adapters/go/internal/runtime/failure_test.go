package runtime

import (
	"context"
	"crypto/tls"
	"net"
	"testing"

	native "github.com/SYNEHQ/kelvo-go/internal/sources/clickhouse"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
)

type discardSink struct{ query.Sink }

func TestFailureCodeCarriesClickHouseSourceFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	engine, err := native.NewResolved(native.ResolvedSource{ID: "analytics", URL: "https://" + addr + "?database=reports", Username: "reader", Password: "test-only-password", TLS: &tls.Config{MinVersion: tls.VersionTLS12}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	_, execErr := engine.Execute(context.Background(), query.Request{SQL: "SELECT 1", Mode: "native", ConnectionID: "analytics"}, discardSink{})
	receipt := withFailureCode(context.Background(), operations.Receipt{Version: operations.Version, Outcome: operations.Failed, Effect: operations.EffectNone, ErrorCode: "SOURCE_FAILED"}, execErr)
	if receipt.ErrorCode != "SOURCE_FAILED" || receipt.SourceFailure == nil || *receipt.SourceFailure != (operations.SourceFailure{Reason: "connect_refused", Host: "127.0.0.1"}) {
		t.Fatalf("receipt %+v err %v", receipt.SourceFailure, execErr)
	}
	if plain := withFailureCode(context.Background(), operations.Receipt{ErrorCode: "SOURCE_FAILED"}, context.Canceled); plain.SourceFailure != nil {
		t.Fatal("non-ClickHouse error produced a source failure")
	}
}
