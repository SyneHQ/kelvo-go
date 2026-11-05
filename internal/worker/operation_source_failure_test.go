package worker

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestConnectionTestSourceFailureIsLoggedByNode(t *testing.T) {
	input := operationProcessInput(t, operations.ConnectionTest, "fixture")
	r := operations.Receipt{Version: operations.Version, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256, Outcome: operations.Failed, Effect: operations.EffectNone, ErrorCode: "SOURCE_FAILED",
		SourceFailure: &operations.SourceFailure{Reason: "connect_refused", Host: "database.hdb-1.svc.cluster.local"}}
	got, err := operationProcessReceipt(input, operationReceiptBytes(t, r), false, query.Stats{}, "", true)
	if err != nil || got.SourceFailure == nil || got.ErrorCode != "SOURCE_FAILED" {
		t.Fatal(got, err)
	}
	var out bytes.Buffer
	log.SetOutput(&out)
	flags := log.Flags()
	log.SetFlags(0)
	defer func() { log.SetOutput(os.Stderr); log.SetFlags(flags) }()
	if got = logSourceFailure(got); got.SourceFailure != nil {
		t.Fatal("source failure left on published receipt")
	}
	if line := strings.TrimSpace(out.String()); line != "kelvo clickhouse source failed: reason=connect_refused http_status=- clickhouse_code=- host=database.hdb-1.svc.cluster.local" {
		t.Fatal(line)
	}

	// Strict validation: a malformed reason makes the whole receipt untrusted;
	// an absent field stays valid.
	r.SourceFailure = &operations.SourceFailure{Reason: "connect refused", Host: "h"}
	if got, err := operationProcessReceipt(input, operationReceiptBytes(t, r), false, query.Stats{}, "", true); err == nil || got.SourceFailure != nil {
		t.Fatal("invalid source failure accepted", got, err)
	}
	r.SourceFailure = nil
	if _, err := operationProcessReceipt(input, operationReceiptBytes(t, r), false, query.Stats{}, "", true); err != nil {
		t.Fatal(err)
	}
}
