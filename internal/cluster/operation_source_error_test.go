package cluster

import (
	"net/http/httptest"
	"testing"

	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestOperationResponseWithholdsSourceErrorFromOlderClients(t *testing.T) {
	source := &operations.SourceError{SQLState: "23502", Message: "null value in column \"name\""}
	record := operationstore.Record{ID: "op-1", Receipt: &operations.Receipt{ErrorCode: "SOURCE_FAILED", SourceError: source}}
	older := httptest.NewRequest("GET", "/v1/operations/op-1", nil)
	if got := operationResponse(older, record); got.Receipt == nil || got.Receipt.SourceError != nil || got.Receipt.ErrorCode != "SOURCE_FAILED" {
		t.Fatalf("older client got %+v", got.Receipt)
	}
	if record.Receipt.SourceError != source {
		t.Fatal("withholding changed the stored receipt")
	}
	current := httptest.NewRequest("GET", "/v1/operations/op-1", nil)
	current.Header.Set(SourceErrorsHeader, "1")
	if got := operationResponse(current, record); got.Receipt == nil || got.Receipt.SourceError != source {
		t.Fatalf("opted-in client got %+v", got.Receipt)
	}
}
