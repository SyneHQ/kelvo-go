package ingestion

import (
	"encoding/json"
	"strings"

	wire "github.com/SYNEHQ/kelvo-go/ingestion"
)

func fixtureBatch() wire.Batch {
	return wire.Batch{ID: "batch-1", RunID: "run-1", ObservedAt: "2026-09-22T00:00:00Z", Records: []wire.Record{{ID: "payment-1", Payload: json.RawMessage(`{"amount":"123456789012345678.10","currency":"INR"}`)}}, Checkpoint: json.RawMessage(`{"page":1}`)}
}
func fixtureScope() wire.Scope {
	return wire.Scope{TeamID: "team-a", SourceID: "source-a", ConnectionID: "connection-a", Database: "commerce_validation", Schema: "commerce_fixture", Stream: "payments", Binding: strings.Repeat("a", 64)}
}
