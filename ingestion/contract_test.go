package ingestion

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func fixtureBatch() Batch {
	return Batch{ID: "batch-1", RunID: "run-1", ObservedAt: "2026-09-22T00:00:00Z", Records: []Record{{ID: "payment-1", Payload: json.RawMessage(`{"amount":"123456789012345678.10","currency":"INR"}`)}}, Checkpoint: json.RawMessage(`{"page":1}`)}
}
func fixtureScope() Scope {
	return Scope{TeamID: "team-a", SourceID: "source-a", ConnectionID: "connection-a", Database: "commerce_validation", Schema: "commerce_fixture", Stream: "payments", Binding: strings.Repeat("a", 64)}
}

func TestBatchValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Batch)
	}{
		{"duplicate-id", func(b *Batch) { b.Records = append(b.Records, b.Records[0]) }},
		{"duplicate-key", func(b *Batch) { b.Records[0].Payload = json.RawMessage(`{"id":1,"id":2}`) }},
		{"nested-duplicate", func(b *Batch) { b.Records[0].Payload = json.RawMessage(`{"a":[{"x":1,"x":2}]}`) }},
		{"array-record", func(b *Batch) { b.Records[0].Payload = json.RawMessage(`[]`) }},
		{"invalid-json", func(b *Batch) { b.Records[0].Payload = json.RawMessage(`{"x":NaN}`) }},
		{"nul-value", func(b *Batch) { b.Records[0].Payload = json.RawMessage(`{"x":"\u0000"}`) }},
		{"deep-json", func(b *Batch) {
			b.Records[0].Payload = json.RawMessage(`{"x":` + strings.Repeat("[", 34) + `0` + strings.Repeat("]", 34) + `}`)
		}},
		{"huge-record", func(b *Batch) {
			b.Records[0].Payload = json.RawMessage(`{"x":"` + strings.Repeat("a", MaxRecordBytes) + `"}`)
		}},
		{"huge-checkpoint", func(b *Batch) {
			b.Checkpoint = json.RawMessage(`{"x":"` + strings.Repeat("a", MaxCheckpointBytes) + `"}`)
		}},
		{"timestamp", func(b *Batch) { b.ObservedAt = "yesterday" }},
		{"negative-sequence", func(b *Batch) { b.ExpectedSequence = -1 }},
		{"too-many-rows", func(b *Batch) { b.Records = make([]Record, MaxRecords+1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := fixtureBatch()
			test.change(&b)
			if _, err := b.Digest(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected invalid: %v", err)
			}
		})
	}
	good := fixtureBatch()
	if _, err := good.Digest(); err != nil {
		t.Fatal(err)
	}
	good.Records = nil
	if _, err := good.Digest(); err != nil {
		t.Fatal("empty terminal page must advance checkpoint", err)
	}
}

func TestScopeValidationAndIsolation(t *testing.T) {
	base := fixtureScope()
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, schema := range []string{`x"; DROP SCHEMA public`, "pg_catalog", "information_schema", "a.b", "", strings.Repeat("a", 64)} {
		s := base
		s.Schema = schema
		if s.Validate() == nil {
			t.Fatal("accepted unsafe schema", schema)
		}
	}
	scopes := []Scope{base, base, base, base, base, base}
	scopes[0].TeamID = "team-b"
	scopes[1].SourceID = "source-b"
	scopes[2].ConnectionID = "connection-b"
	scopes[3].Database = "other"
	scopes[4].Stream = "refunds"
	scopes[5].Binding = strings.Repeat("b", 64)
	for _, s := range scopes {
		if s.Key() == base.Key() {
			t.Fatal("scope collision")
		}
	}
}
