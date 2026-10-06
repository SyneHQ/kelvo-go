// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package ingestion

import (
	"encoding/json"
	"testing"
	"time"
)

func TestReceiptAndStateBinding(t *testing.T) {
	batch := fixtureBatch()
	hash, _ := batch.Digest()
	receipt := Receipt{BatchID: batch.ID, Digest: hash, Sequence: 1, Records: len(batch.Records), CommittedAt: time.Unix(1800000000, 0).UTC()}
	if receipt.ValidateBatch(batch) != nil {
		t.Fatal("valid receipt rejected")
	}
	changed := batch
	changed.ExpectedSequence++
	if receipt.ValidateBatch(changed) == nil {
		t.Fatal("receipt accepted for different batch sequence")
	}
	state := State{Sequence: 1, Checkpoint: json.RawMessage(`{"page":1}`), LastReceipt: &receipt}
	raw, _ := json.Marshal(state)
	if _, err := ParseState(raw); err != nil {
		t.Fatal(err)
	}
	state.Sequence++
	raw, _ = json.Marshal(state)
	if _, err := ParseState(raw); err == nil {
		t.Fatal("state sequence differs from receipt")
	}
}

func TestParseBatchPreservesPayloadAndRejectsUnknownFields(t *testing.T) {
	raw := []byte(`{"id":"batch-1","run_id":"run-1","expected_sequence":0,"observed_at":"2026-10-06T00:00:00Z","records":[{"id":"r","payload":{ "n": 12345678901234567890 },"deleted":false}],"checkpoint":{}}`)
	batch, err := ParseBatch(raw)
	if err != nil || string(batch.Records[0].Payload) != `{ "n": 12345678901234567890 }` {
		t.Fatal("exact payload lost", err)
	}
	for _, bad := range [][]byte{
		append(append([]byte{}, raw...), raw...),
		append([]byte(`{"unknown":1,`), raw[1:]...),
		append([]byte(`{"id":"different",`), raw[1:]...),
	} {
		if _, err := ParseBatch(bad); err == nil {
			t.Fatal("ambiguous batch accepted")
		}
	}
}
