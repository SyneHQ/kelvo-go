// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package watch

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func testScope() Scope {
	return Scope{TeamID: "team", ConnectionID: "saved", Database: "app", Schema: "public", Table: "orders", ID: "watcher", Generation: "generation-1"}
}

func TestResumeCheckpointBindsOpaqueEventsAndEmptyProgress(t *testing.T) {
	scope := testScope()
	from := base64.StdEncoding.EncodeToString([]byte("opaque-from"))
	to := base64.StdEncoding.EncodeToString([]byte("opaque-to"))
	empty, err := NewResumeBatch(scope, nil, from, to)
	if err != nil || empty.Checkpoint == nil || len(empty.Checkpoint.Entries) != 0 {
		t.Fatal("empty stream progress was lost", err)
	}
	raw, _ := json.Marshal(empty)
	if _, err := ParseBatch(raw, scope); err != nil {
		t.Fatal(err)
	}
	wrong := scope
	wrong.Generation = "other"
	if empty.Validate(wrong) == nil {
		t.Fatal("cross-generation resume accepted")
	}
	event := testEvent()
	event.ID = "mongo:" + strings.Repeat("a", 64)
	event.Context = json.RawMessage(`{"operationType":"insert","ns":{"db":"app","coll":"orders"}}`)
	event.DataKind = "document"
	batch, err := NewResumeBatch(scope, []Event{event}, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewBatch(scope, []Event{event}); err == nil {
		t.Fatal("opaque event lacks a resume transition")
	}
	if _, err := NewResumeBatch(scope, []Event{testEvent()}, from, to); err == nil {
		t.Fatal("SQL checkpoint accepted a provider resume token")
	}
	batch.Events[0].Context = json.RawMessage(`{"operationType":"delete"}`)
	if batch.Validate(scope) == nil {
		t.Fatal("changed filter context did not invalidate checkpoint")
	}
	for _, bad := range []*Resume{{From: from, To: from}, {From: "!", To: to}, {From: from + "\n", To: to}, {From: from, To: base64.StdEncoding.EncodeToString(make([]byte, MaxResumeTokenBytes+1))}} {
		if bad.Validate() == nil {
			t.Fatal("invalid/unbounded resume accepted")
		}
	}
}
func testEvent() Event {
	return Event{ID: "1", Operation: "INSERT", Data: json.RawMessage(`{"amount":9007199254740993,"id":1}`), OldData: json.RawMessage(`null`), Timestamp: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
}

func TestCheckpointBindsScopeGenerationAndExactEvents(t *testing.T) {
	scope := testScope()
	batch, err := NewBatch(scope, []Event{testEvent()})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(batch)
	if _, err := ParseBatch(raw, scope); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Scope){"team": func(s *Scope) { s.TeamID = "other" }, "database": func(s *Scope) { s.Database = "other" }, "schema": func(s *Scope) { s.Schema = "other" }, "connection": func(s *Scope) { s.ConnectionID = "other" }, "watcher": func(s *Scope) { s.ID = "other" }, "table": func(s *Scope) { s.Table = "other" }, "generation": func(s *Scope) { s.Generation = "other" }} {
		t.Run(name, func(t *testing.T) {
			changed := scope
			change(&changed)
			if _, err := ParseBatch(raw, changed); err == nil {
				t.Fatal("cross-scope batch accepted")
			}
		})
	}
	batch.Events[0].Data = json.RawMessage(`{"amount":9007199254740992,"id":1}`)
	if batch.Validate(scope) == nil {
		t.Fatal("changed event accepted")
	}
}

func TestEventDigestKeepsNumericPrecisionAndCanonicalObjects(t *testing.T) {
	first := testEvent()
	digest, err := first.Digest()
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.Data = json.RawMessage(`{ "id":1, "amount":9007199254740993 }`)
	got, err := second.Digest()
	if err != nil || got != digest {
		t.Fatal("object order changed identity", err)
	}
	second.Data = json.RawMessage(`{"id":1,"amount":9007199254740992}`)
	got, err = second.Digest()
	if err != nil || got == digest {
		t.Fatal("integer precision lost", err)
	}
	second.Data = json.RawMessage(`{"id":1,"id":2}`)
	if second.Validate() == nil {
		t.Fatal("duplicate object keys accepted")
	}
}

func TestMalformedAcknowledgementsAreRejected(t *testing.T) {
	scope := testScope()
	batch, err := NewBatch(scope, []Event{testEvent()})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(batch.Checkpoint)
	for _, input := range []string{string(raw) + ` {}`, strings.Replace(string(raw), `"version":1`, `"version":1,"Version":1`, 1), strings.Replace(string(raw), `"id":"1"`, `"id":"01"`, 1), strings.Replace(string(raw), `"entries":`, `"dsn":"forbidden","entries":`, 1), `null`} {
		if _, err := ParseCheckpoint([]byte(input), scope); err == nil {
			t.Fatal("malformed checkpoint accepted")
		}
	}
	batch.Checkpoint.Entries = append(batch.Checkpoint.Entries, batch.Checkpoint.Entries[0])
	if batch.Checkpoint.Validate(scope) == nil {
		t.Fatal("duplicate ack id accepted")
	}
	empty, err := NewBatch(scope, nil)
	if err != nil || empty.Checkpoint != nil {
		t.Fatal("empty batch has ack", err)
	}
}
