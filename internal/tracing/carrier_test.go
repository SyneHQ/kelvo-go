// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package tracing

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

func testCarrier(sampled bool) Carrier {
	return Carrier{Version: 1, TraceID: "1234567890abcdef1234567890abcdef", SpanID: "1234567890abcdef", Sampled: sampled}
}

func TestCarrierValidationAndCanonicalJSON(t *testing.T) {
	for _, sampled := range []bool{false, true} {
		want := testCarrier(sampled)
		encoded, err := json.Marshal(want)
		if err != nil || len(encoded) > maxCarrierBytes || !want.Valid() {
			t.Fatal("valid carrier failed bounded encoding")
		}
		var got Carrier
		if err := json.Unmarshal(encoded, &got); err != nil || got != want {
			t.Fatalf("carrier did not round trip: %v", err)
		}
	}
	for _, change := range []func(*Carrier){
		func(c *Carrier) { c.Version = 0 },
		func(c *Carrier) { c.Version = 2 },
		func(c *Carrier) { c.TraceID = strings.ToUpper(c.TraceID) },
		func(c *Carrier) { c.TraceID = strings.Repeat("0", 32) },
		func(c *Carrier) { c.TraceID = "g" + c.TraceID[1:] },
		func(c *Carrier) { c.TraceID += "0" },
		func(c *Carrier) { c.SpanID = strings.Repeat("0", 16) },
		func(c *Carrier) { c.SpanID = c.SpanID[:15] },
		func(c *Carrier) { c.SpanID = strings.ToUpper(c.SpanID) },
	} {
		invalid := testCarrier(true)
		change(&invalid)
		encoded, err := json.Marshal(invalid)
		if invalid.Valid() || err != nil || string(encoded) != "null" {
			t.Fatal("invalid carrier was retained or caused an encoding failure")
		}
	}
}

func TestMalformedOptionalCarrierDoesNotInvalidateEnvelope(t *testing.T) {
	encoded, _ := json.Marshal(testCarrier(true))
	valid := string(encoded)
	malformed := []string{
		`null`, `false`, `7`, `"private-context"`, `[]`, `{}`,
		strings.Replace(valid, `"version":1`, `"version":null`, 1),
		strings.Replace(valid, `"version":1`, `"version":"1"`, 1),
		strings.Replace(valid, `"version":1`, `"version":1.0`, 1),
		strings.Replace(valid, `"version":1`, `"version":256`, 1),
		strings.Replace(valid, `"version":1`, `"version":2`, 1),
		strings.Replace(valid, `"sampled":true`, `"sampled":"true"`, 1),
		strings.Replace(valid, `"sampled":true`, `"sampled":null`, 1),
		strings.Replace(valid, `,"sampled":true`, ``, 1),
		strings.Replace(valid, `"trace_id":"1234567890abcdef1234567890abcdef"`, `"trace_id":{}`, 1),
		strings.TrimSuffix(valid, "}") + `,"sampled":false}`,
		strings.TrimSuffix(valid, "}") + `,"version":1}`,
		strings.TrimSuffix(valid, "}") + `,"tracestate":"private"}`,
		strings.TrimSuffix(valid, "}") + `,"baggage":{"tenant":"private"}}`,
		strings.Replace(valid, "1234567890abcdef1234567890abcdef", strings.Repeat("f", 300), 1),
	}
	for i, raw := range malformed {
		var envelope struct {
			ID    string  `json:"id"`
			Trace Carrier `json:"trace"`
		}
		envelope.Trace = testCarrier(true)
		if err := json.Unmarshal([]byte(`{"id":"job","trace":`+raw+`}`), &envelope); err != nil {
			t.Fatalf("case %d rejected optional telemetry: %v", i, err)
		}
		if envelope.ID != "job" || envelope.Trace != (Carrier{}) {
			t.Fatalf("case %d retained malformed telemetry or changed the envelope", i)
		}
	}
	var old struct {
		ID    string   `json:"id"`
		Trace *Carrier `json:"trace,omitempty"`
	}
	if err := json.Unmarshal([]byte(`{"id":"job"}`), &old); err != nil || old.Trace != nil {
		t.Fatal("legacy envelope acquired a carrier")
	}
	reencoded, err := json.Marshal(old)
	if err != nil || string(reencoded) != `{"id":"job"}` {
		t.Fatal("absent optional field changed legacy serialization")
	}
}

func TestPrivateCarrierContextIgnoresAmbientPropagation(t *testing.T) {
	carrier := testCarrier(true)
	state, _ := trace.ParseTraceState("vendor=private")
	ambient := trace.SpanContextFromContext(carrierContext(carrier)).WithTraceState(state)
	member, _ := baggage.NewMember("tenant", "private")
	bag, _ := baggage.New(member)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = baggage.ContextWithBaggage(trace.ContextWithRemoteSpanContext(ctx, ambient), bag)
	if CarrierFromContext(ctx) != (Carrier{}) || CarrierFromContext(nil) != (Carrier{}) {
		t.Fatal("ambient propagation became an internal carrier")
	}
	bound := WithCarrier(ctx, carrier)
	carrier.TraceID = strings.Repeat("f", 32)
	if CarrierFromContext(bound) != testCarrier(true) {
		t.Fatal("carrier context retained a mutable caller reference")
	}
	cancel()
	if bound.Err() != context.Canceled {
		t.Fatal("carrier context lost cancellation")
	}
	cleared := WithCarrier(bound, Carrier{})
	if CarrierFromContext(cleared) != (Carrier{}) {
		t.Fatal("invalid carrier inherited previous correlation")
	}
	background := context.Background()
	if WithCarrier(background, Carrier{}) != background {
		t.Fatal("absent carrier allocated context state")
	}
	exported := carrierContext(CarrierFromContext(bound))
	if baggage.FromContext(exported).Len() != 0 || trace.SpanContextFromContext(exported).TraceState().Len() != 0 {
		t.Fatal("private propagation retained baggage or tracestate")
	}
	if trace.SpanContextFromContext(carrierContext(Carrier{TraceID: "private"})).IsValid() {
		t.Fatal("invalid carrier became a parent")
	}
}

func FuzzOptionalCarrierDecode(f *testing.F) {
	f.Add([]byte(`null`))
	seed, _ := json.Marshal(testCarrier(true))
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		carrier := testCarrier(true)
		if err := carrier.UnmarshalJSON(data); err != nil {
			t.Fatal("optional decoder returned an error")
		}
		if carrier != (Carrier{}) && !carrier.Valid() {
			t.Fatal("decoder retained invalid state")
		}
		encoded, err := json.Marshal(carrier)
		if err != nil || len(encoded) > maxCarrierBytes {
			t.Fatal("decoded carrier could not be encoded within its bound")
		}
	})
}
