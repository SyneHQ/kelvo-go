// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"go.opentelemetry.io/otel/trace"
)

// Carrier is optional diagnostic context from an authorized durable job, never
// an authority or an HTTP propagation header. It contains no baggage or state.
type Carrier struct {
	Version uint8  `json:"version"`
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
	Sampled bool   `json:"sampled"`
}

const maxCarrierBytes = 256

func (c Carrier) Valid() bool {
	return c.Version == 1 && validHexID(c.TraceID, 32) && validHexID(c.SpanID, 16)
}

func validHexID(value string, size int) bool {
	if len(value) != size {
		return false
	}
	nonzero := false
	for _, b := range []byte(value) {
		if !(b >= '0' && b <= '9') && !(b >= 'a' && b <= 'f') {
			return false
		}
		nonzero = nonzero || b != '0'
	}
	return nonzero
}

// Invalid optional telemetry is absent; it must not prevent a job from decoding.
// Bound parsing before allocating strings and reject duplicates and extra fields.
func (c *Carrier) UnmarshalJSON(data []byte) error {
	*c = Carrier{}
	if len(data) > maxCarrierBytes {
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(data))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return nil
	}
	var decoded Carrier
	var seen uint8
	for d.More() {
		field, err := d.Token()
		if err != nil {
			return nil
		}
		var bit uint8
		switch field {
		case "version":
			bit = 1
			err = d.Decode(&decoded.Version)
		case "trace_id":
			bit = 2
			err = d.Decode(&decoded.TraceID)
		case "span_id":
			bit = 4
			err = d.Decode(&decoded.SpanID)
		case "sampled":
			bit = 8
			var sampled *bool
			err = d.Decode(&sampled)
			if sampled == nil {
				return nil
			}
			decoded.Sampled = *sampled
		default:
			return nil
		}
		if err != nil || seen&bit != 0 {
			return nil
		}
		seen |= bit
	}
	last, err := d.Token()
	if err != nil || last != json.Delim('}') || seen != 15 || !decoded.Valid() {
		return nil
	}
	if _, err := d.Token(); err != io.EOF {
		return nil
	}
	*c = decoded
	return nil
}

func (c Carrier) MarshalJSON() ([]byte, error) {
	if !c.Valid() {
		return []byte("null"), nil
	}
	type wire Carrier
	return json.Marshal(wire(c))
}

type carrierContextKey struct{}

// WithCarrier replaces only Kelvo's private diagnostic context. It deliberately
// ignores any ambient OpenTelemetry context and preserves cancellation/authority.
func WithCarrier(ctx context.Context, carrier Carrier) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if !carrier.Valid() {
		if !CarrierFromContext(ctx).Valid() {
			return ctx
		}
		carrier = Carrier{}
	}
	return context.WithValue(ctx, carrierContextKey{}, carrier)
}

func CarrierFromContext(ctx context.Context) Carrier {
	if ctx == nil {
		return Carrier{}
	}
	carrier, _ := ctx.Value(carrierContextKey{}).(Carrier)
	if !carrier.Valid() {
		return Carrier{}
	}
	return carrier
}

func carrierContext(carrier Carrier) context.Context {
	ctx := context.Background()
	if !carrier.Valid() {
		return ctx
	}
	traceID, _ := trace.TraceIDFromHex(carrier.TraceID)
	spanID, _ := trace.SpanIDFromHex(carrier.SpanID)
	var flags trace.TraceFlags
	if carrier.Sampled {
		flags = trace.FlagsSampled
	}
	return trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: flags, Remote: true,
	}))
}

func spanCarrier(sc trace.SpanContext) Carrier {
	if !sc.IsValid() {
		return Carrier{}
	}
	return Carrier{Version: 1, TraceID: sc.TraceID().String(), SpanID: sc.SpanID().String(), Sampled: sc.IsSampled()}
}

func cleanSpanContext(sc trace.SpanContext) trace.SpanContext {
	if !sc.IsValid() {
		return trace.SpanContext{}
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: sc.TraceID(), SpanID: sc.SpanID(), TraceFlags: sc.TraceFlags() & trace.FlagsSampled, Remote: sc.IsRemote(),
	})
}
