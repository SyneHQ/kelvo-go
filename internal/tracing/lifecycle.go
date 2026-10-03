// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package tracing

import (
	"sync"

	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Operation uint8

const (
	ClusterSubmit Operation = iota
	ClusterDispatch
	ClusterResultWait
	ClusterRelay
	operationCount
)

var operationNames = [...]string{"kelvo.cluster.submit", "kelvo.cluster.dispatch", "kelvo.cluster.result_wait", "kelvo.cluster.relay"}

// Lifecycle measures a local interval. A parent relates spans; it does not make
// timestamps on different hosts a reliable elapsed-time measurement.
type Lifecycle struct {
	span    trace.Span
	carrier Carrier
	end     sync.Once
}

// Start must be called only after the operation's authorization check. Missing
// or invalid optional context starts an unrelated local trace. Disabled tracing
// returns before reading clocks, generating IDs or retaining any state.
func (r *Recorder) Start(parent Carrier, operation Operation) *Lifecycle {
	if r == nil || r.stopped.Load() || operation >= operationCount {
		return nil
	}
	_, span := r.tracer.Start(carrierContext(parent), operationNames[operation], trace.WithAttributes(attribute.String("kelvo.kind", "query")))
	return &Lifecycle{span: span, carrier: spanCarrier(span.SpanContext())}
}

func (l *Lifecycle) Carrier() Carrier {
	if l == nil {
		return Carrier{}
	}
	return l.carrier
}

// End records one terminal outcome. An invalid internal enum becomes a fixed
// error outcome, never an unbounded status or an indefinitely retained span.
func (l *Lifecycle) End(outcome telemetry.Outcome) {
	if l == nil || l.span == nil {
		return
	}
	l.end.Do(func() {
		name := "error"
		switch outcome {
		case telemetry.OutcomeSuccess:
			name = "success"
		case telemetry.OutcomeCanceled:
			name = "canceled"
		}
		l.span.SetAttributes(attribute.String("kelvo.outcome", name))
		if name != "success" {
			l.span.SetStatus(codes.Error, name)
		}
		l.span.End()
	})
}
