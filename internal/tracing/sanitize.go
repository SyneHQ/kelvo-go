// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package tracing

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var fixedResource = resource.NewSchemaless(attribute.String("service.name", "kelvo"))

// The SDK merges ambient resource attributes even with WithResource. Detach
// snapshots BEFORE queueing, not only before export: no queued snapshot retains
// the original SDK span or its environment-populated resource. The embedded nil
// interface provides SDK's private marker; every public accessor is implemented
// here. Review this adapter alongside SDK upgrades.
type cleanSpan struct {
	sdktrace.ReadOnlySpan
	name       string
	sc, parent trace.SpanContext
	start, end time.Time
	attrs      []attribute.KeyValue
	status     sdktrace.Status
	children   int
}

func (s cleanSpan) Name() string                     { return s.name }
func (s cleanSpan) SpanContext() trace.SpanContext   { return s.sc }
func (s cleanSpan) Parent() trace.SpanContext        { return s.parent }
func (s cleanSpan) SpanKind() trace.SpanKind         { return trace.SpanKindInternal }
func (s cleanSpan) StartTime() time.Time             { return s.start }
func (s cleanSpan) EndTime() time.Time               { return s.end }
func (s cleanSpan) Attributes() []attribute.KeyValue { return s.attrs }
func (s cleanSpan) Links() []sdktrace.Link           { return nil }
func (s cleanSpan) Events() []sdktrace.Event         { return nil }
func (s cleanSpan) Status() sdktrace.Status          { return s.status }
func (s cleanSpan) InstrumentationScope() instrumentation.Scope {
	return instrumentation.Scope{Name: "github.com/SYNEHQ/kelvo-go/lifecycle"}
}
func (s cleanSpan) InstrumentationLibrary() instrumentation.Library { return s.InstrumentationScope() }
func (s cleanSpan) Resource() *resource.Resource                    { return fixedResource }
func (s cleanSpan) DroppedAttributes() int                          { return 0 }
func (s cleanSpan) DroppedLinks() int                               { return 0 }
func (s cleanSpan) DroppedEvents() int                              { return 0 }
func (s cleanSpan) ChildSpanCount() int                             { return s.children }

type sanitizingProcessor struct{ next sdktrace.SpanProcessor }

func (p sanitizingProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (p sanitizingProcessor) OnEnd(span sdktrace.ReadOnlySpan) {
	clean := cleanSpan{name: span.Name(), sc: span.SpanContext(), parent: span.Parent(), start: span.StartTime(), end: span.EndTime(), children: span.ChildSpanCount()}
	switch clean.name {
	case "kelvo.query", "kelvo.refresh", "kelvo.admission",
		"kelvo.phase.validation", "kelvo.phase.node_admission", "kelvo.phase.source_admission",
		"kelvo.phase.prepare", "kelvo.phase.execution_delivery", "kelvo.phase.cleanup":
	default:
		return
	}
	for _, a := range span.Attributes() {
		value := a.Value.AsString()
		if (a.Key == "kelvo.kind" && (value == "query" || value == "refresh")) || (a.Key == "kelvo.outcome" && (value == "success" || value == "error" || value == "canceled")) {
			clean.attrs = append(clean.attrs, attribute.String(string(a.Key), value))
		}
	}
	status := span.Status()
	switch status.Description {
	case "", "error", "canceled":
		clean.status = status
	}
	p.next.OnEnd(clean)
}
func (p sanitizingProcessor) Shutdown(ctx context.Context) error   { return p.next.Shutdown(ctx) }
func (p sanitizingProcessor) ForceFlush(ctx context.Context) error { return p.next.ForceFlush(ctx) }
