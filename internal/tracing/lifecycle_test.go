// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package tracing

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func shutdownRecorder(t *testing.T, recorder *Recorder) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := recorder.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestClusterLifecycleParentingAndFixedAttributes(t *testing.T) {
	exporter := &captureExporter{}
	recorder := newRecorder(Config{SampleRatio: 1, QueueSize: 32}, exporter)
	submit := recorder.Start(Carrier{}, ClusterSubmit)
	if !submit.Carrier().Valid() || !submit.Carrier().Sampled {
		t.Fatal("enabled authorized operation has no valid carrier")
	}
	parent := submit.Carrier()
	submit.End(telemetry.OutcomeSuccess)
	dispatch := recorder.Start(parent, ClusterDispatch)
	wait := recorder.Start(parent, ClusterResultWait)
	relay := recorder.Start(parent, ClusterRelay)
	recorder.Record(Event{Parent: dispatch.Carrier(), Kind: telemetry.KindQuery, Outcome: telemetry.OutcomeSuccess, StartedAt: time.Now(), Duration: time.Millisecond})
	dispatch.End(telemetry.OutcomeSuccess)
	wait.End(telemetry.OutcomeError)
	relay.End(telemetry.OutcomeCanceled)
	relay.End(telemetry.OutcomeSuccess)
	shutdownRecorder(t, recorder)
	if len(exporter.spans) != 5 {
		t.Fatalf("expected five bounded spans, got %d", len(exporter.spans))
	}
	seen := map[string]bool{}
	for _, span := range exporter.spans {
		if seen[span.Name()] || span.SpanContext().TraceID().String() != parent.TraceID {
			t.Fatal("duplicate lifecycle completion or broken trace continuity")
		}
		seen[span.Name()] = true
		if span.SpanContext().TraceState().Len() != 0 || span.Parent().TraceState().Len() != 0 || len(span.Events()) != 0 || len(span.Links()) != 0 || len(span.Resource().Attributes()) != 1 {
			t.Fatal("lifecycle trace retained forbidden context")
		}
		attrs := span.Attributes()
		if len(attrs) != 2 {
			t.Fatal("lifecycle trace has unexpected attributes")
		}
		for _, a := range attrs {
			if a.Key != "kelvo.kind" && a.Key != "kelvo.outcome" {
				t.Fatal("lifecycle trace has a dynamic label")
			}
		}
		if span.EndTime().Before(span.StartTime()) {
			t.Fatal("local lifecycle interval is reversed")
		}
		switch span.Name() {
		case "kelvo.cluster.submit":
			if span.Parent().IsValid() {
				t.Fatal("root operation imported ambient context")
			}
		case "kelvo.query":
			if span.Parent().SpanID().String() != dispatch.Carrier().SpanID {
				t.Fatal("worker record lost its validated dispatch parent")
			}
		case "kelvo.cluster.dispatch", "kelvo.cluster.result_wait", "kelvo.cluster.relay":
			if span.Parent().SpanID().String() != parent.SpanID {
				t.Fatal("cluster operation lost its durable job parent")
			}
		default:
			t.Fatal("unapproved cluster operation name")
		}
	}
}

func TestLifecycleSamplingHonorsParentAndLocalCeiling(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ratio   float64
		sampled bool
		traceID string
		want    bool
	}{
		{"local disabled", 0, true, "00000000000000000000000000000001", false},
		{"parent unsampled", 1, false, "00000000000000000000000000000001", false},
		{"sampled parent below local ratio", 0.5, true, "00000000000000000000000000000001", true},
		{"sampled parent above local ratio", 0.5, true, "ffffffffffffffffffffffffffffffff", false},
		{"both sampled", 1, true, "1234567890abcdef1234567890abcdef", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exporter := &captureExporter{}
			recorder := newRecorder(Config{SampleRatio: tc.ratio, QueueSize: 8}, exporter)
			parent := testCarrier(tc.sampled)
			parent.TraceID = tc.traceID
			span := recorder.Start(parent, ClusterDispatch)
			if !span.Carrier().Valid() || span.Carrier().Sampled != tc.want {
				t.Fatal("parent sampling bypassed local policy or was re-enabled")
			}
			span.End(telemetry.OutcomeSuccess)
			shutdownRecorder(t, recorder)
			if (len(exporter.spans) == 1) != tc.want || len(exporter.spans) > 1 {
				t.Fatal("sampling decision and exported spans differ")
			}
		})
	}
}

func TestLifecycleNilInvalidAndConcurrentCompletion(t *testing.T) {
	var disabled *Recorder
	if disabled.Start(testCarrier(true), ClusterSubmit) != nil {
		t.Fatal("disabled recorder created lifecycle state")
	}
	var absent *Lifecycle
	if absent.Carrier() != (Carrier{}) {
		t.Fatal("disabled lifecycle created a carrier")
	}
	absent.End(telemetry.OutcomeSuccess)
	new(Lifecycle).End(telemetry.OutcomeSuccess)
	exporter := &captureExporter{}
	recorder := newRecorder(Config{SampleRatio: 1, QueueSize: 8}, exporter)
	if recorder.Start(Carrier{}, Operation(255)) != nil {
		t.Fatal("unknown operation created a span")
	}
	span := recorder.Start(Carrier{Version: 1, TraceID: "private"}, ClusterRelay)
	if !span.Carrier().Valid() {
		t.Fatal("invalid optional parent prevented fresh diagnostics")
	}
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			span.End(telemetry.OutcomeSuccess)
		}()
	}
	group.Wait()
	shutdownRecorder(t, recorder)
	if len(exporter.spans) != 1 || exporter.spans[0].Parent().IsValid() {
		t.Fatal("invalid parent retained correlation or repeated completion")
	}
	if recorder.Start(testCarrier(true), ClusterSubmit) != nil {
		t.Fatal("stopped recorder created lifecycle state")
	}
}

type samplerCapture struct {
	parent trace.SpanContext
	called bool
}

func (s *samplerCapture) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	s.called = true
	s.parent = trace.SpanContextFromContext(p.ParentContext)
	state, _ := trace.ParseTraceState("vendor=private")
	return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample, Tracestate: state, Attributes: []attribute.KeyValue{attribute.String("tenant", "private")}}
}
func (*samplerCapture) Description() string { return "test" }

func TestSamplingStripsStateBeforeDelegateAndResult(t *testing.T) {
	state, _ := trace.ParseTraceState("vendor=private")
	parent := trace.SpanContextFromContext(carrierContext(testCarrier(true))).WithTraceState(state)
	delegate := &samplerCapture{}
	sampler := ceilingSampler{ratio: delegate}
	result := sampler.ShouldSample(sdktrace.SamplingParameters{ParentContext: trace.ContextWithRemoteSpanContext(context.Background(), parent)})
	if !delegate.called || !delegate.parent.IsValid() || delegate.parent.TraceState().Len() != 0 || result.Tracestate.Len() != 0 || len(result.Attributes) != 0 {
		t.Fatal("sampling retained state before or after its local policy")
	}
	delegate.called = false
	parent = parent.WithTraceFlags(0)
	result = sampler.ShouldSample(sdktrace.SamplingParameters{ParentContext: trace.ContextWithRemoteSpanContext(context.Background(), parent)})
	if delegate.called || result.Decision != sdktrace.Drop {
		t.Fatal("unsampled parent reached a sampler that could enable it")
	}
}

type contextSnapshot struct {
	sdktrace.ReadOnlySpan
	context trace.SpanContext
}

func (s contextSnapshot) Name() string                     { return "kelvo.cluster.relay" }
func (s contextSnapshot) SpanContext() trace.SpanContext   { return s.context }
func (s contextSnapshot) Parent() trace.SpanContext        { return s.context }
func (s contextSnapshot) StartTime() time.Time             { return time.Unix(1, 0) }
func (s contextSnapshot) EndTime() time.Time               { return time.Unix(2, 0) }
func (s contextSnapshot) ChildSpanCount() int              { return 0 }
func (s contextSnapshot) Attributes() []attribute.KeyValue { return nil }
func (s contextSnapshot) Status() sdktrace.Status          { return sdktrace.Status{Code: codes.Unset} }

type queuedSpanCapture struct{ span sdktrace.ReadOnlySpan }

func (*queuedSpanCapture) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (p *queuedSpanCapture) OnEnd(span sdktrace.ReadOnlySpan)              { p.span = span }
func (*queuedSpanCapture) Shutdown(context.Context) error                  { return nil }
func (*queuedSpanCapture) ForceFlush(context.Context) error                { return nil }

func TestQueuedSnapshotStripsBothTraceStates(t *testing.T) {
	state, _ := trace.ParseTraceState("vendor=private")
	sc := trace.SpanContextFromContext(carrierContext(testCarrier(true))).WithTraceState(state).WithTraceFlags(trace.TraceFlags(255))
	queue := &queuedSpanCapture{}
	processor := sanitizingProcessor{next: queue}
	processor.OnEnd(contextSnapshot{context: sc})
	clean, ok := queue.span.(cleanSpan)
	if !ok || clean.ReadOnlySpan != nil || clean.sc.TraceState().Len() != 0 || clean.parent.TraceState().Len() != 0 || clean.sc.TraceFlags() != trace.FlagsSampled || clean.parent.TraceFlags() != trace.FlagsSampled {
		t.Fatal("queued snapshot retained unsanitized trace state")
	}
	if clean.sc.TraceID() != sc.TraceID() || clean.parent.SpanID() != sc.SpanID() {
		t.Fatal("sanitization broke validated correlation IDs")
	}
}
