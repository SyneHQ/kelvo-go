// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	collectorv1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func testAttribute(key, value string) *commonv1.KeyValue {
	return &commonv1.KeyValue{Key: key, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: value}}}
}

func testPayload() *collectorv1.ExportTraceServiceRequest {
	return &collectorv1.ExportTraceServiceRequest{ResourceSpans: []*tracev1.ResourceSpans{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{testAttribute("service.name", "kelvo")}},
		ScopeSpans: []*tracev1.ScopeSpans{{Scope: &commonv1.InstrumentationScope{Name: "github.com/SYNEHQ/kelvo-go/lifecycle"},
			Spans: []*tracev1.Span{{Name: "kelvo.query", TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{2}, 8),
				Kind: tracev1.Span_SPAN_KIND_INTERNAL, StartTimeUnixNano: 10, EndTimeUnixNano: 30,
				Attributes: []*commonv1.KeyValue{testAttribute("kelvo.kind", "query"), testAttribute("kelvo.outcome", "success")}},
				{Name: "kelvo.phase.validation", TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{3}, 8),
					ParentSpanId: bytes.Repeat([]byte{2}, 8), Kind: tracev1.Span_SPAN_KIND_INTERNAL, StartTimeUnixNano: 10, EndTimeUnixNano: 12}}}},
	}}}
}

func newTestCollector(t *testing.T) *collector {
	t.Helper()
	directory := t.TempDir()
	mode := filepath.Join(directory, "mode")
	if err := os.WriteFile(mode, []byte("accept\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := os.OpenFile(filepath.Join(directory, "events"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = output.Close() })
	return &collector{token: "test-fixture-token-is-private-and-bounded", modeFile: mode, output: output}
}

func exportPayload(t *testing.T, c *collector, payload *collectorv1.ExportTraceServiceRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := proto.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return exportBytes(c, body)
}

func exportBytes(c *collector, body []byte) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "https://127.0.0.1/v1/traces/worker", bytes.NewReader(body))
	r.TLS = &tls.ConnectionState{Version: tls.VersionTLS13}
	r.Header.Set("Authorization", "Bearer "+c.token)
	r.Header.Set("Content-Type", "application/x-protobuf")
	w := httptest.NewRecorder()
	c.ServeHTTP(w, r)
	return w
}

func TestCollectorAcceptsFixedOTLPAndPreservesParentEvidence(t *testing.T) {
	t.Parallel()
	c := newTestCollector(t)
	w := exportPayload(t, c, testPayload())
	if w.Code != http.StatusOK || w.Body.Len() != 0 || w.Header().Get("Content-Type") != "application/x-protobuf" {
		t.Fatal("valid fixed OTLP payload was not acknowledged")
	}
	s := c.snapshot()
	if s.Requests != 1 || s.Spans != 2 || s.AcceptedSpans != 2 || s.Roles[2].AcceptedSpans != 2 || s.Roles[2].Accepted != 1 ||
		s.PrivacyViolations+s.CapViolations+s.ProtocolViolations+s.AuthViolations+s.WriteViolations != 0 {
		t.Fatal("valid fixed payload produced incorrect counters")
	}
	if _, err := c.output.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	d := json.NewDecoder(c.output)
	var root, phase event
	if d.Decode(&root) != nil || d.Decode(&phase) != nil || d.Decode(new(event)) != io.EOF {
		t.Fatal("private evidence was not exactly two complete events")
	}
	if root.Role != "worker" || root.Mode != "accept" || root.Name != "kelvo.query" || root.ParentID != "" ||
		phase.ParentID != root.SpanID || phase.TraceID != root.TraceID || phase.Name != "kelvo.phase.validation" || len(phase.Attributes) != 0 {
		t.Fatal("collector changed IDs, roles or parent evidence")
	}
}

func TestCollectorRejectsForbiddenTelemetryBeforeWritingEvidence(t *testing.T) {
	cases := map[string]func(*collectorv1.ExportTraceServiceRequest){
		"resource attribute": func(p *collectorv1.ExportTraceServiceRequest) {
			p.ResourceSpans[0].Resource.Attributes = append(p.ResourceSpans[0].Resource.Attributes, testAttribute("source.url", "not-permitted"))
		},
		"resource value": func(p *collectorv1.ExportTraceServiceRequest) {
			p.ResourceSpans[0].Resource.Attributes[0] = testAttribute("service.name", "not-kelvo")
		},
		"scope attribute": func(p *collectorv1.ExportTraceServiceRequest) {
			p.ResourceSpans[0].ScopeSpans[0].Scope.Attributes = []*commonv1.KeyValue{testAttribute("plan", "not-permitted")}
		},
		"scope version": func(p *collectorv1.ExportTraceServiceRequest) {
			p.ResourceSpans[0].ScopeSpans[0].Scope.Version = "private-build"
		},
		"span attribute": func(p *collectorv1.ExportTraceServiceRequest) {
			p.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes = []*commonv1.KeyValue{testAttribute("db.statement", "not-permitted")}
		},
		"duplicate attribute": func(p *collectorv1.ExportTraceServiceRequest) {
			span := p.ResourceSpans[0].ScopeSpans[0].Spans[0]
			span.Attributes[1] = testAttribute("kelvo.kind", "query")
		},
		"events": func(p *collectorv1.ExportTraceServiceRequest) {
			p.ResourceSpans[0].ScopeSpans[0].Spans[0].Events = []*tracev1.Span_Event{{Name: "not-permitted"}}
		},
		"links": func(p *collectorv1.ExportTraceServiceRequest) {
			p.ResourceSpans[0].ScopeSpans[0].Spans[0].Links = []*tracev1.Span_Link{{TraceId: bytes.Repeat([]byte{4}, 16), SpanId: bytes.Repeat([]byte{5}, 8)}}
		},
		"tracestate": func(p *collectorv1.ExportTraceServiceRequest) {
			p.ResourceSpans[0].ScopeSpans[0].Spans[0].TraceState = "vendor=not-permitted"
		},
		"status message": func(p *collectorv1.ExportTraceServiceRequest) {
			p.ResourceSpans[0].ScopeSpans[0].Spans[0].Status = &tracev1.Status{Code: tracev1.Status_STATUS_CODE_ERROR, Message: "private-error"}
		},
		"name": func(p *collectorv1.ExportTraceServiceRequest) {
			p.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = "private-source"
		},
		"schema url": func(p *collectorv1.ExportTraceServiceRequest) {
			p.ResourceSpans[0].SchemaUrl = "https://not-permitted.invalid"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			payload := testPayload()
			mutate(payload)
			c := newTestCollector(t)
			w := exportPayload(t, c, payload)
			s := c.snapshot()
			info, err := c.output.Stat()
			if err != nil || info.Size() != 0 || w.Code != http.StatusBadRequest || s.PrivacyViolations != 1 || s.AcceptedSpans != 0 {
				t.Fatal("forbidden telemetry was acknowledged or retained")
			}
		})
	}
}

func TestCollectorRejectsRecursiveUnknownProtobufFields(t *testing.T) {
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 999, protowire.VarintType), 1)
	cases := map[string]func(*collectorv1.ExportTraceServiceRequest) protoreflect.Message{
		"request": func(p *collectorv1.ExportTraceServiceRequest) protoreflect.Message { return p.ProtoReflect() },
		"resource": func(p *collectorv1.ExportTraceServiceRequest) protoreflect.Message {
			return p.ResourceSpans[0].Resource.ProtoReflect()
		},
		"scope": func(p *collectorv1.ExportTraceServiceRequest) protoreflect.Message {
			return p.ResourceSpans[0].ScopeSpans[0].Scope.ProtoReflect()
		},
		"span": func(p *collectorv1.ExportTraceServiceRequest) protoreflect.Message {
			return p.ResourceSpans[0].ScopeSpans[0].Spans[0].ProtoReflect()
		},
		"attribute": func(p *collectorv1.ExportTraceServiceRequest) protoreflect.Message {
			return p.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[0].ProtoReflect()
		},
		"value": func(p *collectorv1.ExportTraceServiceRequest) protoreflect.Message {
			return p.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[0].Value.ProtoReflect()
		},
	}
	for name, target := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			payload := testPayload()
			target(payload).SetUnknown(bytes.Clone(unknown))
			c := newTestCollector(t)
			if w := exportPayload(t, c, payload); w.Code != http.StatusBadRequest || c.snapshot().PrivacyViolations != 1 || c.snapshot().AcceptedSpans != 0 {
				t.Fatal("recursive unknown field escaped strict privacy validation")
			}
		})
	}
}

func TestCollectorCapsFailWithoutPartialEvidence(t *testing.T) {
	for _, boundary := range []string{"request bytes", "requests", "total bytes", "per request spans", "total spans"} {
		t.Run(boundary, func(t *testing.T) {
			c := newTestCollector(t)
			payload := testPayload()
			switch boundary {
			case "requests":
				c.counts.Requests = maxRequests
			case "total bytes":
				c.counts.Bytes = maxTotalBytes
			case "per request spans":
				spans := make([]*tracev1.Span, maxSpans+1)
				for i := range spans {
					spans[i] = &tracev1.Span{}
				}
				payload.ResourceSpans[0].ScopeSpans[0].Spans = spans
			case "total spans":
				c.counts.Spans = maxSpans - 1
			}
			var w *httptest.ResponseRecorder
			if boundary == "request bytes" {
				w = exportBytes(c, bytes.Repeat([]byte{0}, maxRequestBytes+1))
			} else {
				w = exportPayload(t, c, payload)
			}
			info, err := c.output.Stat()
			s := c.snapshot()
			if err != nil || info.Size() != 0 || w.Code < 400 || s.CapViolations != 1 || s.AcceptedSpans != 0 || s.PrivacyViolations != 0 {
				t.Fatal("cap failure was silently truncated, misclassified or partially written")
			}
		})
	}
}

func TestCollectorAttributesPrivateCanaryFailures(t *testing.T) {
	for _, planSource := range []bool{false, true} {
		c := newTestCollector(t)
		c.denied = []string{"private-fixture-canary"}
		if planSource {
			c.planSourceDenied = append([]string(nil), c.denied...)
		}
		payload := testPayload()
		payload.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = "private-fixture-canary"
		w := exportPayload(t, c, payload)
		s := c.snapshot()
		wantPlanSource := 0
		if planSource {
			wantPlanSource = 1
		}
		if w.Code != http.StatusBadRequest || s.PrivacyViolations != 1 || s.PlanSourceViolations != wantPlanSource || s.AcceptedSpans != 0 {
			t.Fatal("private canary was retained or attributed to the wrong counter")
		}
	}
}
