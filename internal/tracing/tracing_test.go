// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package tracing

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type captureExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (e *captureExporter) ExportSpans(_ context.Context, s []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, s...)
	return nil
}
func (e *captureExporter) Shutdown(context.Context) error { return nil }
func TestTracePrivacyAndIntervals(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "password=secret,tenant=private")
	t.Setenv("OTEL_SERVICE_NAME", "private-service")
	exporter := &captureExporter{}
	r := newRecorder(Config{SampleRatio: 1, QueueSize: 8}, exporter)
	start := time.Now().Add(-3 * time.Second)
	r.Record(Event{Kind: telemetry.KindQuery, Outcome: telemetry.OutcomeError, StartedAt: start, AdmissionWait: time.Second, Duration: 2 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	if len(exporter.spans) != 2 {
		t.Fatalf("span count=%d", len(exporter.spans))
	}
	for _, span := range exporter.spans {
		if detached, ok := span.(cleanSpan); !ok || detached.ReadOnlySpan != nil {
			t.Fatal("queued span retains original SDK span")
		}
		resource := span.Resource().Attributes()
		if len(resource) != 1 || string(resource[0].Key) != "service.name" || resource[0].Value.AsString() != "kelvo" {
			t.Fatalf("unexpected resource: %v", resource)
		}
		if len(span.Events()) != 0 || len(span.Links()) != 0 {
			t.Fatal("request data leaked in events or links")
		}
		switch span.Name() {
		case "kelvo.query":
			if span.EndTime().Sub(span.StartTime()) != 3*time.Second || len(span.Attributes()) != 2 || span.Status().Description != "error" {
				t.Fatal("incorrect lifecycle span")
			}
			for _, attribute := range span.Attributes() {
				if attribute.Key != "kelvo.kind" && attribute.Key != "kelvo.outcome" {
					t.Fatal("unbounded attribute")
				}
			}
		case "kelvo.admission":
			if span.EndTime().Sub(span.StartTime()) != time.Second || len(span.Attributes()) != 0 || !span.Parent().IsValid() {
				t.Fatal("incorrect admission child")
			}
		default:
			t.Fatalf("unexpected name %q", span.Name())
		}
	}
}
func TestTracingConfigurationAndCredentials(t *testing.T) {
	for _, endpoint := range []string{"http://localhost:4318/v1/traces", "https://user:password@example.com", "https://example.com?secret=x", "https://example.com#fragment", "https://example.com?", "relative"} {
		if (Config{Endpoint: endpoint}).Validate() == nil {
			t.Fatal("unsafe endpoint accepted")
		}
	}
	for _, cfg := range []Config{{Endpoint: "https://example.com", SampleRatio: math.NaN()}, {Endpoint: "https://example.com", SampleRatio: 2}, {Endpoint: "https://example.com", QueueSize: 1025}, {Endpoint: "https://example.com", ExportTimeout: 31 * time.Second}, {Endpoint: "https://example.com", TokenEnv: "invalid=name"}} {
		if cfg.Validate() == nil {
			t.Fatal("invalid tracing bounds accepted")
		}
	}
	if err := (Config{Endpoint: "https://example.com"}).Validate(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KELVO_TEST_TRACE_TOKEN", "secret\r\ninjection")
	if _, err := New(Config{Endpoint: "https://example.com", TokenEnv: "KELVO_TEST_TRACE_TOKEN"}); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("unsafe credential accepted or disclosed")
	}
}
func TestTraceSamplingZeroAndInvalidEvents(t *testing.T) {
	for _, ratio := range []float64{0, 1} {
		exporter := &captureExporter{}
		r := newRecorder(Config{SampleRatio: ratio}, exporter)
		r.Record(Event{Kind: telemetry.Kind(255), StartedAt: time.Now()})
		r.Record(Event{Kind: telemetry.KindQuery, StartedAt: time.Now(), Duration: -1})
		if ratio == 0 {
			r.Record(Event{Kind: telemetry.KindQuery, StartedAt: time.Now(), Duration: time.Second})
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := r.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		if len(exporter.spans) != 0 {
			t.Fatal("disabled/invalid events exported")
		}
	}
	var disabled *Recorder
	disabled.Record(Event{})
	if err := disabled.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestOTLPHTTPSBearerAndWirePrivacy(t *testing.T) {
	received := make(chan *collector.ExportTraceServiceRequest, 1)
	badAuth := make(chan bool, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badAuth <- r.Header.Get("Authorization") != "Bearer private-test-token"
		raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			w.WriteHeader(400)
			return
		}
		var request collector.ExportTraceServiceRequest
		if proto.Unmarshal(raw, &request) != nil {
			w.WriteHeader(400)
			return
		}
		received <- &request
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write([]byte{})
	}))
	defer server.Close()
	cfg := Config{Endpoint: server.URL + "/v1/traces", SampleRatio: 1, QueueSize: 8, ExportTimeout: time.Second}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	// Only this in-package test supplies its fixture certificate. Public New
	// always uses verified system roots and offers no insecure TLS switch.
	client, transport := newExportClient(cfg)
	transport.TLSClientConfig.RootCAs = roots
	defer transport.CloseIdleConnections()
	exporter, err := newHTTPExporter(cfg, "private-test-token", otlptracehttp.WithHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}
	r := newRecorder(cfg, exporter)
	r.Record(Event{Kind: telemetry.KindRefresh, Outcome: telemetry.OutcomeSuccess, StartedAt: time.Now().Add(-time.Second), Duration: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case bad := <-badAuth:
		if bad {
			t.Fatal("bearer token not supplied")
		}
	default:
		t.Fatal("no collector request")
	}
	select {
	case request := <-received:
		if strings.Contains(request.String(), "private-test-token") || strings.Contains(request.String(), "authorization") {
			t.Fatal("credential persisted in trace payload")
		}
		if len(request.ResourceSpans) != 1 || len(request.ResourceSpans[0].ScopeSpans) != 1 || len(request.ResourceSpans[0].ScopeSpans[0].Spans) != 1 {
			t.Fatal("unexpected OTLP framing")
		}
	default:
		t.Fatal("no protobuf export received")
	}
}

type blockedExporter struct {
	entered chan struct{}
	once    sync.Once
}

func (e *blockedExporter) ExportSpans(ctx context.Context, _ []sdktrace.ReadOnlySpan) error {
	e.once.Do(func() { close(e.entered) })
	<-ctx.Done()
	return errors.New("private collector response")
}
func (e *blockedExporter) Shutdown(context.Context) error { return nil }
func TestFullExportQueueNeverBlocksRecording(t *testing.T) {
	exporter := &blockedExporter{entered: make(chan struct{})}
	r := newRecorder(Config{SampleRatio: 1, QueueSize: 1, ExportTimeout: time.Second}, exporter)
	event := Event{Kind: telemetry.KindQuery, StartedAt: time.Now(), Duration: time.Millisecond}
	r.Record(event)
	select {
	case <-exporter.entered:
	case <-time.After(time.Second):
		t.Fatal("export did not start")
	}
	done := make(chan struct{})
	go func() {
		for range 256 {
			r.Record(event)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("full trace queue blocked query recording")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_ = r.Shutdown(ctx)
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("shutdown ignored deadline")
	}
}
func TestExporterErrorsSanitized(t *testing.T) {
	exporter := sanitizedExporter{failingExporter{}}
	if err := exporter.ExportSpans(context.Background(), nil); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("export error leaked")
	}
	if err := exporter.Shutdown(context.Background()); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("shutdown error leaked")
	}
}

type failingExporter struct{}

func (failingExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return errors.New("secret collector details")
}
func (failingExporter) Shutdown(context.Context) error { return errors.New("secret collector details") }

func traceFixtureSpan(t *testing.T) sdktrace.ReadOnlySpan {
	t.Helper()
	capture := &captureExporter{}
	recorder := newRecorder(Config{SampleRatio: 1}, capture)
	recorder.Record(Event{Kind: telemetry.KindQuery, StartedAt: time.Now(), Duration: time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := recorder.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if len(capture.spans) != 1 {
		t.Fatal("fixture span missing")
	}
	return capture.spans[0]
}

func TestHTTPSRejectsAmbientCertificateAndInsecureSettings(t *testing.T) {
	var received atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1); w.WriteHeader(200) }))
	defer server.Close()
	certFile := filepath.Join(t.TempDir(), "ambient.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE", certFile)
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", certFile)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:1/ambient")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1/ambient")
	cfg := Config{Endpoint: server.URL + "/v1/traces", SampleRatio: 1, ExportTimeout: time.Second}
	exporter, err := newHTTPExporter(cfg, "test-token")
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Shutdown(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := exporter.ExportSpans(ctx, []sdktrace.ReadOnlySpan{traceFixtureSpan(t)}); err == nil {
		t.Fatal("ambient config bypassed system-root verification")
	}
	if received.Load() != 0 {
		t.Fatal("untrusted collector received credentials")
	}
}

func TestCollectorRedirectNeverReceivesBearer(t *testing.T) {
	var redirected atomic.Int32
	var initial atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirected" {
			redirected.Add(1)
			w.WriteHeader(200)
			return
		}
		initial.Add(1)
		if r.Header.Get("Authorization") != "Bearer intended-token" {
			t.Error("ambient headers replaced configured token")
		}
		http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:1/ambient")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "authorization=Bearer ambient-token")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	cfg := Config{Endpoint: server.URL + "/v1/traces", SampleRatio: 1, ExportTimeout: time.Second}
	client, transport := newExportClient(cfg)
	defer transport.CloseIdleConnections()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	transport.TLSClientConfig.RootCAs = roots
	exporter, err := newHTTPExporter(cfg, "intended-token", otlptracehttp.WithHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Shutdown(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = exporter.ExportSpans(ctx, []sdktrace.ReadOnlySpan{traceFixtureSpan(t)})
	if initial.Load() != 1 || redirected.Load() != 0 {
		t.Fatalf("redirect followed or configured TLS endpoint not used: initial=%d redirect=%d", initial.Load(), redirected.Load())
	}
}

func TestCollectorResponseLimits(t *testing.T) {
	// A valid unknown protobuf field is ignored by the OTLP response parser.
	// Without a body cap both small and large payloads would succeed, so this
	// fixture distinguishes a size limit from merely invalid response protobuf.
	makeBody := func(size int) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, 127, protowire.BytesType), make([]byte, size))
	}
	small, large := makeBody(1024), makeBody(70<<10)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		switch r.URL.Path {
		case "/headers":
			w.Header().Set("X-Oversized", strings.Repeat("x", 20<<10))
			_, _ = w.Write([]byte{})
		case "/body":
			_, _ = w.Write(large)
		default:
			_, _ = w.Write(small)
		}
	}))
	defer server.Close()
	span := traceFixtureSpan(t)
	for _, path := range []string{"/small", "/body", "/headers"} {
		t.Run(path, func(t *testing.T) {
			cfg := Config{Endpoint: server.URL + path, ExportTimeout: time.Second}
			client, transport := newExportClient(cfg)
			defer transport.CloseIdleConnections()
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			transport.TLSClientConfig.RootCAs = roots
			exporter, err := newHTTPExporter(cfg, "", otlptracehttp.WithHTTPClient(client))
			if err != nil {
				t.Fatal(err)
			}
			defer exporter.Shutdown(context.Background())
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err = exporter.ExportSpans(ctx, []sdktrace.ReadOnlySpan{span})
			if path == "/small" && err != nil {
				t.Fatalf("small valid response rejected: %v", err)
			}
			if path != "/small" && err == nil {
				t.Fatal("oversized collector response accepted")
			}
		})
	}
}
