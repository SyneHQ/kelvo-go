// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package tracing provides opt-in, bounded, sanitized lifecycle tracing. It does
// not install a global provider or import request baggage and trace headers.
package tracing

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Config belongs to the trusted parent process. TokenEnv references a bearer
// token, not a literal secret. HTTPS uses verified system roots exclusively.
// No configuration means no exporter, background goroutine or recording cost.
type Config struct {
	Endpoint      string        `yaml:"endpoint"`
	TokenEnv      string        `yaml:"token_env,omitempty"`
	SampleRatio   float64       `yaml:"sample_ratio"`
	QueueSize     int           `yaml:"queue_size,omitempty"`
	ExportTimeout time.Duration `yaml:"export_timeout,omitempty"`
}

func (c Config) defaults() Config {
	if c.QueueSize == 0 {
		c.QueueSize = 64
	}
	if c.ExportTimeout == 0 {
		c.ExportTimeout = 5 * time.Second
	}
	return c
}
func (c Config) Validate() error {
	c = c.defaults()
	u, err := url.Parse(c.Endpoint)
	if err != nil || len(c.Endpoint) > 2048 || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return errors.New("tracing endpoint must be an HTTPS URL without credentials, query or fragment")
	}
	if c.SampleRatio < 0 || c.SampleRatio > 1 || c.SampleRatio != c.SampleRatio || c.QueueSize < 1 || c.QueueSize > 1024 || c.ExportTimeout <= 0 || c.ExportTimeout > 30*time.Second {
		return errors.New("invalid bounded tracing configuration")
	}
	if c.TokenEnv != "" {
		if len(c.TokenEnv) > 128 || !strings.HasPrefix(c.TokenEnv, "KELVO_") {
			return errors.New("invalid tracing token environment reference")
		}
		for i, b := range []byte(c.TokenEnv) {
			if !(b >= 'A' && b <= 'Z') && !(b >= 'a' && b <= 'z') && b != '_' && !(i > 0 && b >= '0' && b <= '9') {
				return errors.New("invalid tracing token environment reference")
			}
		}
	}
	return nil
}

// Event has no SQL, parameters, source identifiers, query IDs or raw errors.
// Duration excludes admission wait and includes setup, execution and transfer.
// These intervals are local worker observations, not full cluster queue time.
type Event struct {
	Parent        Carrier
	Kind          telemetry.Kind
	Outcome       telemetry.Outcome
	StartedAt     time.Time
	AdmissionWait time.Duration
	Duration      time.Duration
	// Phases are optional, exact local offsets. Their presence replaces the
	// legacy aggregate admission child; refresh events can omit them.
	Phases telemetry.PhaseTimings
}

type Recorder struct {
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
	stopped  atomic.Bool
}

func New(cfg Config) (*Recorder, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.defaults()
	token := ""
	if cfg.TokenEnv != "" {
		value, ok := os.LookupEnv(cfg.TokenEnv)
		if !ok || len(value) == 0 || len(value) > 4096 || strings.ContainsAny(value, "\r\n") {
			return nil, errors.New("tracing bearer token unavailable or invalid")
		}
		token = value
	}
	exporter, err := newHTTPExporter(cfg, token)
	if err != nil {
		return nil, errors.New("tracing exporter initialization failed")
	}
	return newRecorder(cfg, exporter), nil
}

// Extra options are used only by in-package HTTPS transport tests. The public
// constructor never supplies them and never exposes insecure TLS controls.
func newHTTPExporter(cfg Config, token string, extra ...otlptracehttp.Option) (sdktrace.SpanExporter, error) {
	headers := map[string]string{}
	if token != "" {
		headers["Authorization"] = "Bearer " + token
	}
	client, transport := newExportClient(cfg)
	options := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(cfg.Endpoint),
		otlptracehttp.WithHTTPClient(client),
		otlptracehttp.WithTLSClientConfig(&tls.Config{MinVersion: tls.VersionTLS13}),
		otlptracehttp.WithHeaders(headers),
		otlptracehttp.WithTimeout(cfg.ExportTimeout),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}),
		otlptracehttp.WithCompression(otlptracehttp.NoCompression),
		otlptracehttp.WithMaxResponseSize(64 << 10),
	}
	options = append(options, extra...)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ExportTimeout)
	defer cancel()
	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	return transportExporter{SpanExporter: exporter, close: transport.CloseIdleConnections}, nil
}

func newExportClient(cfg Config) (*http.Client, *http.Transport) {
	transport := &http.Transport{
		Proxy:                  nil,
		MaxResponseHeaderBytes: 16 << 10,
		DisableCompression:     true,
		DialContext:            (&net.Dialer{Timeout: cfg.ExportTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           2, MaxIdleConnsPerHost: 2, MaxConnsPerHost: 2,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   cfg.ExportTimeout,
		ResponseHeaderTimeout: cfg.ExportTimeout,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS13},
	}
	client := &http.Client{Transport: transport, Timeout: cfg.ExportTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	return client, transport
}

type transportExporter struct {
	sdktrace.SpanExporter
	close func()
}

func (e transportExporter) Shutdown(ctx context.Context) error {
	defer e.close()
	return e.SpanExporter.Shutdown(ctx)
}

// Errors returned by collectors can include endpoint/provider details. Only
// fixed errors reach SDK diagnostics; no raw export error is retained/unwrapped.
type sanitizedExporter struct{ inner sdktrace.SpanExporter }

func (e sanitizedExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if err := e.inner.ExportSpans(ctx, spans); err != nil {
		return errors.New("Kelvo trace export failed")
	}
	return nil
}
func (e sanitizedExporter) Shutdown(ctx context.Context) error {
	if err := e.inner.Shutdown(ctx); err != nil {
		return errors.New("Kelvo trace exporter shutdown failed")
	}
	return nil
}

func newRecorder(cfg Config, exporter sdktrace.SpanExporter) *Recorder {
	cfg = cfg.defaults()
	batchSize := cfg.QueueSize
	if batchSize > 32 {
		batchSize = 32
	}
	processor := sdktrace.NewBatchSpanProcessor(sanitizedExporter{exporter}, sdktrace.WithMaxQueueSize(cfg.QueueSize), sdktrace.WithMaxExportBatchSize(batchSize), sdktrace.WithBatchTimeout(time.Second), sdktrace.WithExportTimeout(cfg.ExportTimeout))
	// The SDK merges environment attributes into its resource. Our processor
	// detaches and filters spans before queueing; this local provider does not
	// affect other application tracing.
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sanitizingProcessor{processor}), sdktrace.WithSampler(ceilingSampler{sdktrace.TraceIDRatioBased(cfg.SampleRatio)}), sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "kelvo"))))
	return &Recorder{provider: provider, tracer: provider.Tracer("github.com/SYNEHQ/kelvo-go/lifecycle")}
}

// Record never waits for exporter queue space (SDK drop-on-full is retained).
// It imports only the validated internal carrier; client propagation is ignored.
func (r *Recorder) Record(event Event) {
	if r == nil || r.stopped.Load() || event.StartedAt.IsZero() || event.AdmissionWait < 0 || event.Duration < 0 || event.AdmissionWait > 24*time.Hour || event.Duration > 24*time.Hour {
		return
	}
	var name, kind, outcome string
	switch event.Kind {
	case telemetry.KindQuery:
		name, kind = "kelvo.query", "query"
	case telemetry.KindRefresh:
		name, kind = "kelvo.refresh", "refresh"
	default:
		return
	}
	switch event.Outcome {
	case telemetry.OutcomeSuccess:
		outcome = "success"
	case telemetry.OutcomeError:
		outcome = "error"
	case telemetry.OutcomeCanceled:
		outcome = "canceled"
	default:
		return
	}
	ctx, span := r.tracer.Start(carrierContext(event.Parent), name, trace.WithTimestamp(event.StartedAt), trace.WithAttributes(attribute.String("kelvo.kind", kind), attribute.String("kelvo.outcome", outcome)))
	if event.Outcome != telemetry.OutcomeSuccess {
		span.SetStatus(codes.Error, outcome)
	}
	if event.Phases.Valid(event.AdmissionWait+event.Duration) && event.Phases.Intervals[telemetry.PhaseValidation].Observed {
		for phase, interval := range event.Phases.Intervals {
			if !interval.Observed {
				continue
			}
			_, child := r.tracer.Start(ctx, "kelvo.phase."+telemetry.Phase(phase).Name(), trace.WithTimestamp(event.StartedAt.Add(interval.Start)))
			child.End(trace.WithTimestamp(event.StartedAt.Add(interval.End)))
		}
	} else if event.AdmissionWait > 0 {
		_, admission := r.tracer.Start(ctx, "kelvo.admission", trace.WithTimestamp(event.StartedAt))
		admission.End(trace.WithTimestamp(event.StartedAt.Add(event.AdmissionWait)))
	}
	span.End(trace.WithTimestamp(event.StartedAt.Add(event.AdmissionWait).Add(event.Duration)))
}
func (r *Recorder) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.stopped.Store(true)
	if err := r.provider.Shutdown(ctx); err != nil {
		return errors.New("tracing shutdown did not complete")
	}
	return nil
}
