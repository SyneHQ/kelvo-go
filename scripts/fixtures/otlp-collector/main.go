// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Disposable acceptance fixture; not a production collector.
package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	collectorv1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	maxRequestBytes = 256 << 10
	maxRequests     = 128
	maxSpans        = 2048
	maxTotalBytes   = 8 << 20
	maxConnections  = 8
)

var roles = [...]string{"gateway1", "gateway2", "worker"}
var spanNames = map[string]bool{
	"kelvo.query": true, "kelvo.refresh": true, "kelvo.admission": true,
	"kelvo.cluster.submit": true, "kelvo.cluster.dispatch": true,
	"kelvo.cluster.result_wait": true, "kelvo.cluster.relay": true,
	"kelvo.phase.validation": true, "kelvo.phase.node_admission": true,
	"kelvo.phase.source_admission": true, "kelvo.phase.prepare": true,
	"kelvo.phase.execution_delivery": true, "kelvo.phase.cleanup": true,
}

// Private evidence deliberately excludes raw requests, headers and error text.
type event struct {
	Role       string            `json:"role"`
	Mode       string            `json:"mode"`
	Name       string            `json:"name"`
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	ParentID   string            `json:"parent_id"`
	StartNS    uint64            `json:"start_ns"`
	EndNS      uint64            `json:"end_ns"`
	Attributes map[string]string `json:"attributes"`
}

type roleCounts struct {
	Requests      int `json:"requests"`
	Spans         int `json:"spans"`
	Accepted      int `json:"accepted"`
	AcceptedSpans int `json:"accepted_spans"`
	Stalled       int `json:"stalled"`
	Unavailable   int `json:"unavailable"`
	ActiveStalls  int `json:"active_stalls"`
}

type counters struct {
	Roles                [3]roleCounts `json:"roles"`
	Requests             int           `json:"requests"`
	Spans                int           `json:"spans"`
	AcceptedSpans        int           `json:"accepted_spans"`
	Bytes                int           `json:"bytes"`
	PrivacyViolations    int           `json:"privacy_violations"`
	PlanSourceViolations int           `json:"plan_source_privacy_violations"`
	CapViolations        int           `json:"cap_violations"`
	ProtocolViolations   int           `json:"protocol_violations"`
	AuthViolations       int           `json:"auth_violations"`
	WriteViolations      int           `json:"write_violations"`
	ConnectionsRejected  int           `json:"connections_rejected"`
}

type collector struct {
	mu               sync.Mutex
	counts           counters
	token            string
	modeFile         string
	denied           []string
	planSourceDenied []string
	output           *os.File
	stop             <-chan struct{}
}

func (c *collector) fail(kind string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch kind {
	case "privacy":
		c.counts.PrivacyViolations++
	case "plan_source":
		c.counts.PrivacyViolations++
		c.counts.PlanSourceViolations++
	case "cap":
		c.counts.CapViolations++
	case "auth":
		c.counts.AuthViolations++
	case "write":
		c.counts.WriteViolations++
	case "connection":
		c.counts.ConnectionsRejected++
	default:
		c.counts.ProtocolViolations++
	}
}

func (c *collector) snapshot() counters {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil || r.TLS.Version != tls.VersionTLS13 {
		c.fail("protocol")
		http.Error(w, "fixture protocol", http.StatusBadRequest)
		return
	}
	if len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+c.token)) != 1 {
		c.fail("auth")
		http.Error(w, "fixture authentication", http.StatusUnauthorized)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/fixture/status" && r.URL.RawQuery == "" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(c.snapshot())
		return
	}
	role := -1
	for i, name := range roles {
		if r.URL.Path == "/v1/traces/"+name {
			role = i
		}
	}
	if r.ContentLength > maxRequestBytes {
		c.fail("cap")
		http.Error(w, "fixture cap", http.StatusRequestEntityTooLarge)
		return
	}
	if role < 0 || r.URL.RawQuery != "" || r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/x-protobuf" || r.Header.Get("Content-Encoding") != "" || r.ContentLength <= 0 {
		c.fail("protocol")
		http.Error(w, "fixture protocol", http.StatusBadRequest)
		return
	}
	if r.Header.Get("Traceparent") != "" || r.Header.Get("Tracestate") != "" || r.Header.Get("Baggage") != "" {
		c.fail("privacy")
		http.Error(w, "fixture privacy", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil || len(body) != int(r.ContentLength) {
		c.fail("protocol")
		http.Error(w, "fixture body", http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	c.counts.Requests++
	c.counts.Bytes += len(body)
	c.counts.Roles[role].Requests++
	tooLarge := c.counts.Requests > maxRequests || c.counts.Bytes > maxTotalBytes
	c.mu.Unlock()
	if tooLarge {
		c.fail("cap")
		http.Error(w, "fixture cap", http.StatusTooManyRequests)
		return
	}
	for _, denied := range c.planSourceDenied {
		if bytes.Contains(body, []byte(denied)) {
			c.fail("plan_source")
			http.Error(w, "fixture privacy", http.StatusBadRequest)
			return
		}
	}
	for _, denied := range c.denied {
		if bytes.Contains(body, []byte(denied)) {
			c.fail("privacy")
			http.Error(w, "fixture privacy", http.StatusBadRequest)
			return
		}
	}
	var request collectorv1.ExportTraceServiceRequest
	if err := (proto.UnmarshalOptions{RecursionLimit: 32}).Unmarshal(body, &request); err != nil {
		c.fail("protocol")
		http.Error(w, "fixture protobuf", http.StatusBadRequest)
		return
	}
	spanCount := 0
	for _, resource := range request.ResourceSpans {
		for _, scoped := range resource.GetScopeSpans() {
			spanCount += len(scoped.GetSpans())
			if spanCount > maxSpans {
				c.fail("cap")
				http.Error(w, "fixture cap", http.StatusTooManyRequests)
				return
			}
		}
	}
	modeBytes, err := boundedFile(c.modeFile, 32)
	mode := strings.TrimSpace(string(modeBytes))
	if err != nil || (mode != "accept" && mode != "stall" && mode != "unavailable") {
		c.fail("protocol")
		http.Error(w, "fixture mode", http.StatusServiceUnavailable)
		return
	}
	events, ok := decodeEvents(&request, roles[role], mode)
	if !ok {
		c.fail("privacy")
		http.Error(w, "fixture privacy", http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	if c.counts.Spans+len(events) > maxSpans {
		c.counts.CapViolations++
		c.mu.Unlock()
		http.Error(w, "fixture cap", http.StatusTooManyRequests)
		return
	}
	c.counts.Spans += len(events)
	c.counts.Roles[role].Spans += len(events)
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	for _, e := range events {
		_ = encoder.Encode(e) // Fixed strings, numbers and string map cannot fail.
	}
	if n, err := c.output.Write(encoded.Bytes()); err != nil || n != encoded.Len() {
		c.counts.WriteViolations++
		c.mu.Unlock()
		http.Error(w, "fixture storage", http.StatusInternalServerError)
		return
	}
	switch mode {
	case "accept":
		c.counts.Roles[role].Accepted++
		c.counts.Roles[role].AcceptedSpans += len(events)
		c.counts.AcceptedSpans += len(events)
	case "stall":
		c.counts.Roles[role].Stalled++
		c.counts.Roles[role].ActiveStalls++
	case "unavailable":
		c.counts.Roles[role].Unavailable++
	}
	c.mu.Unlock()
	if mode == "stall" {
		defer func() {
			c.mu.Lock()
			c.counts.Roles[role].ActiveStalls--
			c.mu.Unlock()
		}()
		timer := time.NewTimer(8 * time.Second)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
		case <-c.stop:
		case <-timer.C:
		}
		return
	}
	if mode == "unavailable" {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

func stringAttributes(attributes []*commonv1.KeyValue, resource bool) (map[string]string, bool) {
	if len(attributes) > 2 {
		return nil, false
	}
	values := make(map[string]string, len(attributes))
	for _, attribute := range attributes {
		if attribute == nil || attribute.Value == nil || !onlyFields(attribute.ProtoReflect(), "key", "value") || !onlyFields(attribute.Value.ProtoReflect(), "string_value") {
			return nil, false
		}
		value, ok := attribute.Value.Value.(*commonv1.AnyValue_StringValue)
		if !ok {
			return nil, false
		}
		if _, duplicate := values[attribute.Key]; duplicate {
			return nil, false
		}
		if resource {
			if attribute.Key != "service.name" || value.StringValue != "kelvo" {
				return nil, false
			}
		} else if !((attribute.Key == "kelvo.kind" && (value.StringValue == "query" || value.StringValue == "refresh")) ||
			(attribute.Key == "kelvo.outcome" && (value.StringValue == "success" || value.StringValue == "error" || value.StringValue == "canceled"))) {
			return nil, false
		}
		values[attribute.Key] = value.StringValue
	}
	return values, !resource || len(values) == 1
}

func decodeEvents(request *collectorv1.ExportTraceServiceRequest, role, mode string) ([]event, bool) {
	if hasUnknown(request.ProtoReflect()) || !onlyFields(request.ProtoReflect(), "resource_spans") || len(request.ResourceSpans) == 0 {
		return nil, false
	}
	var events []event
	for _, resource := range request.ResourceSpans {
		if resource == nil || resource.Resource == nil || !onlyFields(resource.ProtoReflect(), "resource", "scope_spans") || !onlyFields(resource.Resource.ProtoReflect(), "attributes") {
			return nil, false
		}
		if _, ok := stringAttributes(resource.Resource.Attributes, true); !ok {
			return nil, false
		}
		for _, scoped := range resource.ScopeSpans {
			if scoped == nil || scoped.Scope == nil || !onlyFields(scoped.ProtoReflect(), "scope", "spans") || !onlyFields(scoped.Scope.ProtoReflect(), "name") || scoped.Scope.Name != "github.com/SYNEHQ/kelvo-go/lifecycle" {
				return nil, false
			}
			for _, span := range scoped.Spans {
				if span == nil || !spanNames[span.Name] || !nonzeroID(span.TraceId, 16) || !nonzeroID(span.SpanId, 8) ||
					(len(span.ParentSpanId) != 0 && !nonzeroID(span.ParentSpanId, 8)) || span.TraceState != "" ||
					span.Kind != tracev1.Span_SPAN_KIND_INTERNAL || span.StartTimeUnixNano == 0 || span.EndTimeUnixNano < span.StartTimeUnixNano ||
					len(span.Events) != 0 || len(span.Links) != 0 || span.DroppedAttributesCount != 0 || span.DroppedEventsCount != 0 || span.DroppedLinksCount != 0 {
					return nil, false
				}
				if !onlyFields(span.ProtoReflect(), "trace_id", "span_id", "parent_span_id", "name", "kind", "start_time_unix_nano", "end_time_unix_nano", "attributes", "status", "flags") {
					return nil, false
				}
				if span.Status != nil && ((span.Status.Message != "" && span.Status.Message != "error" && span.Status.Message != "canceled") ||
					(span.Status.Code != tracev1.Status_STATUS_CODE_UNSET && span.Status.Code != tracev1.Status_STATUS_CODE_ERROR) || !onlyFields(span.Status.ProtoReflect(), "code", "message")) {
					return nil, false
				}
				attributes, ok := stringAttributes(span.Attributes, false)
				if !ok {
					return nil, false
				}
				events = append(events, event{Role: role, Mode: mode, Name: span.Name, TraceID: hex.EncodeToString(span.TraceId), SpanID: hex.EncodeToString(span.SpanId),
					ParentID: hex.EncodeToString(span.ParentSpanId), StartNS: span.StartTimeUnixNano, EndNS: span.EndTimeUnixNano, Attributes: attributes})
			}
		}
	}
	return events, len(events) != 0
}

func onlyFields(message protoreflect.Message, names ...protoreflect.Name) bool {
	ok := len(message.GetUnknown()) == 0
	message.Range(func(field protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		found := false
		for _, name := range names {
			found = found || field.Name() == name
		}
		ok = ok && found
		return ok
	})
	return ok
}

func nonzeroID(value []byte, size int) bool {
	return len(value) == size && !bytes.Equal(value, make([]byte, size))
}

func hasUnknown(message protoreflect.Message) bool {
	if len(message.GetUnknown()) != 0 {
		return true
	}
	unknown := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
			return true
		}
		if field.IsList() {
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				unknown = unknown || hasUnknown(list.Get(i).Message())
			}
		} else {
			unknown = hasUnknown(value.Message())
		}
		return !unknown
	})
	return unknown
}

type cappedListener struct {
	net.Listener
	slots chan struct{}
	fail  func(string)
}

func (l cappedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &cappedConn{Conn: conn, release: func() { <-l.slots }}, nil
		default:
			l.fail("connection")
			_ = conn.Close()
		}
	}
}

type cappedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *cappedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

func boundedFile(path string, maximum int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	value, err := io.ReadAll(io.LimitReader(f, maximum+1))
	if err != nil || int64(len(value)) > maximum {
		return nil, errors.New("fixture file bound")
	}
	return value, nil
}

func run() error {
	listen := flag.String("listen", "127.0.0.1:14318", "loopback TLS address")
	cert := flag.String("cert", "", "prepared server certificate")
	key := flag.String("key", "", "prepared private key")
	tokenEnv := flag.String("token-env", "KELVO_OTLP_FIXTURE_TOKEN", "private authentication environment name")
	mode := flag.String("mode-file", "", "private atomic mode file")
	denyFile := flag.String("deny-file", "", "private JSON array of source/plan/secret canaries")
	planSourceFile := flag.String("plan-source-deny-file", "", "optional private JSON array for plan/source violation attribution")
	output := flag.String("events", "", "new private JSONL evidence")
	summary := flag.String("summary", "", "new final private counter evidence")
	flag.Parse()
	host, _, err := net.SplitHostPort(*listen)
	token := os.Getenv(*tokenEnv)
	if err != nil || host != "127.0.0.1" || len(token) < 32 || len(token) > 4096 || strings.ContainsAny(token, "\r\n") || *output == "" || *summary == "" {
		return errors.New("fixture arguments")
	}
	deniedBytes, err := boundedFile(*denyFile, 128<<10)
	var denied []string
	if err != nil || json.Unmarshal(deniedBytes, &denied) != nil || len(denied) == 0 || len(denied) > 32 {
		return errors.New("fixture denylist")
	}
	for _, value := range denied {
		if len(value) < 8 || len(value) > 4096 {
			return errors.New("fixture denylist bound")
		}
	}
	var planSourceDenied []string
	if *planSourceFile != "" {
		data, err := boundedFile(*planSourceFile, 128<<10)
		if err != nil || json.Unmarshal(data, &planSourceDenied) != nil || len(planSourceDenied) > 32 {
			return errors.New("fixture plan/source denylist")
		}
		for _, value := range planSourceDenied {
			if len(value) < 8 || len(value) > 4096 {
				return errors.New("fixture plan/source denylist bound")
			}
		}
	}
	certificate, err := tls.LoadX509KeyPair(*cert, *key)
	if err != nil {
		return errors.New("fixture certificate")
	}
	file, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.New("fixture evidence")
	}
	defer file.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	c := &collector{token: token, modeFile: *mode, denied: denied, planSourceDenied: planSourceDenied, output: file, stop: ctx.Done()}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return errors.New("fixture listener")
	}
	bounded := cappedListener{Listener: listener, slots: make(chan struct{}, maxConnections), fail: c.fail}
	tlsListener := tls.NewListener(bounded, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, NextProtos: []string{"http/1.1"}})
	server := &http.Server{Handler: c, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 2 * time.Second, MaxHeaderBytes: 16 << 10, ErrorLog: log.New(io.Discard, "", 0)}
	ended := make(chan error, 1)
	go func() { ended <- server.Serve(tlsListener) }()
	select {
	case err = <-ended:
		cancel()
	case <-ctx.Done():
		shutdown, stop := context.WithTimeout(context.Background(), 2*time.Second)
		err = server.Shutdown(shutdown)
		stop()
		if err != nil {
			_ = server.Close()
		}
		serveErr := <-ended
		if err == nil && !errors.Is(serveErr, http.ErrServerClosed) {
			err = serveErr
		}
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		c.fail("protocol")
	}
	if file.Sync() != nil {
		c.fail("write")
	}
	counts := c.snapshot()
	data, _ := json.Marshal(counts)
	final, openErr := os.OpenFile(*summary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if openErr != nil {
		return errors.New("fixture summary")
	}
	_, writeErr := final.Write(append(data, '\n'))
	closeErr := final.Close()
	if writeErr != nil || closeErr != nil || counts.PrivacyViolations+counts.CapViolations+counts.ProtocolViolations+counts.AuthViolations+counts.WriteViolations+counts.ConnectionsRejected != 0 {
		return errors.New("fixture acceptance failed")
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		// Every returned error is fixed fixture text, never upstream/private text.
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}
