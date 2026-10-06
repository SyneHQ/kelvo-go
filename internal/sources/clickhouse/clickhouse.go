// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package clickhouse delivers source-produced Arrow batches from ClickHouse's
// HTTP interface. It does not copy results through DuckDB or per-row Go maps.
package clickhouse

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

const maxErrorBytes = 4096

// Engine is safe for concurrent executions. Configuration is immutable after
// construction; credentials are resolved from the environment for each query.
type Engine struct {
	sources     map[string]catalog.Source
	compression map[string]string
	limits      query.Limits
	client      *http.Client
	resolved    map[string]ResolvedSource
}

func New(config catalog.Config, limits query.Limits) (*Engine, error) {
	if limits.MaxRows < 1 || limits.MaxBytes < 1 || limits.Timeout <= 0 || limits.MemoryMB < 1 || limits.Threads < 1 || int64(limits.MemoryMB) > math.MaxInt64/(1<<20) {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid ClickHouse execution limits")
	}
	sources := make(map[string]catalog.Source)
	compression := make(map[string]string)
	for _, source := range config.Sources {
		if source.Type != "clickhouse" {
			continue
		}
		if !catalog.ValidID(source.ID) || source.URLEnv == "" {
			return nil, query.NewError("INVALID_ARGUMENT", "Invalid ClickHouse source configuration")
		}
		if _, exists := sources[source.ID]; exists {
			return nil, query.NewError("INVALID_ARGUMENT", "Duplicate ClickHouse source")
		}
		codec, selected := source.Options["arrow_compression"]
		if !selected {
			codec = "none"
		}
		if codec != "none" && codec != "lz4_frame" {
			return nil, query.NewError("INVALID_ARGUMENT", "ClickHouse arrow_compression must be none or lz4_frame")
		}
		if _, err := sourceURL(source); err != nil {
			return nil, err
		}
		sources[source.ID] = source
		// Freeze the validated option independently of the caller's options map.
		compression[source.ID] = codec
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 64
	transport.MaxIdleConnsPerHost = 16
	transport.DisableCompression = true
	return &Engine{
		sources:     sources,
		compression: compression,
		limits:      limits,
		client: &http.Client{
			Transport: transport,
			// Never forward configured credentials to an HTTP redirect destination.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

// ResolvedSource is private one-operation connection material. Callers must
// authorize and resolve it before construction; never put it in a queue or log.
type ResolvedSource struct {
	ID          string
	URL         string
	Username    string
	Password    string
	Compression string
	TLS         *tls.Config
}

// NewResolved reuses the bounded native Arrow reader with transient credentials.
// It accepts only verified HTTPS and ignores all proxy/credential environment.
func NewResolved(source ResolvedSource, limits query.Limits) (*Engine, error) {
	if limits.Validate() != nil || !catalog.ValidID(source.ID) || source.Username == "" || source.Password == "" || len(source.Username) > 32<<10 || len(source.Password) > 32<<10 || strings.ContainsAny(source.Username, ":\x00\r\n") || strings.ContainsAny(source.Password, "\x00\r\n") {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid resolved ClickHouse source")
	}
	endpoint, err := parseSourceURL(source.URL)
	if err != nil || endpoint.Scheme != "https" || (endpoint.Path != "" && endpoint.Path != "/") || endpoint.Query().Get("database") == "" {
		return nil, query.NewError("INVALID_ARGUMENT", "Resolved ClickHouse source requires an HTTPS origin and database")
	}
	codec := source.Compression
	if codec == "" {
		codec = "none"
	}
	if codec != "none" && codec != "lz4_frame" {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid Arrow compression")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: endpoint.Hostname()}
	if source.TLS != nil {
		config = source.TLS.Clone()
	}
	if config.InsecureSkipVerify || config.MaxVersion != 0 && config.MaxVersion < tls.VersionTLS12 {
		return nil, query.NewError("INVALID_ARGUMENT", "Verified TLS 1.2 or newer is required")
	}
	if config.MinVersion < tls.VersionTLS12 {
		config.MinVersion = tls.VersionTLS12
	}
	if config.ServerName == "" {
		config.ServerName = endpoint.Hostname()
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = config
	transport.DisableCompression = true
	transport.MaxConnsPerHost = 1
	transport.MaxIdleConns = 1
	transport.MaxIdleConnsPerHost = 1
	source.TLS = nil
	return &Engine{sources: map[string]catalog.Source{source.ID: {ID: source.ID, Type: "clickhouse"}}, compression: map[string]string{source.ID: codec}, resolved: map[string]ResolvedSource{source.ID: source}, limits: limits, client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Close releases pooled idle HTTP connections. Active execution is cancelled by
// the context supplied to Execute.
func (e *Engine) Close() error {
	e.client.CloseIdleConnections()
	return nil
}

func (e *Engine) Execute(parent context.Context, req query.Request, sink query.Sink) (stats query.Stats, err error) {
	stats.Backend = "clickhouse"
	stats.EngineStreaming = true
	started := time.Now()
	defer func() { stats.DurationNS = time.Since(started).Nanoseconds() }()
	if sink == nil || strings.TrimSpace(req.SQL) == "" || req.ConnectionID == "" {
		return stats, query.NewError("INVALID_ARGUMENT", "Query, connection_id and result sink are required")
	}
	if req.Mode != "" && req.Mode != "native" {
		return stats, query.NewError("INVALID_ARGUMENT", "ClickHouse requires native execution mode")
	}
	if len(req.Parameters) != 0 || len(req.Sources) != 0 {
		return stats, query.NewError("UNSUPPORTED", "Native ClickHouse parameters and federation are not supported")
	}
	source, found := e.sources[req.ConnectionID]
	if !found {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	var endpoint *url.URL
	var username, password string
	if resolved, ok := e.resolved[source.ID]; ok {
		endpoint, err = parseSourceURL(resolved.URL)
		username, password = resolved.Username, resolved.Password
	} else {
		endpoint, err = sourceURL(source)
		if err == nil {
			username, password, err = credentials(source)
		}
	}
	if err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return stats, query.PublicError(err)
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return stats, query.NewError("QUERY_FAILED", "Could not prepare source query")
	}
	settings := endpoint.Query()
	settings.Set("query_id", "kelvo-"+hex.EncodeToString(id))
	settings.Set("readonly", "1")
	settings.Set("default_format", "ArrowStream")
	settings.Set("cancel_http_readonly_queries_on_client_close", "1")
	settings.Set("max_execution_time", strconv.FormatInt(int64(math.Ceil(e.limits.Timeout.Seconds())), 10))
	settings.Set("max_result_rows", strconv.FormatInt(e.limits.MaxRows, 10))
	settings.Set("max_result_bytes", strconv.FormatInt(e.limits.MaxBytes, 10))
	settings.Set("result_overflow_mode", "throw")
	settings.Set("max_memory_usage", strconv.FormatInt(int64(e.limits.MemoryMB)*(1<<20), 10))
	settings.Set("max_threads", strconv.Itoa(e.limits.Threads))
	settings.Set("output_format_arrow_compression_method", e.compression[source.ID])
	settings.Set("wait_end_of_query", "0")
	endpoint.RawQuery = settings.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(req.SQL))
	if err != nil {
		return stats, query.NewError("QUERY_FAILED", "Could not prepare source request")
	}
	httpReq.SetBasicAuth(username, password)
	httpReq.Header.Set("Content-Type", "text/plain; charset=utf-8")
	httpReq.Header.Set("Accept", "application/vnd.apache.arrow.stream")
	httpReq.Header.Set("User-Agent", "kelvo-go")
	response, err := e.client.Do(httpReq)
	if err != nil {
		return stats, sourceError(ctx, "Could not execute source query")
	}
	defer response.Body.Close()
	// Closing the response on cancellation also interrupts a blocked IPC read.
	stopClose := context.AfterFunc(ctx, func() { _ = response.Body.Close() })
	defer stopClose()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxErrorBytes))
		return stats, sourceError(ctx, "Source rejected query")
	}
	// An explicit SQL FORMAT override cannot be parsed as Arrow and fails closed.
	// ClickHouse >=26.8 can override FORMAT through output_format; older supported
	// servers only expose default_format, so no SQL rewriting is attempted here.
	stream := &streamReader{reader: response.Body, remaining: wireLimit(e.limits.MaxBytes)}
	defer func() { stats.SourceWireBytes = stream.bytesRead }()
	allocator := &boundedAllocator{base: memory.NewGoAllocator(), limit: int64(e.limits.MemoryMB) * (1 << 20)}
	framing := &boundedMessageReader{stream: stream, allocator: allocator}
	framing.refs.Store(1)
	reader, err := ipc.NewReaderFromMessageReader(framing, ipc.WithAllocator(allocator))
	if err != nil {
		framing.Release()
		return stats, streamError(ctx, allocator, stream, framing)
	}
	defer reader.Release()
	stats.PrepareNS = time.Since(started).Nanoseconds()
	if err := sink.Schema(reader.Schema()); err != nil {
		return stats, query.PublicError(err)
	}
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return stats, query.PublicError(err)
		}
		record := reader.RecordBatch()
		if record == nil {
			return stats, sourceError(ctx, "Source returned an invalid Arrow batch")
		}
		if record.NumRows() > e.limits.MaxRows-stats.Rows {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds row limit")
		}
		batchBytes := arrowutil.TotalRecordSize(record)
		if batchBytes > e.limits.MaxBytes-stats.Bytes {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds byte limit")
		}
		if err := sink.Write(record); err != nil {
			return stats, query.PublicError(err)
		}
		stats.Rows += record.NumRows()
		stats.Bytes += batchBytes
		stats.Batches++
	}
	if reader.Err() != nil || !framing.complete {
		return stats, streamError(ctx, allocator, stream, framing)
	}
	// ClickHouse may append a text exception after sending HTTP 200. An Arrow
	// decoder stops at its end marker, so require HTTP EOF without trailing data.
	var trailing [1]byte
	n, readErr := response.Body.Read(trailing[:])
	stream.bytesRead += int64(n)
	if n != 0 || !errors.Is(readErr, io.EOF) {
		return stats, sourceError(ctx, "Source returned an incomplete or invalid Arrow stream")
	}
	if err := ctx.Err(); err != nil {
		return stats, query.PublicError(err)
	}
	return stats, nil
}

func sourceURL(source catalog.Source) (*url.URL, error) {
	return parseSourceURL(os.Getenv(source.URLEnv))
}
func parseSourceURL(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || raw == "" || endpoint == nil || (endpoint.Scheme != "https" && endpoint.Scheme != "http") || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.Opaque != "" {
		return nil, query.NewError("INVALID_ARGUMENT", "ClickHouse source URL is unavailable or invalid")
	}
	values, err := url.ParseQuery(endpoint.RawQuery)
	if err != nil {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid ClickHouse source URL parameters")
	}
	// Only an administrator-configured database selection belongs in the URL.
	// In particular, credentials, query, sessions and settings cannot be injected.
	for key, values := range values {
		if key != "database" || len(values) != 1 {
			return nil, query.NewError("INVALID_ARGUMENT", "Only database may be configured in the ClickHouse source URL")
		}
	}
	return endpoint, nil
}

func credentials(source catalog.Source) (username, password string, err error) {
	username = "default"
	var found bool
	if source.UsernameEnv != "" {
		username, found = os.LookupEnv(source.UsernameEnv)
		if !found || username == "" || strings.Contains(username, ":") {
			return "", "", query.NewError("QUERY_FAILED", "Source credentials are unavailable")
		}
	}
	if source.PasswordEnv != "" {
		password, found = os.LookupEnv(source.PasswordEnv)
		if !found {
			return "", "", query.NewError("QUERY_FAILED", "Source credentials are unavailable")
		}
	}
	return username, password, nil
}

func sourceError(ctx context.Context, message string) error {
	if err := ctx.Err(); err != nil {
		return query.PublicError(err)
	}
	return query.NewError("QUERY_FAILED", message)
}

func streamError(ctx context.Context, allocator *boundedAllocator, stream *streamReader, framing *boundedMessageReader) error {
	if err := ctx.Err(); err != nil {
		return query.PublicError(err)
	}
	if framing.sourceFailure != nil {
		return framing.sourceFailure
	}
	if allocator.exceeded || stream.exceeded {
		return query.NewError("RESOURCE_EXHAUSTED", "Source Arrow stream exceeds memory or byte limit")
	}
	return query.NewError("QUERY_FAILED", "Source returned an incomplete or invalid Arrow stream")
}

func wireLimit(resultLimit int64) int64 {
	// Bound IPC metadata/dictionaries as well as result buffers. Allow bounded
	// framing overhead; this is not the reported uncompressed Arrow buffer count.
	overhead := resultLimit/8 + (1 << 20)
	if resultLimit > math.MaxInt64-overhead {
		return math.MaxInt64
	}
	return resultLimit + overhead
}

type streamReader struct {
	reader    io.Reader
	remaining int64
	bytesRead int64
	exceeded  bool
}

func (r *streamReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining <= 0 {
		r.exceeded = true
		return 0, errors.New("Arrow input limit exceeded")
	}
	if int64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	r.bytesRead += int64(n)
	return n, err
}

// Arrow's standard message reader allocates metadata outside its Allocator.
// Validate the metadata length before delegating so hostile source bytes cannot
// request a multi-gigabyte Go allocation. Body buffers use boundedAllocator.
type boundedMessageReader struct {
	refs          atomic.Int64
	stream        *streamReader
	allocator     *boundedAllocator
	current       ipc.MessageReader
	complete      bool
	sourceFailure error
}

func (r *boundedMessageReader) Retain() { r.refs.Add(1) }

func (r *boundedMessageReader) Release() {
	if r.refs.Add(-1) == 0 && r.current != nil {
		r.current.Release()
		r.current = nil
	}
}

func (r *boundedMessageReader) Message() (*ipc.Message, error) {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	var prefix [8]byte
	if _, err := io.ReadFull(r.stream, prefix[:4]); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	length, prefixLen := binary.LittleEndian.Uint32(prefix[:4]), 4
	if length == math.MaxUint32 {
		if _, err := io.ReadFull(r.stream, prefix[4:]); err != nil {
			return nil, io.ErrUnexpectedEOF
		}
		length, prefixLen = binary.LittleEndian.Uint32(prefix[4:]), 8
	}
	if length == 0 {
		r.complete = true
		return nil, io.EOF
	}
	if length > 8<<20 || int64(length) > r.allocator.limit {
		if prefixLen == 4 {
			// ClickHouse can send a textual exception after already flushing
			// HTTP 200 and valid batches. Its first four bytes are not an IPC
			// length. Read only bounded diagnostics and expose a numeric code,
			// never the source text (which can include SQL or credentials).
			tail, _ := io.ReadAll(io.LimitReader(r.stream, maxErrorBytes-4))
			r.sourceFailure = exceptionError(append(prefix[:4], tail...))
			return nil, r.sourceFailure
		}
		r.stream.exceeded = true
		return nil, errors.New("Arrow metadata limit exceeded")
	}
	r.current = ipc.NewMessageReader(io.MultiReader(bytes.NewReader(prefix[:prefixLen]), r.stream), ipc.WithAllocator(r.allocator))
	return r.current.Message()
}

func exceptionError(diagnostic []byte) error {
	const marker = "Code: "
	start := bytes.Index(diagnostic, []byte(marker))
	if start >= 0 {
		digits := diagnostic[start+len(marker):]
		end := 0
		for end < len(digits) && end < 8 && digits[end] >= '0' && digits[end] <= '9' {
			end++
		}
		if end > 0 {
			code, err := strconv.Atoi(string(digits[:end]))
			if err == nil && code == 241 {
				return query.NewError("RESOURCE_EXHAUSTED", "ClickHouse source query exceeded its memory limit (code 241)")
			}
			if err == nil {
				return query.NewError("QUERY_FAILED", fmt.Sprintf("ClickHouse source query failed during Arrow delivery (code %d)", code))
			}
		}
	}
	return query.NewError("QUERY_FAILED", "Source returned an incomplete or invalid Arrow stream")
}

// Arrow decodes lengths before reading payload bytes. Limiting only the network
// reader would still allow a malformed length to cause an oversized allocation.
// IPC's panic recovery converts this allocation refusal into a stream error.
type boundedAllocator struct {
	mu       sync.Mutex
	base     memory.Allocator
	used     int64
	limit    int64
	exceeded bool
}

func (a *boundedAllocator) Allocate(size int) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if size < 0 || int64(size) > a.limit-a.used {
		a.exceeded = true
		panic("Arrow allocation limit exceeded")
	}
	buf := a.base.Allocate(size)
	a.used += int64(len(buf))
	return buf
}

func (a *boundedAllocator) Reallocate(size int, buf []byte) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if size < 0 || int64(size)-int64(len(buf)) > a.limit-a.used {
		a.exceeded = true
		panic("Arrow allocation limit exceeded")
	}
	next := a.base.Reallocate(size, buf)
	a.used += int64(len(next)) - int64(len(buf))
	return next
}

func (a *boundedAllocator) Free(buf []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.used -= int64(len(buf))
	a.base.Free(buf)
}

var _ query.Executor = (*Engine)(nil)
