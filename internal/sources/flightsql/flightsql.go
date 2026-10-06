// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package flightsql executes read-only SQL through the Arrow Flight SQL protocol.
package flightsql

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	flightSQL "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"github.com/SYNEHQ/kelvo-go/internal/arrowipc"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/SYNEHQ/kelvo-go/operations"
)

const maxTokenBytes = 16 << 10

type Engine struct {
	sources   map[string]catalog.Source
	limits    query.Limits
	tlsConfig func(endpoint) *tls.Config
	resolved  *cloudapi.Credentials
}

type endpoint struct{ address, host string }

func New(config catalog.Config, limits query.Limits) (*Engine, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	sources := make(map[string]catalog.Source)
	for _, source := range config.Sources {
		if source.Type != "arrow_flight" {
			continue
		}
		if !catalog.ValidID(source.ID) || source.URLEnv == "" || source.TokenEnv == "" || source.Options["protocol"] != "flightsql" {
			return nil, query.NewError("INVALID_ARGUMENT", "Arrow Flight source requires protocol flightsql, url_env and token_env")
		}
		if _, ok := sources[source.ID]; ok {
			return nil, query.NewError("INVALID_ARGUMENT", "Duplicate Arrow Flight source")
		}
		if _, err := parseEndpointEnv(source.URLEnv); err != nil {
			return nil, err
		}
		sources[source.ID] = source
	}
	return &Engine{sources: sources, limits: limits, tlsConfig: defaultTLSConfig}, nil
}

// NewResolved pins one source and never reads its environment references.
func NewResolved(config catalog.Config, limits query.Limits, value cloudapi.Credentials) (*Engine, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	source, err := cloudapi.SingleSource(config, "arrow_flight")
	if err != nil {
		return nil, err
	}
	if !catalog.ValidID(source.ID) || source.Options["protocol"] != "flightsql" || len(source.Options) != 1 {
		return nil, query.NewError("CONFIGURATION_ERROR", "Flight SQL requires an explicit protocol")
	}
	ep, err := parseEndpoint(value.URL)
	if err != nil {
		return nil, err
	}
	if _, err = validToken(value.Token); err != nil {
		return nil, err
	}
	cfg := defaultTLSConfig(ep)
	if value.TLS != nil {
		if value.TLS.InsecureSkipVerify || value.TLS.MaxVersion != 0 && value.TLS.MaxVersion < tls.VersionTLS12 {
			return nil, query.NewError("CONFIGURATION_ERROR", "Flight SQL requires verified TLS")
		}
		cfg = value.TLS.Clone()
		cfg.MinVersion = max(tls.VersionTLS12, cfg.MinVersion)
		if cfg.ServerName == "" {
			cfg.ServerName = ep.host
		}
		if cfg.RootCAs != nil {
			cfg.RootCAs = cfg.RootCAs.Clone()
		}
	}
	value.TLS = nil
	return &Engine{sources: map[string]catalog.Source{source.ID: source}, limits: limits, resolved: &value, tlsConfig: func(endpoint) *tls.Config { return cfg.Clone() }}, nil
}
func (e *Engine) Close() error {
	if e.resolved != nil {
		*e.resolved = cloudapi.Credentials{}
	}
	return nil
}

func (e *Engine) Execute(parent context.Context, req query.Request, sink query.Sink) (query.Stats, error) {
	return e.execute(parent, req, sink, nil)
}
func (e *Engine) execute(parent context.Context, req query.Request, sink query.Sink, discovery *operations.MetadataSpec) (stats query.Stats, err error) {
	stats.Backend, stats.EngineStreaming = "flightsql", true
	started := time.Now()
	defer func() { stats.DurationNS = time.Since(started).Nanoseconds() }()
	if sink == nil || req.Mode != "native" || req.ConnectionID == "" || len(req.Sources) != 0 {
		return stats, query.NewError("INVALID_ARGUMENT", "Flight SQL requires native mode, connection_id and result sink")
	}
	if req.Mongo != nil || len(req.Parameters) != 0 {
		return stats, query.NewError("UNSUPPORTED", "Flight SQL native queries do not support Mongo pipelines or parameters")
	}
	source, ok := e.sources[req.ConnectionID]
	if !ok {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	sql := req.SQL
	if discovery == nil {
		sql, err = sqlguard.ReadOnly(req.SQL)
		if err != nil {
			return stats, err
		}
	}
	var ep endpoint
	var token string
	if e.resolved != nil {
		ep, err = parseEndpoint(e.resolved.URL)
		if err == nil {
			token, err = validToken(e.resolved.Token)
		}
	} else {
		ep, err = parseEndpointEnv(source.URLEnv)
		if err == nil {
			token, err = tokenEnv(source.TokenEnv)
		}
	}
	if err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	allocator := &boundedAllocator{base: memory.NewGoAllocator(), limit: int64(e.limits.MemoryMB) << 20}
	client, err := flightSQL.NewClient(ep.address, nil, nil, grpc.WithTransportCredentials(credentials.NewTLS(e.tlsConfig(ep))), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMessage(e.limits))))
	if err != nil {
		return stats, sourceError(ctx, allocator)
	}
	defer client.Client.Close()
	var info *flight.FlightInfo
	if discovery == nil {
		info, err = client.Execute(ctx, sql)
	} else {
		var catalogName, schemaName, tableName *string
		if discovery.Target.Catalog != "" {
			catalogName = &discovery.Target.Catalog
		}
		if discovery.Target.Schema != "" {
			schemaName = &discovery.Target.Schema
		}
		if discovery.Target.Name != "" {
			tableName = &discovery.Target.Name
		}
		switch discovery.Object {
		case "catalogs", "databases":
			info, err = client.GetCatalogs(ctx)
		case "schemas":
			info, err = client.GetDBSchemas(ctx, &flightSQL.GetDBSchemasOpts{Catalog: catalogName, DbSchemaFilterPattern: schemaName})
		case "tables", "columns":
			info, err = client.GetTables(ctx, &flightSQL.GetTablesOpts{Catalog: catalogName, DbSchemaFilterPattern: schemaName, TableNameFilterPattern: tableName, IncludeSchema: discovery.Object == "columns"})
		default:
			return stats, query.NewError("UNSUPPORTED", "Flight SQL metadata object is unsupported")
		}
	}
	if err != nil {
		return stats, sourceError(ctx, allocator)
	}
	if info != nil {
		defer func() {
			if err != nil {
				cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
				defer stop()
				cleanup = metadata.AppendToOutgoingContext(cleanup, "authorization", "Bearer "+token)
				_, _ = client.Client.CancelFlightInfo(cleanup, &pb.CancelFlightInfoRequest{Info: info})
			}
		}()
	}
	if info == nil || len(info.Schema) == 0 || len(info.Schema) > 8<<20 || len(info.Endpoint) != 1 || info.Endpoint[0] == nil || info.Endpoint[0].Ticket == nil || len(info.Endpoint[0].Ticket.Ticket) == 0 || len(info.Endpoint[0].Ticket.Ticket) > 16<<10 || !sameEndpoint(info.Endpoint[0].Location, ep) {
		return stats, query.NewError("QUERY_FAILED", "Flight SQL returned an unsupported result endpoint")
	}
	if err := validateSchema(info.Schema); err != nil {
		return stats, query.NewError("QUERY_FAILED", "Flight SQL returned invalid Arrow schema")
	}
	expected, decodeErr := safeDeserializeSchema(info.Schema, allocator)
	if decodeErr != nil {
		return stats, sourceError(ctx, allocator)
	}
	stream, err := client.Client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return stats, sourceError(ctx, allocator)
	}
	reader, err := safeNewRecordReader(&validatingStream{DataStreamReader: stream}, allocator)
	if err != nil {
		return stats, sourceError(ctx, allocator)
	}
	defer reader.Release()
	stats.PrepareNS = time.Since(started).Nanoseconds()
	if !expected.Equal(reader.Schema()) {
		return stats, query.NewError("QUERY_FAILED", "Flight SQL returned a schema different from FlightInfo")
	}
	if err := sink.Schema(reader.Schema()); err != nil {
		return stats, query.PublicError(err)
	}
	for {
		next, nextErr := safeNext(reader, allocator)
		if nextErr != nil {
			return stats, sourceError(ctx, allocator)
		}
		if !next {
			break
		}
		if err := ctx.Err(); err != nil {
			return stats, query.PublicError(err)
		}
		record := reader.RecordBatch()
		if record == nil {
			return stats, query.NewError("QUERY_FAILED", "Flight SQL returned an invalid Arrow batch")
		}
		if record.NumRows() > e.limits.MaxRows-stats.Rows {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds row limit")
		}
		bytes := arrowutil.TotalRecordSize(record)
		if bytes > e.limits.MaxBytes-stats.Bytes {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds byte limit")
		}
		if err := sink.Write(record); err != nil {
			return stats, query.PublicError(err)
		}
		stats.Rows += record.NumRows()
		stats.Bytes += bytes
		stats.Batches++
	}
	if err := reader.Err(); err != nil {
		return stats, sourceError(ctx, allocator)
	}
	if err := ctx.Err(); err != nil {
		return stats, query.PublicError(err)
	}
	return stats, nil
}

func defaultTLSConfig(ep endpoint) *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12, ServerName: ep.host}
}

func parseEndpointEnv(name string) (endpoint, error) { return parseEndpoint(os.Getenv(name)) }
func parseEndpoint(raw string) (endpoint, error) {
	u, err := url.Parse(raw)
	if err != nil || raw == "" || u.Scheme != "grpcs" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return endpoint{}, query.NewError("INVALID_ARGUMENT", "Flight SQL source URL must be a grpcs host and port")
	}
	_, port, splitErr := net.SplitHostPort(u.Host)
	if splitErr != nil || port == "" {
		return endpoint{}, query.NewError("INVALID_ARGUMENT", "Flight SQL source URL must include a valid port")
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return endpoint{}, query.NewError("INVALID_ARGUMENT", "Flight SQL source URL must include a valid port")
	}
	return endpoint{address: u.Host, host: u.Hostname()}, nil
}
func tokenEnv(name string) (string, error) { return validToken(os.Getenv(name)) }
func validToken(token string) (string, error) {
	if token == "" || len(token) > maxTokenBytes || strings.ContainsAny(token, "\r\n\x00") {
		return "", query.NewError("QUERY_FAILED", "Flight SQL source credentials are unavailable")
	}
	return token, nil
}

// Empty locations mean use the connected server. Non-empty locations are accepted
// only when they name the exact configured TLS endpoint; no external endpoint is followed.
func sameEndpoint(locations []*flight.Location, ep endpoint) bool {
	for _, location := range locations {
		if location == nil {
			return false
		}
		u, err := url.Parse(location.Uri)
		if err != nil || u.Scheme != "grpc+tls" && u.Scheme != "grpcs" || u.Host != ep.address || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return false
		}
	}
	return true
}

type validatingStream struct{ flight.DataStreamReader }

func (s *validatingStream) Recv() (*flight.FlightData, error) {
	d, err := s.DataStreamReader.Recv()
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, arrowipc.ErrInvalid
	}
	if len(d.AppMetadata) > 64<<10 {
		return nil, arrowipc.ErrLimit
	}
	if len(d.DataHeader) == 0 {
		if len(d.DataBody) != 0 {
			return nil, arrowipc.ErrInvalid
		}
		return d, nil
	}
	if len(d.DataHeader) > 8<<20 {
		return nil, arrowipc.ErrLimit
	}
	body, err := arrowipc.ValidateMessageMetadata(d.DataHeader)
	if err != nil || body != int64(len(d.DataBody)) {
		return nil, arrowipc.ErrInvalid
	}
	return d, nil
}
func safeDeserializeSchema(data []byte, a memory.Allocator) (schema *arrow.Schema, err error) {
	defer func() {
		if recover() != nil {
			err = arrowipc.ErrInvalid
		}
	}()
	return flight.DeserializeSchema(data, a)
}
func safeNewRecordReader(stream flight.DataStreamReader, a memory.Allocator) (r *flight.Reader, err error) {
	defer func() {
		if recover() != nil {
			err = arrowipc.ErrInvalid
		}
	}()
	return flight.NewRecordReader(stream, ipc.WithAllocator(a))
}
func validateSchema(data []byte) error {
	if len(data) < 4 {
		return arrowipc.ErrInvalid
	}
	n := int(binary.LittleEndian.Uint32(data[:4]))
	off := 4
	if n == int(^uint32(0)) {
		if len(data) < 8 {
			return arrowipc.ErrInvalid
		}
		n = int(binary.LittleEndian.Uint32(data[4:8]))
		off = 8
	}
	if n <= 0 || n > 8<<20 || (n != len(data)-off && (len(data) != off+n+8 || binary.LittleEndian.Uint32(data[off+n:off+n+4]) != uint32(^uint32(0)) || binary.LittleEndian.Uint32(data[off+n+4:]) != 0)) {
		return arrowipc.ErrInvalid
	}
	_, err := arrowipc.ValidateMessageMetadata(data[off : off+n])
	return err
}

func maxRecvMessage(l query.Limits) int {
	n := int64(l.MemoryMB) << 18
	if n < 1<<20 {
		n = 1 << 20
	}
	if n > 64<<20 {
		n = 64 << 20
	}
	if n > int64(^uint(0)>>1) {
		return int(^uint(0) >> 1)
	}
	return int(n)
}
func safeNext(r *flight.Reader, allocator *boundedAllocator) (next bool, err error) {
	defer func() {
		if recover() != nil {
			allocator.mu.Lock()
			allocator.exceeded = true
			allocator.mu.Unlock()
			err = query.NewError("RESOURCE_EXHAUSTED", "Flight SQL result exceeds memory limit")
		}
	}()
	return r.Next(), nil
}

func sourceError(ctx context.Context, allocator *boundedAllocator) error {
	if err := ctx.Err(); err != nil {
		return query.PublicError(err)
	}
	allocator.mu.Lock()
	exceeded := allocator.exceeded
	allocator.mu.Unlock()
	if exceeded {
		return query.NewError("RESOURCE_EXHAUSTED", "Flight SQL result exceeds memory limit")
	}
	return query.NewError("QUERY_FAILED", "Flight SQL source query failed")
}

type boundedAllocator struct {
	mu          sync.Mutex
	base        memory.Allocator
	used, limit int64
	exceeded    bool
}

func (a *boundedAllocator) Allocate(size int) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if size < 0 || int64(size) > a.limit-a.used {
		a.exceeded = true
		panic("Arrow allocation limit exceeded")
	}
	b := a.base.Allocate(size)
	a.used += int64(len(b))
	return b
}
func (a *boundedAllocator) Reallocate(size int, b []byte) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if size < 0 || int64(size)-int64(len(b)) > a.limit-a.used {
		a.exceeded = true
		panic("Arrow allocation limit exceeded")
	}
	next := a.base.Reallocate(size, b)
	a.used += int64(len(next)) - int64(len(b))
	return next
}
func (a *boundedAllocator) Free(b []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.used -= int64(len(b))
	a.base.Free(b)
}

var _ query.Executor = (*Engine)(nil)
