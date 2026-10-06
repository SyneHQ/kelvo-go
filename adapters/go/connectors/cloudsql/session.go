// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cloudsql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"maps"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/d1"
	"github.com/SYNEHQ/kelvo-go/internal/sources/databricks"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

type Session struct {
	source           catalog.Source
	credentials      cloudapi.Credentials
	database, schema string
	memoryMB         int
}

func Capabilities(engine string) operations.Capabilities {
	c := operations.Capabilities{Version: operations.Version, Engine: engine}
	if !adapter.CloudSQL(engine) {
		return c
	}
	types := []string{"null", "string", "bool", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "date", "timestamp"}
	if engine == "d1" {
		types = append(types, "binary")
	} else {
		types = append(types, "decimal128")
	}
	c.Operations = []operations.Capability{
		{Kind: operations.ConnectionTest, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.QueryRead, ParameterTypes: types, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.MetadataInspect, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.StatementExecute, ParameterTypes: types, Transactions: []operations.TransactionMode{operations.TransactionAutocommit}, Idempotency: "none", Cancellation: "best_effort"},
	}
	return c
}

// Open performs no source request; capability checks precede source effects.
// Existing bounded analytical readers handle Arrow types and pagination.
func Open(ctx context.Context, spec adapter.ConnectionSpec, limits adapter.ProcessLimits) (*Session, error) {
	if ctx == nil || ctx.Err() != nil || limits.MemoryMB < 16 || limits.MemoryMB > 1048576 || adapter.ValidateCloudSQLProcessSource(spec) != nil || spec.TenantID == "" || spec.ConnectionID == "" || spec.Revision == "" {
		return nil, adapter.ErrInvalid
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: spec.Options["tls_server_name"]}
	if pem := spec.Options["tls_ca_pem"]; pem != "" {
		config.RootCAs = x509.NewCertPool()
		if !config.RootCAs.AppendCertsFromPEM([]byte(pem)) {
			return nil, adapter.ErrInvalid
		}
	}
	options := maps.Clone(spec.Options)
	delete(options, "tls_ca_pem")
	delete(options, "tls_server_name")
	return &Session{source: catalog.Source{ID: "operation_source", Type: spec.Engine, Options: options}, credentials: cloudapi.Credentials{URL: spec.URL, Token: spec.Token, TLS: config}, database: spec.Database, schema: spec.Schema, memoryMB: limits.MemoryMB}, nil
}

func (s *Session) Close() error { s.credentials = cloudapi.Credentials{}; return nil }

func (s *Session) cloudLimits(ctx context.Context, rows, bytes int64) (query.Limits, error) {
	l := query.Limits{MaxRows: rows, MaxBytes: bytes, Timeout: 30 * time.Second, MemoryMB: s.memoryMB, Threads: 1, MaxTempMB: 1}
	if deadline, ok := ctx.Deadline(); ok {
		l.Timeout = min(l.Timeout, time.Until(deadline))
	}
	if l.Timeout <= 0 {
		return l, context.DeadlineExceeded
	}
	return l, l.Validate()
}

func (s *Session) Query(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	if s == nil || ctx == nil || sink == nil || q.Validate() != nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	limits, err := s.cloudLimits(ctx, q.MaxRows, q.MaxBytes)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	var engine interface {
		ExecuteBound(context.Context, query.Request, []operations.Parameter, query.Sink) (query.Stats, error)
		Close() error
	}
	config := catalog.Config{Sources: []catalog.Source{s.source}}
	switch s.source.Type {
	case "d1":
		engine, err = d1.NewResolved(config, limits, s.credentials)
	case "databricks":
		engine, err = databricks.NewResolved(config, limits, s.credentials)
	default:
		err = adapter.ErrUnsupported
	}
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer engine.Close()
	stats, err := engine.ExecuteBound(ctx, query.Request{Mode: "native", ConnectionID: s.source.ID, SQL: q.Statement}, q.Parameters, splitSink{next: sink, rows: int64(q.BatchRows)})
	return adapter.QueryStats{Rows: stats.Rows, Bytes: stats.Bytes, Elapsed: time.Duration(stats.DurationNS)}, err
}

func (s *Session) Test(ctx context.Context) error {
	stats, err := s.Query(ctx, adapter.Query{Statement: "SELECT 1", MaxRows: 1, MaxBytes: 64 << 10, BatchRows: 1}, discardSink{})
	if err == nil && stats.Rows != 1 {
		return adapter.ErrInvalid
	}
	return err
}

type discardSink struct{}

func (discardSink) Schema(*arrow.Schema) error    { return nil }
func (discardSink) Write(arrow.RecordBatch) error { return nil }

type splitSink struct {
	next adapter.Sink
	rows int64
}

func (s splitSink) Schema(schema *arrow.Schema) error { return s.next.Schema(schema) }
func (s splitSink) Write(batch arrow.RecordBatch) error {
	for start := int64(0); start < batch.NumRows(); start += s.rows {
		part := batch.NewSlice(start, min(start+s.rows, batch.NumRows()))
		err := s.next.Write(part)
		part.Release()
		if err != nil {
			return err
		}
	}
	return nil
}
