// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package nativereader

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"maps"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/saas"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/athena"
	"github.com/SYNEHQ/kelvo-go/internal/sources/awsapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/bigquery"
	"github.com/SYNEHQ/kelvo-go/internal/sources/clickhouselambda"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cosmosdb"
	"github.com/SYNEHQ/kelvo-go/internal/sources/dynamodb"
	"github.com/SYNEHQ/kelvo-go/internal/sources/elasticsearch"
	"github.com/SYNEHQ/kelvo-go/internal/sources/exasol"
	"github.com/SYNEHQ/kelvo-go/internal/sources/flightsql"
	"github.com/SYNEHQ/kelvo-go/internal/sources/ignite"
	"github.com/SYNEHQ/kelvo-go/internal/sources/snowflake"
	"github.com/SYNEHQ/kelvo-go/internal/sources/spanner"
	"github.com/SYNEHQ/kelvo-go/internal/sources/trino"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

type Session struct {
	spec     adapter.ConnectionSpec
	source   catalog.Source
	tls      *tls.Config
	memoryMB int
}
type reader interface {
	query.Executor
	Close() error
}

// Open validates only. Every source connection and credential belongs to this invocation.
func Open(ctx context.Context, spec adapter.ConnectionSpec, limits adapter.ProcessLimits) (*Session, error) {
	if ctx == nil || ctx.Err() != nil || limits.MemoryMB < 16 || limits.MemoryMB > 1048576 || adapter.ValidateNativeReaderProcessSource(spec) != nil || spec.TenantID == "" || spec.ConnectionID == "" || spec.Revision == "" {
		return nil, adapter.ErrInvalid
	}
	if spec.Engine == "bigquery" {
		token, err := saas.GoogleAccessToken(ctx, spec.Token, "https://www.googleapis.com/auth/bigquery")
		if err != nil {
			return nil, adapter.ErrInvalid
		}
		spec.Token = token
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: spec.Options["tls_server_name"]}
	if pem := spec.Options["tls_ca_pem"]; pem != "" {
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM([]byte(pem)) {
			return nil, adapter.ErrInvalid
		}
	}
	spec.Options = maps.Clone(spec.Options)
	delete(spec.Options, "tls_ca_pem")
	delete(spec.Options, "tls_server_name")
	options := maps.Clone(spec.Options)
	if spec.Engine == "elasticsearch" && spec.Username != "" {
		delete(options, "authentication")
	}
	if spec.Engine == "exasol" {
		delete(options, "schema")
	}
	return &Session{spec: spec, source: catalog.Source{ID: "operation_source", Type: spec.Engine, Options: options}, tls: cfg, memoryMB: limits.MemoryMB}, nil
}
func (s *Session) Close() error {
	if s != nil {
		s.spec = adapter.ConnectionSpec{}
		s.source = catalog.Source{}
		s.tls = nil
	}
	return nil
}
func (s *Session) limits(ctx context.Context, rows, bytes int64) (query.Limits, error) {
	l := query.Limits{MaxRows: rows, MaxBytes: bytes, MemoryMB: s.memoryMB, Threads: 1, MaxTempMB: 1, Timeout: 30 * time.Second}
	if deadline, ok := ctx.Deadline(); ok {
		l.Timeout = min(l.Timeout, time.Until(deadline))
	}
	if l.Timeout <= 0 {
		return l, context.DeadlineExceeded
	}
	return l, l.Validate()
}
func (s *Session) open(l query.Limits) (reader, error) {
	c := catalog.Config{Sources: []catalog.Source{s.source}}
	creds := cloudapi.Credentials{URL: s.spec.URL, Token: s.spec.Token, TLS: s.tls}
	switch s.spec.Engine {
	case "elasticsearch":
		if s.spec.Username != "" {
			return elasticsearch.NewResolvedBasic(c, l, creds, s.spec.Username, s.spec.Password)
		}
		return elasticsearch.NewResolved(c, l, creds)
	case "trino", "presto":
		if s.spec.Password != "" {
			return trino.NewResolvedBasic(c, l, creds, s.spec.Username, s.spec.Password)
		}
		return trino.NewResolved(c, l, creds, s.spec.Username)
	case "arrow_flight":
		return flightsql.NewResolved(c, l, creds)
	case "bigquery":
		return bigquery.NewResolved(c, l, creds)
	case "snowflake":
		return snowflake.NewResolved(c, l, creds)
	case "spanner":
		return spanner.NewResolved(c, l, creds)
	case "cosmosdb":
		return cosmosdb.NewDiscoveryResolved(c, l, creds)
	case "ignite":
		return ignite.NewResolved(c, l, ignite.Credentials{URL: s.spec.URL, Username: s.spec.Username, Password: s.spec.Password, TLS: s.tls})
	case "exasol":
		return exasol.NewResolved(c, l, exasol.Credentials{URL: s.spec.URL, Username: s.spec.Username, Password: s.spec.Password, Schema: s.spec.Database, TLS: s.tls})
	case "athena":
		return athena.NewResolved(c, l, s.awsCredentials())
	case "dynamodb":
		return dynamodb.NewResolved(c, l, s.awsCredentials())
	case "clickhouse_lambda":
		return clickhouselambda.NewResolved(c, l, s.awsCredentials())
	}
	return nil, adapter.ErrUnsupported
}
func (s *Session) awsCredentials() awsapi.Credentials {
	return awsapi.Credentials{URL: s.spec.URL, AccessKeyID: s.spec.Username, SecretAccessKey: s.spec.Password, SessionToken: s.spec.Token, TLS: s.tls}
}
func (s *Session) Query(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	if s == nil || ctx == nil || sink == nil || q.Validate() != nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	if len(q.Parameters) != 0 {
		return adapter.QueryStats{}, adapter.ErrUnsupported
	}
	l, err := s.limits(ctx, q.MaxRows, q.MaxBytes)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	e, err := s.open(l)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer e.Close()
	stats, err := e.Execute(ctx, query.Request{Mode: "native", ConnectionID: s.source.ID, SQL: q.Statement}, splitSink{next: sink, rows: int64(q.BatchRows)})
	return adapter.QueryStats{Rows: stats.Rows, Bytes: stats.Bytes, Elapsed: time.Duration(stats.DurationNS)}, err
}
func (s *Session) Test(ctx context.Context) error {
	if s == nil || ctx == nil {
		return adapter.ErrInvalid
	}
	if s.spec.Engine == "cosmosdb" {
		object := "databases"
		if s.spec.Schema != "" {
			object = "tables"
		}
		_, err := s.Inspect(ctx, operations.MetadataSpec{Object: object, Limit: 1}, adapter.Limits{MaxRows: 1, MaxBytes: 64 << 10, BatchRows: 1}, discardSink{})
		return err
	}
	if s.spec.Engine == "dynamodb" {
		l, err := s.limits(ctx, 1, 64<<10)
		if err != nil {
			return err
		}
		c, err := awsapi.NewResolved(s.source, l, "dynamodb", s.awsCredentials())
		if err != nil {
			return err
		}
		defer c.Close()
		if s.spec.Database == "" {
			var out struct {
				Names *[]string `json:"TableNames"`
			}
			_, err = c.Do(ctx, "DynamoDB_20120810.ListTables", map[string]int{"Limit": 1}, &out)
			if err == nil && out.Names == nil {
				return adapter.ErrInvalid
			}
			return err
		}
		var out struct {
			Table *struct {
				Name string `json:"TableName"`
			} `json:"Table"`
		}
		_, err = c.Do(ctx, "DynamoDB_20120810.DescribeTable", map[string]string{"TableName": s.spec.Database}, &out)
		if err == nil && (out.Table == nil || out.Table.Name != s.spec.Database) {
			return adapter.ErrInvalid
		}
		return err
	}
	statement := "SELECT 1"
	if s.spec.Engine == "cosmosdb" {
		statement = "SELECT * FROM c WHERE false"
	}
	_, err := s.Query(ctx, adapter.Query{Statement: statement, MaxRows: 1, MaxBytes: 64 << 10, BatchRows: 1}, discardSink{})
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
